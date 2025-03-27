package com.tencent.supersonic.headless.chat.parser.llm;

import com.hankcs.hanlp.seg.common.Term;
import com.tencent.supersonic.common.calcite.AtomicFieldExtractor;
import com.tencent.supersonic.common.jsqlparser.SqlValidHelper;
import com.tencent.supersonic.common.pojo.Constants;
import com.tencent.supersonic.common.pojo.DateConf;
import com.tencent.supersonic.common.pojo.Text2SQLExemplar;
import com.tencent.supersonic.headless.api.pojo.*;
import com.tencent.supersonic.headless.chat.ChatQueryContext;
import com.tencent.supersonic.headless.chat.knowledge.DatabaseMapResult;
import com.tencent.supersonic.headless.chat.mapper.DatabaseMatchStrategy;
import com.tencent.supersonic.headless.chat.query.QueryManager;
import com.tencent.supersonic.headless.chat.query.llm.LLMSemanticQuery;
import com.tencent.supersonic.headless.chat.query.llm.s2sql.LLMReq;
import com.tencent.supersonic.headless.chat.query.llm.s2sql.LLMResp;
import com.tencent.supersonic.headless.chat.query.llm.s2sql.LLMSqlQuery;
import com.tencent.supersonic.headless.chat.query.llm.s2sql.LLMSqlResp;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class LLMResponseService {

    public void addParseInfo(ChatQueryContext queryCtx, ParseResult parseResult, String s2SQL,
            Double weight) {
        if (Objects.isNull(weight)) {
            weight = 0D;
        }


        LLMSemanticQuery semanticQuery = QueryManager.createLLMQuery(LLMSqlQuery.QUERY_MODE);
        SemanticParseInfo parseInfo = semanticQuery.getParseInfo();
        parseInfo.setDataSet(queryCtx.getSemanticSchema().getDataSet(parseResult.getDataSetId()));
        parseInfo.setQueryConfig(queryCtx.getSemanticSchema().getQueryConfig(parseResult.getDataSetId()));

        // 通过 SQL提取的字段，重新精确定义匹配的字段，不应该继续用之前的 Mapper结果
        DataSetSchema dataSetSchema = queryCtx.getSemanticSchema().getDataSetSchemaMap().get(parseInfo.getDataSetId());
        try {
            AtomicFieldExtractor.ExtractResult result = AtomicFieldExtractor.extractFromSql(s2SQL);
            
            // 处理模型和字段之间的关系
            Map<Long, List<SchemaElement>> metricsByModel = groupSchemaElementsByModel(dataSetSchema.getMetrics());
            Map<Long, List<SchemaElement>> dimensionsByModel = groupSchemaElementsByModel(dataSetSchema.getDimensions());
            
            // 创建ID到SchemaElement的映射，用于后续查找关联元素
            Map<Long, SchemaElement> idToElementMap = createIdToElementMap(dataSetSchema);
            
            // 筛选保留的模型和字段
            Set<Long> validModels = findValidModels(metricsByModel, dimensionsByModel, result, idToElementMap);
            
            // 获取有效模型下的字段，并创建SchemaElementMatch对象
            Set<SchemaElementMatch> filteredMetrics = filterElementsToMatches(dataSetSchema.getMetrics(), validModels, result, idToElementMap);
            Set<SchemaElementMatch> filteredDimensions = filterElementsToMatches(dataSetSchema.getDimensions(), validModels, result, idToElementMap);
            
            // 将筛选后的字段匹配添加到解析信息中
            parseInfo.getElementMatches().addAll(filteredMetrics);
            parseInfo.getElementMatches().addAll(filteredDimensions);

            log.info("Extracted valid models: {}. Metrics size: {}, Dimensions size: {}", 
                    validModels, filteredMetrics.size(), filteredDimensions.size());
        } catch (Exception e) {
            log.warn("extract filed from sql failed.", e);
            // 兼容老的，如果有错
            parseInfo.getElementMatches().addAll(queryCtx.getMapInfo().getMatchedElements(parseInfo.getDataSetId()));
        }


        Map<String, Object> properties = new HashMap<>();
        properties.put(Constants.CONTEXT, parseResult);
        properties.put("type", "internal");
        Text2SQLExemplar exemplar =
                Text2SQLExemplar.builder().question(queryCtx.getRequest().getQueryText())
                        .sideInfo(parseResult.getLlmResp().getSideInfo())
                        .dbSchema(parseResult.getLlmResp().getSchema())
                        .sql(parseResult.getLlmResp().getSqlOutput()).build();
        properties.put(Text2SQLExemplar.PROPERTY_KEY, exemplar);
        parseInfo.setProperties(properties);
        parseInfo.setScore(queryCtx.getRequest().getQueryText().length() * (1 + weight));
        parseInfo.setQueryMode(semanticQuery.getQueryMode());
        parseInfo.getSqlInfo().setParsedS2SQL(s2SQL);
        parseInfo.getSqlInfo().setCorrectedS2SQL(s2SQL);


        SchemaElement partitionDimension = dataSetSchema.getPartitionDimension();
        if (Objects.nonNull(partitionDimension)) {
            DateConf dateConf = new DateConf();
            dateConf.setDateField(partitionDimension.getName());
            parseInfo.setDateInfo(dateConf);
        }
        queryCtx.getCandidateQueries().add(semanticQuery);
    }
    
    /**
     * 创建ID到SchemaElement的映射
     */
    private Map<Long, SchemaElement> createIdToElementMap(DataSetSchema dataSetSchema) {
        Map<Long, SchemaElement> idToElementMap = new HashMap<>();
        
        // 添加所有指标
        if (CollectionUtils.isNotEmpty(dataSetSchema.getMetrics())) {
            dataSetSchema.getMetrics().forEach(element -> {
                if (element.getId() != null) {
                    idToElementMap.put(element.getId(), element);
                }
            });
        }
        
        // 添加所有维度
        if (CollectionUtils.isNotEmpty(dataSetSchema.getDimensions())) {
            dataSetSchema.getDimensions().forEach(element -> {
                if (element.getId() != null) {
                    idToElementMap.put(element.getId(), element);
                }
            });
        }
        
        return idToElementMap;
    }
    
    /**
     * 按模型分组SchemaElement
     */
    private Map<Long, List<SchemaElement>> groupSchemaElementsByModel(Set<SchemaElement> elements) {
        if (CollectionUtils.isEmpty(elements)) {
            return Collections.emptyMap();
        }
        return elements.stream().collect(Collectors.groupingBy(SchemaElement::getModel));
    }
    
    /**
     * 找出有效的模型
     * 如果一个模型下没有任何字段匹配SQL提取的表和字段，则该模型被认为是无效的
     */
    private Set<Long> findValidModels(
            Map<Long, List<SchemaElement>> metricsByModel, 
            Map<Long, List<SchemaElement>> dimensionsByModel, 
            AtomicFieldExtractor.ExtractResult result,
            Map<Long, SchemaElement> idToElementMap) {
        
        Set<Long> validModels = new HashSet<>();
        
        // 处理指标
        for (Map.Entry<Long, List<SchemaElement>> entry : metricsByModel.entrySet()) {
            Long modelId = entry.getKey();
            List<SchemaElement> elements = entry.getValue();
            
            if (hasMatchingElements(elements, result, idToElementMap)) {
                validModels.add(modelId);
            }
        }
        
        // 处理维度
        for (Map.Entry<Long, List<SchemaElement>> entry : dimensionsByModel.entrySet()) {
            Long modelId = entry.getKey();
            List<SchemaElement> elements = entry.getValue();
            
            if (hasMatchingElements(elements, result, idToElementMap)) {
                validModels.add(modelId);
            }
        }
        
        return validModels;
    }
    
    /**
     * 检查模型中是否有匹配的字段
     */
    private boolean hasMatchingElements(
            List<SchemaElement> elements, 
            AtomicFieldExtractor.ExtractResult result,
            Map<Long, SchemaElement> idToElementMap) {
            
        if (CollectionUtils.isEmpty(elements)) {
            return false;
        }
        
        // 创建包含直接名称和关联名称的所有可能名称集合
        Set<String> allPossibleNames = new HashSet<>();
        
        // 收集所有可能的名称（包括关联元素的名称）
        for (SchemaElement element : elements) {
            // 添加元素自身的名称
            allPossibleNames.add(element.getName());
            
            // 添加关联元素的名称
            if (CollectionUtils.isNotEmpty(element.getRelatedSchemaElements())) {
                for (RelatedSchemaElement related : element.getRelatedSchemaElements()) {
                    SchemaElement relatedElement = idToElementMap.get(related.getDimensionId());
                    if (relatedElement != null) {
                        allPossibleNames.add(relatedElement.getName());
                    }
                }
            }
        }
        
        // 检查是否有任何名称出现在实际表或表字段中
        for (String name : allPossibleNames) {
            // 检查名称是否在真实表中
            if (result.getRealTables().contains(name)) {
                return true;
            }
            
            // 检查名称是否在表字段中
            for (Map.Entry<String, Set<String>> entry : result.getTableToFields().entrySet()) {
                if (entry.getValue().contains(name)) {
                    return true;
                }
            }
        }
        
        return false;
    }
    
    /**
     * 根据有效模型筛选SchemaElement并转换为SchemaElementMatch
     */
    private Set<SchemaElementMatch> filterElementsToMatches(
            Set<SchemaElement> elements,
            Set<Long> validModels, 
            AtomicFieldExtractor.ExtractResult result,
            Map<Long, SchemaElement> idToElementMap) {
        
        if (CollectionUtils.isEmpty(elements)) {
            return Collections.emptySet();
        }
        
        return elements.stream()
                .filter(element -> validModels.contains(element.getModel()) && 
                           isElementMatchingSql(element, result, idToElementMap))
                .map(this::createSchemaElementMatch)
                .collect(Collectors.toSet());
    }
    
    /**
     * 创建SchemaElementMatch对象
     */
    private SchemaElementMatch createSchemaElementMatch(SchemaElement element) {
        return SchemaElementMatch.builder()
                .element(element)
                .similarity(1.0) // 完全匹配
                .llmMatched(true) // 通过LLM匹配
                .build();
    }
    
    /**
     * 检查单个元素或其关联元素是否匹配SQL提取的表和字段
     */
    private boolean isElementMatchingSql(
            SchemaElement element, 
            AtomicFieldExtractor.ExtractResult result,
            Map<Long, SchemaElement> idToElementMap) {
            
        // 检查元素自身名称
        if (isNameMatchingSql(element.getName(), result)) {
            return true;
        }
        
        // 检查关联元素名称
        if (CollectionUtils.isNotEmpty(element.getRelatedSchemaElements())) {
            for (RelatedSchemaElement related : element.getRelatedSchemaElements()) {
                SchemaElement relatedElement = idToElementMap.get(related.getDimensionId());
                if (relatedElement != null && isNameMatchingSql(relatedElement.getName(), result)) {
                    return true;
                }
            }
        }
        
        return false;
    }
    
    /**
     * 检查名称是否匹配SQL提取的表和字段
     */
    private boolean isNameMatchingSql(String name, AtomicFieldExtractor.ExtractResult result) {
        // 检查名称是否在真实表中
        if (result.getRealTables().contains(name)) {
            return true;
        }
        
        // 检查名称是否在表字段中
        for (Map.Entry<String, Set<String>> entry : result.getTableToFields().entrySet()) {
            if (entry.getValue().contains(name)) {
                return true;
            }
        }
        
        return false;
    }

    public Map<String, LLMSqlResp> getDeduplicationSqlResp(int currentRetry, LLMResp llmResp) {
        Map<String, LLMSqlResp> sqlRespMap = llmResp.getSqlRespMap();
        if (MapUtils.isEmpty(sqlRespMap)) {
            LLMSqlResp llmSqlResp = new LLMSqlResp(1D, new ArrayList<>());
            sqlRespMap.put(llmResp.getSqlOutput(), llmSqlResp);
        }
        Map<String, LLMSqlResp> result = new HashMap<>();
        for (Map.Entry<String, LLMSqlResp> entry : sqlRespMap.entrySet()) {
            String key = entry.getKey();
            if (result.keySet().stream()
                    .anyMatch(existKey -> SqlValidHelper.equals(existKey, key))) {
                continue;
            }
            if (!SqlValidHelper.isValidSQL(key)) {
                log.error("currentRetry:{},sql is not valid:{}", currentRetry, key);
                continue;
            }
            result.put(key, entry.getValue());
        }
        return result;
    }
}
