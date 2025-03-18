//package com.tencent.supersonic.headless.chat.parser.llm;
//
//import cn.hutool.core.collection.CollectionUtil;
//import com.meicloud.mdv.props.MdvIntegrationProps;
//import com.meicloud.mdv.supersonic.context.ChatBIContext;
//import com.meicloud.mdv.supersonic.service.ModelMatchService;
//import com.tencent.supersonic.common.pojo.DimensionConstants;
//import com.tencent.supersonic.common.pojo.ModelRela;
//import com.tencent.supersonic.common.util.ContextUtils;
//import com.tencent.supersonic.common.util.DateUtils;
//import com.tencent.supersonic.headless.api.pojo.*;
//import com.tencent.supersonic.headless.api.pojo.enums.DimensionType;
//import com.tencent.supersonic.headless.api.pojo.response.ModelResp;
//import com.tencent.supersonic.headless.chat.ChatQueryContext;
//import com.tencent.supersonic.headless.chat.parser.ParserConfig;
//import com.tencent.supersonic.headless.chat.parser.llm.LLMRequestService;
//import com.tencent.supersonic.headless.chat.query.llm.s2sql.LLMReq;
//import com.tencent.supersonic.headless.server.service.DimensionService;
//import com.tencent.supersonic.headless.server.service.MetricService;
//import com.tencent.supersonic.headless.server.service.ModelRelaService;
//import com.tencent.supersonic.headless.server.service.ModelService;
//import lombok.extern.slf4j.Slf4j;
//import org.jetbrains.annotations.NotNull;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import java.util.*;
//import java.util.function.Function;
//import java.util.stream.Collectors;
//
//import static com.tencent.supersonic.headless.chat.parser.ParserConfig.PARSER_LINKING_VALUE_ENABLE;
//import static com.tencent.supersonic.headless.chat.parser.ParserConfig.PARSER_STRATEGY_TYPE;
//
//@Slf4j
//@Service
//public class MDVLLMRequestService extends LLMRequestService {
//
//    @Autowired
//    private ParserConfig parserConfig;
//
//    @Autowired
//    private DimensionService dimensionService;
//
//    @Autowired
//    private MetricService metricService;
//
//    @Autowired
//    private MdvIntegrationProps mdvIntegrationProps;
//
//    @Autowired
//    private ModelMatchService modelMatchService;
//
//    @Autowired
//    private ModelService modelService;
//
//    @Autowired
//    private MdvIntegrationProps integrationProps;
//
//    public LLMReq getLlmReq(ChatQueryContext queryCtx, Long dataSetId) {
//        Map<Long, String> dataSetIdToName = queryCtx.getSemanticSchema().getDataSetIdToName();
//        String queryText = queryCtx.getRequest().getQueryText();
//        // 本来采用queryCtx.getRequest().setModelIds(matchModelList); 但是后面逻辑没有复用该对象，所以才自定义上下文对象。
//        Set<Long> matchModelIds = getMatchModelList(queryCtx, dataSetId);
//        ChatBIContext.setMatchModelIds(matchModelIds);
//        // 计算模型名称相似度
//        // 术语 TODO
//        LLMReq llmReq = new LLMReq();
//        llmReq.setQueryText(queryText);
//        LLMReq.LLMSchema llmSchema = new LLMReq.LLMSchema();
//        llmReq.setSchema(llmSchema);
//        llmSchema.setDatabaseType(getDatabaseType(queryCtx, dataSetId));
//        llmSchema.setDataSetId(dataSetId);
//        llmSchema.setDataSetName(dataSetIdToName.get(dataSetId));
//
//        llmSchema.setMetrics(getMappedMetrics(queryCtx, dataSetId, matchModelIds));
//        llmSchema.setDimensions(getMappedDimensions(queryCtx, dataSetId, matchModelIds));
//        // 待确认PartitionTime参数、PartitionTime的用法。
//        llmSchema.setPartitionTime(getPartitionTime(queryCtx, dataSetId, matchModelIds));
//        llmSchema.setPrimaryKey(getPrimaryKey(queryCtx, dataSetId));
//
//        boolean linkingValueEnabled =
//                Boolean.parseBoolean(parserConfig.getParameterValue(PARSER_LINKING_VALUE_ENABLE));
//        if (linkingValueEnabled) {
//            llmSchema.setValues(getMappedValues(queryCtx, dataSetId));
//        }
//
//        llmReq.setCurrentDate(DateUtils.getBeforeDate(0));
//        llmReq.setTerms(getMappedTerms(queryCtx, dataSetId));
//        llmReq.setSqlGenType(
//                LLMReq.SqlGenType.valueOf(parserConfig.getParameterValue(PARSER_STRATEGY_TYPE)));
//        llmReq.setChatAppConfig(queryCtx.getRequest().getChatAppConfig());
//        llmReq.setDynamicExemplars(queryCtx.getRequest().getDynamicExemplars());
//
//        return llmReq;
//    }
//
//    /***
//     * 匹配最佳模型及模型的度量和维度
//     * 规则：1、最佳匹配的模型
//     */
//    private Set<Long> getMatchModelList(ChatQueryContext queryCtx, Long dataSetId) {
//        Set<Long> matchModelIds = new HashSet<>();
//        ModelRelaService modelRelaService = ContextUtils.getBean(ModelRelaService.class);
//        Map<Long, List<SchemaElementMatch>> dataSetElementMatches = queryCtx.getMapInfo().getDataSetElementMatches();
//        List<SchemaElementMatch> schemaElementMatches = dataSetElementMatches.get(dataSetId);
//        if (CollectionUtil.isEmpty(schemaElementMatches)) {
//            log.info("not matched model");
//            return matchModelIds;
//        }
//        Map<Long, List<SchemaElementMatch>> schemaElementMatchMap = schemaElementMatches.stream()
//                .collect(Collectors.groupingBy(v -> v.getElement().getModel()));
//        // 按照相似度对模型进行排序
//        // 同一个关键词匹配多个字段，只获取分数最高的字段。关键词:detectWord，匹配到的关键词:word。
//        List<Map.Entry<Long, Double>> schemaElementMatchMapList = schemaElementMatchMap.entrySet().stream()
//                .map(e -> Map.entry(e.getKey(), e.getValue().stream().collect(Collectors.toMap(v -> v.getDetectWord(), Function.identity(), (o, n) ->
//                                o.getSimilarity() > n.getSimilarity() ? o : n)).values().stream().map(SchemaElementMatch::getSimilarity)
//                        .reduce(0D, Double::sum))).sorted(Comparator.comparingDouble(Map.Entry<Long, Double>::getValue).reversed())
//                .collect(Collectors.toList());
//        log.info("final result model list:");
//        List<ModelMatchService.ModelInfo> modelInfoList = new ArrayList<>();
//        for (Map.Entry<Long, Double> entry : schemaElementMatchMapList) {
//            ModelMatchService.ModelInfo modelInfo = new ModelMatchService.ModelInfo();
//            Long modelId = entry.getKey();
//            List<SchemaElementMatch> currentSchemaElementMatches = schemaElementMatchMap.get(modelId);
//            modelInfo.setModelId(modelId);
//            if (CollectionUtil.isNotEmpty(currentSchemaElementMatches)) {
//                log.info("model=[{}] score=[{}] count=[{}]", modelId, entry.getValue(), currentSchemaElementMatches.size());
//                for (SchemaElementMatch currentSchemaElementMatch : currentSchemaElementMatches) {
//                    SchemaElement element = currentSchemaElementMatch.getElement();
//                    log.info("  type=[{}] id=[{}] name=[{}] similarity=[{}] detectWord=[{}]",
//                            element.getType().name(), element.getId(), element.getName(), currentSchemaElementMatch.getSimilarity(), currentSchemaElementMatch.getDetectWord());
//                    modelInfo.getFields().add(element.getName());
//                }
//            }
//            modelInfoList.add(modelInfo);
//        }
//
//        if (schemaElementMatchMapList.size() > 0) {
//            ModelMatchService.ModelInfo bestModelInfo = null;
//            if (schemaElementMatchMapList.size() > 1 && integrationProps.getSupersonic().getEnableLlmToMatchModel()) {
//                // 查找模型名称
//                List<Long> modelIdList = modelInfoList.stream().map(ModelMatchService.ModelInfo::getModelId).collect(Collectors.toList());
//                if(CollectionUtil.isNotEmpty(modelIdList)) {
//                    Map<Long, ModelResp> modelMap = modelService.getModelList(new MetaFilter(modelIdList)).stream().collect(Collectors.toMap(ModelResp::getId, Function.identity(), (o, n) -> n));
//                    modelInfoList.forEach(modelInfo -> {
//                        ModelResp modelResp = modelMap.get(modelInfo.getModelId());
//                        if (modelResp != null) {
//                            modelInfo.setModelName(modelResp.getName());
//                        }
//                    });
//                    bestModelInfo = modelMatchService.findBestModel(queryCtx.getRequest().getQueryText(), modelInfoList);
//                }
//            }
//            Long matchModelId = null;
//            if (bestModelInfo != null && schemaElementMatchMapList.stream().map(Map.Entry::getKey).collect(Collectors.toSet()).contains(bestModelInfo.getModelId())) {
//                matchModelId = bestModelInfo.getModelId();
//            } else {
//                matchModelId = schemaElementMatchMapList.get(0).getKey();
//            }
//
//            matchModelIds.add(matchModelId);
//            // 与最匹配的模型进行关系比较，存在关系即可加入匹配模型列表。
//            Set<Long> modelIds = schemaElementMatchMap.keySet();
//            List<ModelRela> modelRelations = modelRelaService.getModelRela(new ArrayList<>(modelIds));
//            if (CollectionUtil.isNotEmpty(modelRelations)) {
//                // 匹配:区分直接和间接匹配。（数据结构：采用广度优先搜索。）
//                Queue<Long> queue = new LinkedList<>();
//                queue.offer(matchModelId);
//                while (!queue.isEmpty()) {
//                    Long currentModelId = queue.poll();
//                    List<ModelRela> relationModelIds = modelRelations.stream()
//                            .filter(r -> r.getFromModelId().equals(currentModelId) || r.getToModelId().equals(currentModelId))
//                            .collect(Collectors.toList());
//                    // 存在关联关系。
//                    if (CollectionUtil.isNotEmpty(relationModelIds)) {
//                        for (ModelRela modelRela : relationModelIds) {
//                            Long fromModelId = modelRela.getFromModelId();
//                            Long toModelId = modelRela.getToModelId();
//                            if (!matchModelIds.contains(fromModelId)) {
//                                queue.offer(fromModelId);
//                                matchModelIds.add(fromModelId);
//                            }
//                            if (!matchModelIds.contains(toModelId)) {
//                                queue.offer(toModelId);
//                                matchModelIds.add(toModelId);
//                            }
//                        }
//                    }
//                }
//            }
//        }
//        return matchModelIds;
//    }
//
//    /***
//     * 此处将匹配的模型的所有字段透传给大模型，包含维度和度量，避免缺少数据问题。
//     */
//    protected List<SchemaElement> getMappedMetrics(@NotNull ChatQueryContext queryCtx, Long dataSetId, Set<Long> matchModelList) {
//        return super.getMappedMetrics(queryCtx, dataSetId).stream().filter(m -> matchModelList.contains(m.getModel())).collect(Collectors.toList());
//    }
//
//    protected List<SchemaElement> getMappedDimensions(@NotNull ChatQueryContext queryCtx, Long dataSetId, Set<Long> matchModelList) {
//        return super.getMappedDimensions(queryCtx, dataSetId).stream().filter(d -> matchModelList.contains(d.getModel())).collect(Collectors.toList());
//    }
//
//    /***
//     * 目前MDV没有标记分割时间（PartitionTime），兼容处理取第一个时间字段。TODO
//     */
//    protected SchemaElement getPartitionTime(@NotNull ChatQueryContext queryCtx, Long dataSetId, Set<Long> matchModelList) {
//        SchemaElement partitionTime = super.getPartitionTime(queryCtx, dataSetId);
//        if (partitionTime != null && matchModelList.contains(partitionTime.getModel())) {
//            return partitionTime;
//        }
//        SemanticSchema semanticSchema = queryCtx.getSemanticSchema();
//        if (semanticSchema == null || semanticSchema.getDataSetSchemaMap() == null) {
//            return null;
//        }
//        Map<Long, DataSetSchema> dataSetSchemaMap = semanticSchema.getDataSetSchemaMap();
//        DataSetSchema dataSetSchema = dataSetSchemaMap.get(dataSetId);
//        return dataSetSchema.getDimensions().stream().filter(d -> {
//            DimensionType dimensionType = null;
//            Object o = d.getExtInfo().get(DimensionConstants.DIMENSION_TYPE);
//            if (o instanceof DimensionType) {
//                dimensionType = (DimensionType) o;
//            }
//            if (o instanceof String) {
//                dimensionType = DimensionType.valueOf((String) o);
//            }
//            return dimensionType != null && DimensionType.isTimeDimension(dimensionType) && matchModelList.contains(d.getModel());
//        }).findFirst().orElse(null);
//    }
//}
