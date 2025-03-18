package com.tencent.supersonic.headless.chat.mapper;

import com.tencent.supersonic.common.util.ContextUtils;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SchemaElementMatch;
import com.tencent.supersonic.headless.api.pojo.SchemaElementType;
import com.tencent.supersonic.headless.api.pojo.SchemaMapInfo;
import com.tencent.supersonic.headless.api.pojo.response.S2Term;
import com.tencent.supersonic.headless.chat.ChatQueryContext;
import com.tencent.supersonic.headless.chat.knowledge.*;
import com.tencent.supersonic.headless.chat.knowledge.builder.BaseWordBuilder;
import com.tencent.supersonic.headless.chat.knowledge.helper.HanlpHelper;
import com.tencent.supersonic.headless.chat.knowledge.helper.NatureHelper;
import com.tencent.supersonic.headless.chat.utils.EditDistanceUtils;
import dev.langchain4j.store.embedding.Retrieval;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

/***
 * 自定义KeywordMapper，度量维度匹配优先级:
 * 1、匹配维度值所对应的维度。
 * 2、Hanlp、Database匹配到的度量和维度。
 * 3、
 * 其他:考虑不同分词器，中英文。
 * 什么场景需要调用大模型：1、存在相似度比较高，无法确认使用哪个度量维度？
 *                     2、
 */
@Slf4j
public class MDVKeywordMapper extends BaseMapper {

    public static final Double HANLP_VALUE_THRESHOLD = 0.8D;

    @Override
    public void doMap(ChatQueryContext chatQueryContext) {
        String queryText = chatQueryContext.getRequest().getQueryText();

        // 1. hanlpDict Match
        List<S2Term> terms = HanlpHelper.getTerms(queryText, chatQueryContext.getModelIdToDataSetIds());
        HanlpDictMatchStrategy hanlpMatchStrategy = ContextUtils.getBean(HanlpDictMatchStrategy.class);
        List<HanlpMapResult> hanlpMatchResults = getMatches(chatQueryContext, hanlpMatchStrategy);

        hanlpMatchResults = hanlpMatchResults.stream().filter(v -> {
            List<String> natures = v.getNatures();
            if (org.apache.commons.collections.CollectionUtils.isNotEmpty(natures)) {
                String nature = natures.get(0);
                if (SchemaElementType.VALUE.equals(NatureHelper.convertToElementType(nature))) {
                    return v.getSimilarity() >= HANLP_VALUE_THRESHOLD;
                }
            }
            return true;
        }).collect(Collectors.toList());

        convertMapResultToMapInfo(hanlpMatchResults, chatQueryContext, terms);

        // 2. database Match
        DatabaseMatchStrategy databaseMatchStrategy = ContextUtils.getBean(DatabaseMatchStrategy.class);
        List<DatabaseMapResult> databaseMatchResults = getMatches(chatQueryContext, databaseMatchStrategy);
        convertMapResultToMapInfo(chatQueryContext, databaseMatchResults);

        // 3. embedding Match
        EmbeddingMatchStrategy embeddingMatchStrategy = ContextUtils.getBean(EmbeddingMatchStrategy.class);
        List<EmbeddingResult> embeddingMatchResults = getMatches(chatQueryContext, embeddingMatchStrategy);
        convertEmbeddingMapResultToMapInfo(chatQueryContext, embeddingMatchResults);
        // 4. print Match
        hanlpMatchResults.forEach(this::printMapResultInfo);
        databaseMatchResults.forEach(this::printMapResultInfo);
        embeddingMatchResults.forEach(this::printMapResultInfo);
    }

    private void convertMapResultToMapInfo(List<HanlpMapResult> mapResults,
                                           ChatQueryContext chatQueryContext, List<S2Term> terms) {
        if (CollectionUtils.isEmpty(mapResults)) {
            return;
        }

        HanlpHelper.transLetterOriginal(mapResults);
        Map<String, Long> wordNatureToFrequency =
                terms.stream().collect(Collectors.toMap(term -> term.getWord() + term.getNature(),
                        term -> Long.valueOf(term.getFrequency()), (value1, value2) -> value2));

        for (HanlpMapResult hanlpMapResult : mapResults) {
            for (String nature : hanlpMapResult.getNatures()) {
                Long dataSetId = NatureHelper.getDataSetId(nature);
                if (Objects.isNull(dataSetId)) {
                    continue;
                }
                SchemaElementType elementType = NatureHelper.convertToElementType(nature);
                if (Objects.isNull(elementType)) {
                    continue;
                }
                Long elementID = NatureHelper.getElementID(nature);
                SchemaElement element = getSchemaElement(dataSetId, elementType, elementID,
                        chatQueryContext.getSemanticSchema());
                if (Objects.isNull(element)) {
                    continue;
                }

                Long frequency = wordNatureToFrequency.get(hanlpMapResult.getName() + nature);
                SchemaElementMatch schemaElementMatch = SchemaElementMatch.builder()
                        .element(element).frequency(frequency).word(hanlpMapResult.getName())
                        .similarity(hanlpMapResult.getSimilarity())
                        .detectWord(hanlpMapResult.getDetectWord()).build();
                // doDimValueAliasLogic 将维度值别名进行替换成真实维度值
                doDimValueAliasLogic(schemaElementMatch);
                addToSchemaMap(chatQueryContext.getMapInfo(), dataSetId, schemaElementMatch);
            }
        }
    }

    private void doDimValueAliasLogic(SchemaElementMatch schemaElementMatch) {
        SchemaElement element = schemaElementMatch.getElement();
        if (SchemaElementType.VALUE.equals(element.getType())) {
            Long dimId = element.getId();
            String word = schemaElementMatch.getWord();
            Map<Long, List<DictWord>> dimValueAlias = KnowledgeBaseService.getDimValueAlias();
            if (Objects.nonNull(dimId) && StringUtils.isNotEmpty(word)
                    && dimValueAlias.containsKey(dimId)) {
                Map<String, DictWord> aliasAndDictMap = dimValueAlias.get(dimId).stream()
                        .collect(Collectors.toMap(dictWord -> dictWord.getAlias(),
                                dictWord -> dictWord, (v1, v2) -> v2));
                if (aliasAndDictMap.containsKey(word)) {
                    String wordTech = aliasAndDictMap.get(word).getWord();
                    schemaElementMatch.setWord(wordTech);
                }
            }
        }
    }

    private void convertMapResultToMapInfo(ChatQueryContext chatQueryContext, List<DatabaseMapResult> mapResults) {
        for (DatabaseMapResult match : mapResults) {
            SchemaElement schemaElement = match.getSchemaElement();
            Set<Long> regElementSet =
                    getRegElementSet(chatQueryContext.getMapInfo(), schemaElement);
            if (regElementSet.contains(schemaElement.getId())) {
                continue;
            }
            SchemaElementMatch schemaElementMatch = SchemaElementMatch.builder()
                    .element(schemaElement).word(schemaElement.getName())
                    .detectWord(match.getDetectWord()).frequency(BaseWordBuilder.DEFAULT_FREQUENCY)
                    .similarity(EditDistanceUtils.getSimilarity(match.getDetectWord(),
                            schemaElement.getName()))
                    .build();
            log.info("add to schema, elementMatch {}", schemaElementMatch);
            addToSchemaMap(chatQueryContext.getMapInfo(), schemaElement.getDataSetId(),
                    schemaElementMatch);
        }
    }

    private void convertEmbeddingMapResultToMapInfo(ChatQueryContext chatQueryContext, List<EmbeddingResult> matchResults) {
        // Process match results
        HanlpHelper.transLetterOriginal(matchResults);

        // 2. Build SchemaElementMatch based on match results
        for (EmbeddingResult matchResult : matchResults) {
            Long elementId = Retrieval.getLongId(matchResult.getId());
            Long dataSetId = Retrieval.getLongId(matchResult.getMetadata().get("dataSetId"));

            // Skip if dataSetId is null
            if (Objects.isNull(dataSetId)) {
                continue;
            }
            SchemaElementType elementType =
                    SchemaElementType.valueOf(matchResult.getMetadata().get("type"));
            SchemaElement schemaElement = getSchemaElement(dataSetId, elementType, elementId,
                    chatQueryContext.getSemanticSchema());

            // Skip if schemaElement is null
            if (schemaElement == null) {
                continue;
            }

            Set<Long> regElementSet = getRegElementSet(chatQueryContext.getMapInfo(), schemaElement);
            if (regElementSet.contains(schemaElement.getId())) {
                continue;
            }


            // Build SchemaElementMatch object
            SchemaElementMatch schemaElementMatch = SchemaElementMatch.builder()
                    .element(schemaElement).frequency(BaseWordBuilder.DEFAULT_FREQUENCY)
                    .word(matchResult.getName()).similarity(matchResult.getSimilarity())
                    .detectWord(matchResult.getDetectWord())
                    .build();

            schemaElementMatch.setLlmMatched(matchResult.isLlmMatched());

            // 3. Add SchemaElementMatch to mapInfo
            addToSchemaMap(chatQueryContext.getMapInfo(), dataSetId, schemaElementMatch);
        }
    }

    private Set<Long> getRegElementSet(SchemaMapInfo schemaMap, SchemaElement schemaElement) {
        List<SchemaElementMatch> elements =
                schemaMap.getMatchedElements(schemaElement.getDataSetId());
        if (CollectionUtils.isEmpty(elements)) {
            return new HashSet<>();
        }
        return elements.stream().filter(
                        elementMatch -> SchemaElementType.METRIC.equals(elementMatch.getElement().getType())
                                || SchemaElementType.DIMENSION.equals(elementMatch.getElement().getType()))
                .map(elementMatch -> elementMatch.getElement().getId()).collect(Collectors.toSet());
    }

    private void printMapResultInfo(MapResult mapResult) {
        if (mapResult instanceof HanlpMapResult) {
            HanlpMapResult hanlpMatchResult = (HanlpMapResult) mapResult;
            hanlpMatchResult.getNatures().forEach(nature -> {
                try {
                    SchemaElementType elementType = NatureHelper.convertToElementType(nature);
                    // 有些场景是这样：_146_1195dimension
                    Long id = NatureHelper.getElementID(nature.replace("dimension", "").replace("metric", ""));
                    log.info("match=[hanlp] model=[{}] id=[{}] type=[{}] name=[{}] detectWord=[{}] similarity=[{}]",
                            "", id, elementType != null ? elementType.name() : "", hanlpMatchResult.getName(), hanlpMatchResult.getDetectWord(), hanlpMatchResult.getSimilarity());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } else if (mapResult instanceof DatabaseMapResult) {
            DatabaseMapResult databaseMapResult = (DatabaseMapResult) mapResult;
            SchemaElement schemaElement = databaseMapResult.getSchemaElement();
            log.info("match=[database] model=[{}] id=[{}] type=[{}] name=[{}] detectWord=[{}] similarity=[{}] dataSetName=[{}]",
                    schemaElement.getModel(), schemaElement.getId(), schemaElement.getType().name(),
                    databaseMapResult.getName(), databaseMapResult.getDetectWord(), databaseMapResult.getSimilarity(), schemaElement.getDataSetName());
        } else if (mapResult instanceof EmbeddingResult) {
            EmbeddingResult matchResult = (EmbeddingResult) mapResult;
            Map<String, String> metadata = matchResult.getMetadata();
            // embedding的modelId和id多了_字符的前缀
            String modelId = metadata.get("modelId") != null ? String.valueOf(metadata.get("modelId")).replace("_", "") : "";
            String id = metadata.get("id") != null ? String.valueOf(metadata.get("id")).replace("_", "") : "";
            log.info("match=[embedding] model=[{}] id=[{}] type=[{}] name=[{}] detectWord=[{}] similarity=[{}]",
                    modelId, id, metadata.get("type"),
                    matchResult.getName(), matchResult.getDetectWord(), matchResult.getSimilarity());
        }
    }
}
