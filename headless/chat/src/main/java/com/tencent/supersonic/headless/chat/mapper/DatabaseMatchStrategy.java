package com.tencent.supersonic.headless.chat.mapper;

import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SchemaElementMatch;
import com.tencent.supersonic.headless.chat.ChatQueryContext;
import com.tencent.supersonic.headless.chat.knowledge.DatabaseMapResult;
import com.tencent.supersonic.headless.chat.utils.EditDistanceUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DatabaseMatchStrategy uses SQL LIKE operator to match schema elements. It currently supports
 * fuzzy matching against names and aliases.
 */
@Service
@Slf4j
public class DatabaseMatchStrategy extends SingleMatchStrategy<DatabaseMapResult> {

    private final ThreadLocal<List<SchemaElement>> allElements =
            ThreadLocal.withInitial(ArrayList::new);

    @Override
    public Map<MatchText, List<DatabaseMapResult>> match(ChatQueryContext chatQueryContext,
            Set<Long> detectDataSetIds) {
        allElements.set(getSchemaElements(chatQueryContext));
        return super.match(chatQueryContext, detectDataSetIds);
    }

    public List<DatabaseMapResult> detectByStep(ChatQueryContext chatQueryContext,
            Set<Long> detectDataSetIds, String detectSegment, int offset) {
        if (StringUtils.isBlank(detectSegment)) {
            return new ArrayList<>();
        }

        double metricDimensionThresholdConfig = getThreshold(chatQueryContext);
        Map<String, Set<SchemaElement>> nameToItems = getNameToItems(allElements.get());
        List<DatabaseMapResult> results = new ArrayList<>();
        for (Entry<String, Set<SchemaElement>> entry : nameToItems.entrySet()) {
            String name = entry.getKey();
            double similarity = EditDistanceUtils.getSimilarity(detectSegment, name);
            if (!name.contains(detectSegment) || similarity < metricDimensionThresholdConfig) {
                continue;
            }
            Set<SchemaElement> schemaElements = entry.getValue();
            if (!CollectionUtils.isEmpty(detectDataSetIds)) {
                schemaElements = schemaElements.stream().filter(
                        schemaElement -> detectDataSetIds.contains(schemaElement.getDataSetId()))
                        .collect(Collectors.toSet());
            }
            for (SchemaElement schemaElement : schemaElements) {
                DatabaseMapResult databaseMapResult = new DatabaseMapResult();
                databaseMapResult.setDetectWord(detectSegment);
                databaseMapResult.setName(schemaElement.getName());
                databaseMapResult.setSimilarity(similarity);
                databaseMapResult.setSchemaElement(schemaElement);
                results.add(databaseMapResult);
            }
        }
        return results;
    }

    private List<SchemaElement> getSchemaElements(ChatQueryContext chatQueryContext) {
        List<SchemaElement> allElements = new ArrayList<>();
        allElements.addAll(chatQueryContext.getSemanticSchema().getDimensions());
        allElements.addAll(chatQueryContext.getSemanticSchema().getMetrics());
        return allElements;
    }

    private Double getThreshold(ChatQueryContext chatQueryContext) {
        double threshold = Double
                .parseDouble(mapperConfig.getParameterValue(MapperConfig.MAPPER_NAME_THRESHOLD));
        double minThreshold = Double.parseDouble(
                mapperConfig.getParameterValue(MapperConfig.MAPPER_NAME_THRESHOLD_MIN));

        Map<Long, List<SchemaElementMatch>> modelElementMatches =
                chatQueryContext.getMapInfo().getDataSetElementMatches();

        boolean existElement = modelElementMatches.entrySet().stream()
                .anyMatch(entry -> !entry.getValue().isEmpty());

        if (!existElement) {
            threshold = threshold / 2;
            log.debug("ModelElementMatches:{},not exist Element threshold reduce by half:{}",
                    modelElementMatches, threshold);
        }

        return getThreshold(threshold, minThreshold,
                chatQueryContext.getRequest().getMapModeEnum());
    }

    private Map<String, Set<SchemaElement>> getNameToItems(List<SchemaElement> models) {
        return models.stream().collect(Collectors.toMap(SchemaElement::getName, a -> {
            Set<SchemaElement> result = new HashSet<>();
            result.add(a);
            return result;
        }, (k1, k2) -> {
            k1.addAll(k2);
            return k1;
        }));
    }
}
