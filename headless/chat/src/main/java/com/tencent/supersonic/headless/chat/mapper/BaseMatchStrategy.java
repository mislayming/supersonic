package com.tencent.supersonic.headless.chat.mapper;

import com.tencent.supersonic.headless.api.pojo.enums.MapModeEnum;
import com.tencent.supersonic.headless.chat.ChatQueryContext;
import com.tencent.supersonic.headless.chat.knowledge.MapResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

@Service
@Slf4j
public abstract class BaseMatchStrategy<T extends MapResult> implements MatchStrategy<T> {

    @Override
    public Map<MatchText, List<T>> match(ChatQueryContext chatQueryContext,
            Set<Long> detectDataSetIds) {

        String text = chatQueryContext.getRequest().getQueryText();
        List<T> detects = detect(chatQueryContext, detectDataSetIds);
        Map<MatchText, List<T>> result = new HashMap<>();

        result.put(MatchText.builder().regText(text).detectSegment(text).build(), detects);
        return result;
    }

    public List<T> detect(ChatQueryContext chatQueryContext, Set<Long> detectDataSetIds) {
        throw new RuntimeException("Not implemented");
    }

    public void selectResultInOneRound(Set<T> existResults, List<T> oneRoundResults) {
        if (CollectionUtils.isEmpty(oneRoundResults)) {
            return;
        }
        for (T oneRoundResult : oneRoundResults) {
            if (existResults.contains(oneRoundResult)) {
                boolean isDeleted = existResults.removeIf(existResult -> {
                    boolean delete = existResult.lessSimilar(oneRoundResult);
                    if (delete) {
                        log.info("deleted existResult:{}", existResult);
                    }
                    return delete;
                });
                if (isDeleted) {
                    log.info("deleted, add oneRoundResult:{}", oneRoundResult);
                    existResults.add(oneRoundResult);
                }
            } else {
                existResults.add(oneRoundResult);
            }
        }
    }


    public double getThreshold(Double threshold, Double minThreshold, MapModeEnum mapModeEnum) {
        if (MapModeEnum.STRICT.equals(mapModeEnum)) {
            return 1.0d;
        }
        double decreaseAmount = (threshold - minThreshold) / 4;
        double divideThreshold = threshold - mapModeEnum.threshold * decreaseAmount;
        return divideThreshold >= minThreshold ? divideThreshold : minThreshold;
    }
}
