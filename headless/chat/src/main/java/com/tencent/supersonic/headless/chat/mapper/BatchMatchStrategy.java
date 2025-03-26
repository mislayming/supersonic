package com.tencent.supersonic.headless.chat.mapper;

import com.hankcs.hanlp.seg.common.Term;
import com.tencent.supersonic.headless.chat.ChatQueryContext;
import com.tencent.supersonic.headless.chat.knowledge.MapResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Slf4j
public abstract class BatchMatchStrategy<T extends MapResult> extends BaseMatchStrategy<T> {

    @Override
    public List<T> detect(ChatQueryContext chatQueryContext, Set<Long> detectDataSetIds) {
        String text = chatQueryContext.getRequest().getQueryText();
        Set<String> detectSegments = new HashSet<>();

        // 使用基类方法获取并过滤分词结果
        List<Term> allTerms = getTerms(text);
        List<Term> termList = filterTerms(allTerms);

        // 添加单个词项到检测集合
        for (Term term : termList) {
            detectSegments.add(term.word);
        }

        // 使用基类方法生成n-gram短语
        generateNGrams(text, termList, detectSegments);
        return detectByBatch(chatQueryContext, detectDataSetIds, detectSegments);

        /*
        List<T> retList = detectByBatch(chatQueryContext, detectDataSetIds, detectSegments);
        if (!retList.isEmpty()) {
            return retList;
        }


        // 如果分词结果为空，使用传统的滑动窗口方法作为备选
        detectSegments.clear();
        int embeddingTextSize = Integer.parseInt(mapperConfig.getParameterValue(MapperConfig.EMBEDDING_MAPPER_TEXT_SIZE));
        int embeddingTextStep = Integer.parseInt(mapperConfig.getParameterValue(MapperConfig.EMBEDDING_MAPPER_TEXT_STEP));

        for (int startIndex = 0; startIndex < text.length(); startIndex += embeddingTextStep) {
            int endIndex = Math.min(startIndex + embeddingTextSize, text.length());
            String detectSegment = text.substring(startIndex, endIndex).trim();
            if (!detectSegment.isEmpty()) {
                detectSegments.add(detectSegment);
            }
        }

        return detectByBatch(chatQueryContext, detectDataSetIds, detectSegments);
         */
    }

    public abstract List<T> detectByBatch(ChatQueryContext chatQueryContext,
                                          Set<Long> detectDataSetIds, Set<String> detectSegments);
}
