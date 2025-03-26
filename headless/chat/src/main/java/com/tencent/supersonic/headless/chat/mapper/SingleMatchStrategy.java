package com.tencent.supersonic.headless.chat.mapper;

import com.hankcs.hanlp.seg.common.Term;
import com.tencent.supersonic.headless.api.pojo.response.S2Term;
import com.tencent.supersonic.headless.chat.ChatQueryContext;
import com.tencent.supersonic.headless.chat.knowledge.MapResult;
import com.tencent.supersonic.headless.chat.knowledge.helper.HanlpHelper;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public abstract class SingleMatchStrategy<T extends MapResult> extends BaseMatchStrategy<T> {
    @Autowired
    protected MapperConfig mapperConfig;
    @Autowired
    protected MapperHelper mapperHelper;

    public List<T> detect(ChatQueryContext chatQueryContext, Set<Long> detectDataSetIds) {
        String text = chatQueryContext.getRequest().getQueryText();
        Set<T> results = ConcurrentHashMap.newKeySet();
        List<Task> tasks = new ArrayList<>();

        // 使用基类方法获取并过滤分词结果
        List<Term> allTerms = getTerms(text);
        List<Term> termList = filterTerms(allTerms);

        // 检测单个词项
        for (Term term : termList) {
            int offset = term.getOffset();
            tasks.add(Task.builder().detect(term.word).offset(offset).build());
        }
        
        // 如果过滤后没有足够的词来构成n-gram，直接返回
        if (termList.size() < 2) {
            return executeTasks(chatQueryContext, detectDataSetIds, tasks);
        }
        
        // 生成n-gram短语并添加到任务列表中
        Set<String> ngramPhrases = new HashSet<>();
        generateNGrams(text, termList, ngramPhrases);
        
        // 为每个n-gram短语创建任务
        for (String phrase : ngramPhrases) {
            // 获取短语的第一个词的offset作为短语的offset
            for (int i = 0; i <= termList.size() - 2; i++) {
                Term firstTerm = termList.get(i);
                int startOffset = firstTerm.offset;
                if (text.substring(startOffset).startsWith(phrase)) {
                    tasks.add(Task.builder().detect(phrase).offset(startOffset).build());
                    break;
                }
            }
        }

        return executeTasks(chatQueryContext, detectDataSetIds, tasks);
    }

    // public List<T> _detect(ChatQueryContext chatQueryContext, List<S2Term> terms,
    // Set<Long> detectDataSetIds) {
    // // 获取在 query 原文中的 offset 位置, 因为这个 term 是从原文中分词出来的
    // Map<Integer, Integer> regOffsetToLength = mapperHelper.getRegOffsetToLength(terms);
    // String text = chatQueryContext.getRequest().getQueryText();
    // Set<T> results = ConcurrentHashMap.newKeySet();
    // List<Callable<Void>> tasks = new ArrayList<>();
    //
    // for (int startIndex = 0; startIndex <= text.length() - 1;) {
    // for (int index = startIndex; index <= text.length();) {
    //
    // // 永远返回startIndex 在两个terms offset中间时的,前一个位置.
    // // 比如 A-offset <= startIndex < B-offset, 此时取值 A-offset
    // int offset = mapperHelper.getStepOffset(terms, startIndex);
    //
    // // 跳过命中的切分词长度, 直接 substring 到命中的长度
    // // ▽ startIndex
    // // 1,2,3,4,5,6,7,8,9
    // // ^ index
    // // ^ 此位置刚好有个单词命中且长度为 4
    // // △ 所以返回 8, 这样后续切割就从 2~8
    // // △ 如果没命中, 则返回 index + 1, 在这个例子里就是切割 2~5
    // index = mapperHelper.getStepIndex(regOffsetToLength, index);
    // if (index <= text.length()) {
    // String detectSegment = text.substring(startIndex, index).trim();
    // Callable<Void> task = createTask(chatQueryContext, detectDataSetIds,
    // detectSegment, offset, results);
    // tasks.add(task);
    // }
    // }
    // startIndex = mapperHelper.getStepIndex(regOffsetToLength, startIndex);
    // }
    // executeTasks(tasks);
    // return new ArrayList<>(results);
    // }

    @Data
    @Builder
    public static class Task {
        private String detect;
        private int offset;
    }

    protected List<T> executeTasks(ChatQueryContext chatQueryContext, Set<Long> detectDataSetIds,
            List<Task> tasks) {
        Set<T> results = ConcurrentHashMap.newKeySet();
        tasks.stream().parallel().forEach(task -> {
            List<T> oneRoundResults = detectByStep(chatQueryContext, detectDataSetIds,
                    task.getDetect(), task.getOffset());
            synchronized (results) {
                selectResultInOneRound(results, oneRoundResults);
            }
        });

        return new ArrayList<>(results);
    }

    public abstract List<T> detectByStep(ChatQueryContext chatQueryContext,
            Set<Long> detectDataSetIds, String detectSegment, int offset);
}
