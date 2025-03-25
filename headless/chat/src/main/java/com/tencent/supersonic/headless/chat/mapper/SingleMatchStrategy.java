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

        // 直接利用分词,不要像原来一样无脑按照字符切割,效率不高的情况,在中英文混杂的领域更是可怕
        List<Term> allTerms = HanlpHelper.getSegment().seg(text.toLowerCase());
        Set<String> skipTerm = new HashSet<>();

        // 预处理：过滤掉不需要处理的词性和无效词
        List<Term> termList = new ArrayList<>();
        for (Term term : allTerms) {
            // 1. 过滤过短的词 (长度小于2)
            if (term.word.length() < 2) {
                continue;
            }

            // 2. 过滤纯空格、标点符号等
            if (term.word.trim().isEmpty() || isPunctuation(term.word)) {
                continue;
            }

            // 3. 过滤不关注的词性 (如标点、语气词、助词等)
            String nature = term.nature != null ? term.nature.toString() : "";
            if (isIgnorableNature(nature)) {
                continue;
            }

            String duplicateKey = term.word.toLowerCase() + "_" + term.offset;
            if (!skipTerm.contains(duplicateKey)) {
                termList.add(term);
                skipTerm.add(duplicateKey);
            }
        }


        // 检测单个词项 - 已经过滤了，不需要再次判断长度
        for (Term term : termList) {
            int offset = term.getOffset();
            tasks.add(Task.builder().detect(term.word).offset(offset).build());
        }

        // 如果过滤后没有足够的词来构成n-gram，直接返回
        if (termList.size() < 2) {
            return executeTasks(chatQueryContext, detectDataSetIds, tasks);
        }

        // 生成n-gram短语
        for (int n = 2; n <= 5 && n <= termList.size(); n++) {
            for (int i = 0; i <= termList.size() - n; i++) {
                try {
                    // 获取首尾term
                    Term firstTerm = termList.get(i);
                    Term lastTerm = termList.get(i + n - 1);

                    // 计算起始和结束位置
                    int startOffset = firstTerm.offset;
                    int endOffset = lastTerm.offset + lastTerm.length();

                    // 安全检查：确保索引有效且起始位置小于结束位置
                    if (startOffset < 0 || endOffset > text.length() || startOffset >= endOffset) {
                        continue; // 跳过无效的索引组合
                    }

                    // 检查这n个term是否在语义上连续 (考虑到我们过滤了某些词)
                    boolean isConsecutive = true;
                    String phrase = text.substring(startOffset, endOffset);
                    if (phrase.trim().isEmpty()) {
                        continue;
                    }

                    // 如果短语中包含太多非词内容，则认为不连续
                    double contentRatio = calculateContentRatio(phrase, termList, i, n);
                    if (contentRatio < 0.5) { // 如果有效内容少于50%，认为不连续
                        isConsecutive = false;
                    }

                    // 如果这n个词组成了有意义的短语
                    if (isConsecutive) {
                        tasks.add(Task.builder().detect(phrase).offset(startOffset).build());
                    }
                } catch (Exception e) {
                    // 记录异常但继续处理其他n-gram
                    log.warn("Error generating n-gram: {}", e.getMessage());
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

    /**
     * 计算短语中有效内容的比例
     */
    private double calculateContentRatio(String phrase, List<Term> termList, int startIndex,
            int count) {
        int totalLength = phrase.length();
        int contentLength = 0;

        for (int i = 0; i < count; i++) {
            Term term = termList.get(startIndex + i);
            contentLength += term.word.length();
        }

        return (double) contentLength / totalLength;
    }

    /**
     * 检查是否是标点符号或特殊字符
     */
    private boolean isPunctuation(String word) {
        return word.matches("[\\p{Punct}\\s]+");
    }

    /**
     * 检查是否是可忽略的词性 可根据业务需求调整忽略的词性列表
     */
    private boolean isIgnorableNature(String nature) {
        // 标点、助词、语气词、叹词等
        Set<String> ignorableNatures =
                new HashSet<>(Arrays.asList("w", "u", "uj", "ul", "e", "y", "o"));

        for (String ignore : ignorableNatures) {
            if (nature.startsWith(ignore)) {
                return true;
            }
        }

        return false;
    }
}
