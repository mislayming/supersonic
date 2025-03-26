package com.tencent.supersonic.headless.chat.mapper;

import com.hankcs.hanlp.seg.common.Term;
import com.tencent.supersonic.headless.api.pojo.enums.MapModeEnum;
import com.tencent.supersonic.headless.chat.ChatQueryContext;
import com.tencent.supersonic.headless.chat.knowledge.MapResult;
import com.tencent.supersonic.headless.chat.knowledge.helper.HanlpHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

@Service
@Slf4j
public abstract class BaseMatchStrategy<T extends MapResult> implements MatchStrategy<T> {

    @Autowired
    protected MapperConfig mapperConfig;
    
    // 英文常见功能词集合，这些词通常对语义匹配贡献较小
    private static final Set<String> ENGLISH_STOP_WORDS = new HashSet<>(Arrays.asList(
            // 冠词
            "a", "an", "the",
            // 人称代词
            "i", "you", "he", "she", "it", "we", "they", "me", "him", "her", "us", "them", 
            "my", "your", "his", "its", "our", "their", "mine", "yours", "hers", "ours", "theirs",
            // 指示代词
            "this", "that", "these", "those",
            // 常见介词
            "in", "on", "at", "by", "with", "for", "to", "from", "of", "about", "against", 
            "between", "during", "without", "before", "after", "above", "below", "under", 
            "over", "up", "down",
            // 连词
            "and", "or", "but", "so", "because", "if", "though", "although", "unless", "since",
            // 助动词
            "am", "is", "are", "was", "were", "be", "been", "being",
            "have", "has", "had", "having",
            "do", "does", "did", "doing",
            "can", "could", "shall", "should", "will", "would", "may", "might", "must",
            // 其他常见功能词
            "not", "no", "very", "too", "as", "than", "then", "when", "where", "why", "how",
            "all", "any", "both", "each", "few", "more", "most", "some", "such", "only", "own",
            "same", "just", "even", "also"
    ));

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
    
    /**
     * 处理分词结果，过滤不需要的词项
     * 
     * @param allTerms 原始分词结果
     * @return 过滤后的词项列表
     */
    protected List<Term> filterTerms(List<Term> allTerms) {
        List<Term> termList = new ArrayList<>();
        Set<String> skipTerm = new HashSet<>();
        for (Term term : allTerms) {
            // 1. 过滤过短的词 (长度小于2)
            if (term.word.length() < 2) {
                continue;
            }
            
            // 2. 过滤纯空格、标点符号等
            if (term.word.trim().isEmpty() || isPunctuation(term.word)) {
                continue;
            }
            
            // 3. 过滤不关注的词性
            String nature = term.nature != null ? term.nature.toString() : "";
            if (isIgnorableNature(nature)) {
                continue;
            }
            
            // 4. 过滤英文功能词（大小写不敏感）
            if (isEnglishStopWord(term.word)) {
                continue;
            }

            String duplicateKey = term.word.toLowerCase() + "_" + (term.nature.startsWith("_") ? -1 : term.offset);
            if (!skipTerm.contains(duplicateKey)) {
                termList.add(term);
                skipTerm.add(duplicateKey);
            }
        }
        return termList;
    }
    
    /**
     * 检查是否是英文功能词
     */
    protected boolean isEnglishStopWord(String word) {
        // 检查词是否为纯英文
        if (word.matches("[a-zA-Z]+")) {
            return ENGLISH_STOP_WORDS.contains(word.toLowerCase());
        }
        return false;
    }
    
    /**
     * 生成n-gram短语
     * 
     * @param text 原始文本
     * @param termList 词项列表
     * @param detectSegments 用于添加结果的集合
     */
    protected void generateNGrams(String text, List<Term> termList, Set<String> detectSegments) {
        if (termList.size() >= 2) {
            // 生成n-gram短语，最多5-gram
            for (int n = 2; n <= 5 && n <= termList.size(); n++) {
                for (int i = 0; i <= termList.size() - n; i++) {
                    try {
                        // 获取首尾term
                        Term firstTerm = termList.get(i);
                        Term lastTerm = termList.get(i + n - 1);
                        
                        // 计算起始和结束位置
                        int startOffset = firstTerm.offset;
                        int endOffset = lastTerm.offset + lastTerm.length();
                        
                        // 安全检查
                        if (startOffset < 0 || endOffset > text.length() || startOffset >= endOffset) {
                            continue;
                        }
                        
                        String phrase = text.substring(startOffset, endOffset);
                        if (phrase.trim().isEmpty()) {
                            continue;
                        }
                        
                        // 检查内容质量
                        double contentRatio = calculateContentRatio(phrase, termList, i, n);
                        if (contentRatio >= 0.5) { // 有效内容超过50%
                            detectSegments.add(phrase);
                        }
                    } catch (Exception e) {
                        log.warn("生成n-gram过程中出现错误: {}", e.getMessage());
                    }
                }
            }
        }
    }
    
    /**
     * 获取分词结果
     * 
     * @param text 待分词文本
     * @return 分词结果
     */
    protected List<Term> getTerms(String text) {
        return HanlpHelper.getSegment().seg(text.toLowerCase());
    }
    
    /**
     * 计算短语中有效内容的比例
     */
    protected double calculateContentRatio(String phrase, List<Term> termList, int startIndex, int count) {
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
    protected boolean isPunctuation(String word) {
        return word.matches("[\\p{Punct}\\s]+");
    }
    
    /**
     * 检查是否是可忽略的词性
     */
    protected boolean isIgnorableNature(String nature) {
        // 标点、助词、语气词、叹词等
        Set<String> ignorableNatures = new HashSet<>(Arrays.asList(
                "w", "u", "uj", "ul", "e", "y", "o"
        ));
        
        for (String ignore : ignorableNatures) {
            if (nature.startsWith(ignore)) {
                return true;
            }
        }
        
        return false;
    }
}
