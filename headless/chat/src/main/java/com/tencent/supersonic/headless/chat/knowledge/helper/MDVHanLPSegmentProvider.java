package com.tencent.supersonic.headless.chat.knowledge.helper;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.model.perceptron.PerceptronLexicalAnalyzer;
import com.hankcs.hanlp.seg.Segment;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MDVHanLPSegmentProvider implements HanLPSegmentProvider {

    @Autowired
    private ResourceLoader resourceLoader;

    public static final String FILE_CWS = "data/model/perceptron/pku199801/cws.bin";
    public static final String FILE_POS = "data/model/perceptron/pku199801/pos.bin";

    @Override
    public Segment getSegment() {
        try {
            return new PerceptronLexicalAnalyzer(getFilePath(FILE_CWS), getFilePath(FILE_POS));
        } catch (Exception e) {
            log.error("读取HandLP文件失败。", e);
        }
        return HanLP.newSegment();
    }

    /***
     * 文件读取，兼容正式运行环境（jar包方式）和IDE中开发环境。
     */
    @SneakyThrows
    private String getFilePath(String path) {
        Resource resource = resourceLoader.getResource("file:" + path);
        if (resource.exists()) {
            return resource.getFile().getPath();
        }
        return resourceLoader.getResource("classpath:" + path).getFile().getPath();
    }
}
