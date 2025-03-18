package com.tencent.supersonic.headless.server.listener;

import com.tencent.supersonic.common.config.EmbeddingConfig;
import com.tencent.supersonic.common.pojo.DataEvent;
import com.tencent.supersonic.common.pojo.DataItem;
import com.tencent.supersonic.common.pojo.enums.EventType;
import com.tencent.supersonic.common.service.EmbeddingService;
import com.tencent.supersonic.headless.api.pojo.response.ModelResp;
import com.tencent.supersonic.headless.server.service.ModelService;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.store.embedding.TextSegmentConvert;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Component
@Slf4j
public class MetaEmbeddingListener {

    @Autowired
    private EmbeddingConfig embeddingConfig;

    @Autowired
    private EmbeddingService embeddingService;

    @Value("${s2.embedding.operation.sleep.time:3000}")
    private Integer embeddingOperationSleepTime;

    @Autowired
    private ModelService modelService;

    @Async // TODO
    @EventListener
    public void onApplicationEvent(DataEvent event) {
        List<DataItem> dataItems = event.getDataItems();
        if (CollectionUtils.isEmpty(dataItems)) {
            return;
        }


        Map<String, ModelResp> modelMap = dataItems.stream().map(t -> Long.parseLong(t.getModelId()))
                .distinct()
                .parallel()
                .map(t -> modelService.getModel(t))
                .filter(Objects::nonNull) // 过滤掉可能的 null 值
                .collect(Collectors.toMap(model -> model.getId().toString(), model -> model));

        dataItems.stream().forEach(t -> {
            ModelResp rsp = modelMap.get(t.getModelId());
            if(rsp != null) {
                t.setName(rsp.getName() + "_" + t.getName());
            }
        });

        List<TextSegment> textSegments = TextSegmentConvert.convertToEmbedding(dataItems);
        if (CollectionUtils.isEmpty(textSegments)) {
            return;
        }
        sleep();
        if (event.getEventType().equals(EventType.ADD)) {
            embeddingService.addQuery(embeddingConfig.getMetaCollectionName(), textSegments);
        } else if (event.getEventType().equals(EventType.DELETE)) {
            embeddingService.deleteQuery(embeddingConfig.getMetaCollectionName(), textSegments);
        } else if (event.getEventType().equals(EventType.UPDATE)) {
            embeddingService.deleteQuery(embeddingConfig.getMetaCollectionName(), textSegments);
            embeddingService.addQuery(embeddingConfig.getMetaCollectionName(), textSegments);
        }
    }

    private void sleep() {
        try {
            Thread.sleep(embeddingOperationSleepTime);
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }
}
