package com.tencent.supersonic.headless.server.task;

import com.tencent.supersonic.common.config.EmbeddingConfig;
import com.tencent.supersonic.common.pojo.DataItem;
import com.tencent.supersonic.common.service.EmbeddingService;
import com.tencent.supersonic.headless.api.pojo.response.ModelResp;
import com.tencent.supersonic.headless.server.service.DimensionService;
import com.tencent.supersonic.headless.server.service.MetricService;
import com.tencent.supersonic.headless.server.service.ModelService;
import dev.langchain4j.inmemory.spring.InMemoryEmbeddingStoreFactory;
import dev.langchain4j.store.embedding.EmbeddingStoreFactory;
import dev.langchain4j.store.embedding.EmbeddingStoreFactoryProvider;
import dev.langchain4j.store.embedding.TextSegmentConvert;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@Slf4j
@Order(2)
public class MetaEmbeddingTask implements CommandLineRunner {

    @Autowired
    private EmbeddingService embeddingService;

    @Autowired
    private EmbeddingConfig embeddingConfig;

    @Autowired
    private MetricService metricService;

    @Autowired
    private DimensionService dimensionService;

    @Autowired
    private ModelService modelService;

    @PreDestroy
    public void onShutdown() {
        embeddingStorePersistFile();
    }

    private void embeddingStorePersistFile() {
        EmbeddingStoreFactory embeddingStoreFactory = EmbeddingStoreFactoryProvider.getFactory();
        if (embeddingStoreFactory instanceof InMemoryEmbeddingStoreFactory) {
            long startTime = System.currentTimeMillis();
            InMemoryEmbeddingStoreFactory inMemoryFactory =
                    (InMemoryEmbeddingStoreFactory) embeddingStoreFactory;
            inMemoryFactory.persistFile();
            long duration = System.currentTimeMillis() - startTime;
            log.info("Embedding file has been regularly persisted in {} milliseconds", duration);
        }
    }

    @Scheduled(cron = "${s2.inMemoryEmbeddingStore.persist.cron:0 0 * * * ?}")
    public void executePersistFileTask() {
        embeddingStorePersistFile();
    }

    /** * reload meta embedding */
    @Scheduled(cron = "${s2.reload.meta.embedding.corn:0 0 */2 * * ?}")
    public void reloadMetaEmbedding() {
        long startTime = System.currentTimeMillis();
        try {
            List<DataItem> metricDataItems = metricService.getDataEvent().getDataItems();
            List<DataItem> dimensionDataItems = dimensionService.getAllDataEvents().getDataItems();


            Map<String, ModelResp> modelMap = Stream.concat(
                            metricDataItems.stream().map(t -> Long.parseLong(t.getModelId())),
                            dimensionDataItems.stream().map(t -> Long.parseLong(t.getModelId()))
                    )
                    .distinct()
                    .parallel()
                    .map(t -> modelService.getModel(t))
                    .filter(Objects::nonNull) // 过滤掉可能的 null 值
                    .collect(Collectors.toMap(model -> model.getId().toString(), model -> model));


            metricDataItems.stream().forEach(t -> {
                ModelResp rsp = modelMap.get(t.getModelId());
                if(rsp != null) {
                    t.setName(rsp.getName() + "_" + t.getName());
                }
            });

            embeddingService.addQuery(embeddingConfig.getMetaCollectionName(),
                    TextSegmentConvert.convertToEmbedding(metricDataItems));


            dimensionDataItems.stream().forEach(t -> {
                ModelResp rsp = modelMap.get(t.getModelId());
                if(rsp != null) {
                    t.setName(rsp.getName() + "_" + t.getName());
                }
            });
            embeddingService.addQuery(embeddingConfig.getMetaCollectionName(),
                    TextSegmentConvert.convertToEmbedding(dimensionDataItems));
        } catch (Exception e) {
            log.error("Failed to reload meta embedding.", e);
        }
        long duration = System.currentTimeMillis() - startTime;
        log.info("Embedding has been regularly reloaded  in {} milliseconds", duration);
    }

    @Override
    public void run(String... args) throws Exception {
        try {
            reloadMetaEmbedding();
        } catch (Exception e) {
            log.error("initMetaEmbedding error", e);
        }
    }
}
