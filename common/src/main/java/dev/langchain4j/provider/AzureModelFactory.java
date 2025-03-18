package dev.langchain4j.provider;

import com.azure.core.http.ProxyOptions;
import com.tencent.supersonic.common.pojo.ChatModelConfig;
import com.tencent.supersonic.common.pojo.EmbeddingModelConfig;
import dev.langchain4j.model.azure.AzureOpenAiChatModel;
import dev.langchain4j.model.azure.AzureOpenAiEmbeddingModel;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.embedding.EmbeddingModel;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.time.Duration;

@Service
public class AzureModelFactory implements ModelFactory, InitializingBean {
    public static final String PROVIDER = "AZURE";
    public static final String DEFAULT_BASE_URL = "https://your-resource-name.openai.azure.com/";
    public static final String DEFAULT_MODEL_NAME = "gpt-35-turbo";
    public static final String DEFAULT_EMBEDDING_MODEL_NAME = "text-embedding-ada-002";

    @Autowired
    private Environment env;

    @Override
    public ChatLanguageModel createChatModel(ChatModelConfig modelConfig) {
        AzureOpenAiChatModel.Builder builder = AzureOpenAiChatModel.builder()
                .endpoint(modelConfig.getBaseUrl()).apiKey(modelConfig.getApiKey())
                .deploymentName(modelConfig.getModelName())
                .temperature(modelConfig.getTemperature()).maxRetries(modelConfig.getMaxRetries())
                .topP(modelConfig.getTopP())
                .timeout(Duration.ofSeconds(
                        modelConfig.getTimeOut() == null ? 0L : modelConfig.getTimeOut()))
                .logRequestsAndResponses(
                        modelConfig.getLogRequests() != null && modelConfig.getLogResponses());
        // 增加HTTP代理
        String proxyHost = env.getProperty("integration.supersonic.http-proxy-host");
        String proxyPort = env.getProperty("integration.supersonic.http-proxy-port");
        if (StringUtils.isNotBlank(proxyHost) && StringUtils.isNotBlank(proxyPort)) {
            builder.proxyOptions(new ProxyOptions(ProxyOptions.Type.HTTP, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort))));
        }
        return builder.build();
    }

    @Override
    public EmbeddingModel createEmbeddingModel(EmbeddingModelConfig embeddingModelConfig) {
        AzureOpenAiEmbeddingModel.Builder builder =
                AzureOpenAiEmbeddingModel.builder().endpoint(embeddingModelConfig.getBaseUrl())
                        .apiKey(embeddingModelConfig.getApiKey())
                        .deploymentName(embeddingModelConfig.getModelName())
                        .maxRetries(embeddingModelConfig.getMaxRetries())
                        .logRequestsAndResponses(embeddingModelConfig.getLogRequests() != null
                                && embeddingModelConfig.getLogResponses());
        return builder.build();
    }

    @Override
    public void afterPropertiesSet() {
        ModelProvider.add(PROVIDER, this);
    }
}
