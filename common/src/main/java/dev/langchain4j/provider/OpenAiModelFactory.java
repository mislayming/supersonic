package dev.langchain4j.provider;

import com.tencent.supersonic.common.pojo.ChatModelConfig;
import com.tencent.supersonic.common.pojo.EmbeddingModelConfig;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.time.Duration;

@Service
public class OpenAiModelFactory implements ModelFactory, InitializingBean {

    public static final String PROVIDER = "OPEN_AI";
    public static final String DEFAULT_BASE_URL = "https://api.openai.com/v1";
    public static final String DEFAULT_MODEL_NAME = "gpt-4o-mini";
    public static final String DEFAULT_EMBEDDING_MODEL_NAME = "text-embedding-ada-002";

    @Autowired
    private Environment env;

    @Override
    public ChatLanguageModel createChatModel(ChatModelConfig modelConfig) {
        // 增加HTTP代理
        String proxyHost = env.getProperty("integration.supersonic.http-proxy-host");
        String proxyPort = env.getProperty("integration.supersonic.http-proxy-port");
        Proxy httpProxy = null;
        if (StringUtils.isNotBlank(proxyHost) && StringUtils.isNotBlank(proxyPort)) {
            httpProxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)));
        }
        return OpenAiChatModel.builder().baseUrl(modelConfig.getBaseUrl())
                .modelName(modelConfig.getModelName()).apiKey(modelConfig.keyDecrypt())
                .temperature(modelConfig.getTemperature()).topP(modelConfig.getTopP())
                .maxRetries(modelConfig.getMaxRetries())
                .timeout(Duration.ofSeconds(modelConfig.getTimeOut()))
                .logRequests(modelConfig.getLogRequests())
                .logResponses(modelConfig.getLogResponses())
                .proxy(httpProxy)
                .build();
    }

    @Override
    public EmbeddingModel createEmbeddingModel(EmbeddingModelConfig embeddingModel) {
        return OpenAiEmbeddingModel.builder().baseUrl(embeddingModel.getBaseUrl())
                .apiKey(embeddingModel.getApiKey()).modelName(embeddingModel.getModelName())
                .maxRetries(embeddingModel.getMaxRetries())
                .logRequests(embeddingModel.getLogRequests())
                .logResponses(embeddingModel.getLogResponses()).build();
    }

    @Override
    public void afterPropertiesSet() {
        ModelProvider.add(PROVIDER, this);
    }
}
