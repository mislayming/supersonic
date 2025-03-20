package com.tencent.supersonic.demo;  

import com.alibaba.fastjson.JSONObject;  
import com.google.common.collect.Lists;  
import com.google.common.collect.Maps;  
import com.tencent.supersonic.chat.server.agent.Agent;  
import com.tencent.supersonic.chat.server.agent.AgentToolType;  
import com.tencent.supersonic.chat.server.agent.DatasetTool;  
import com.tencent.supersonic.chat.server.agent.ToolConfig;  
import com.tencent.supersonic.common.pojo.ChatApp;  
import com.tencent.supersonic.common.pojo.JoinCondition;  
import com.tencent.supersonic.common.pojo.ModelRela;  
import com.tencent.supersonic.common.pojo.enums.*;  
import com.tencent.supersonic.common.util.ChatAppManager;  
import com.tencent.supersonic.headless.api.pojo.*;  
import com.tencent.supersonic.headless.api.pojo.enums.DimensionType;  
import com.tencent.supersonic.headless.api.pojo.enums.IdentifyType;  
import com.tencent.supersonic.headless.api.pojo.request.DataSetReq;  
import com.tencent.supersonic.headless.api.pojo.request.DomainReq;  
import com.tencent.supersonic.headless.api.pojo.request.ModelReq;  
import com.tencent.supersonic.headless.api.pojo.response.DataSetResp;  
import com.tencent.supersonic.headless.api.pojo.response.DatabaseResp;  
import com.tencent.supersonic.headless.api.pojo.response.DomainResp;  
import com.tencent.supersonic.headless.api.pojo.response.ModelResp;  
import lombok.extern.slf4j.Slf4j;  
import org.springframework.core.annotation.Order;  
import org.springframework.stereotype.Component;  

import java.util.*;  

@Component  
@Slf4j  
@Order(3)  
public class S2TvChannelDemo extends S2BaseDemo {  

    public static final String AGENT_NAME = "TVChannelAssistant";

    public void doRun() {  
        try {  
            DomainResp domain = addDomain();  

            ModelResp model_tv_channel = addTvChannelModel(domain, demoDatabase);  
            ModelResp model_cartoon = addCartoonModel(domain, demoDatabase);  
            ModelResp model_tv_series = addTvSeriesModel(domain, demoDatabase);  

            addModelRela(domain, model_cartoon, model_tv_channel, "Channel");  
            addModelRela(domain, model_tv_series, model_tv_channel, "Channel");  

            DataSetResp dataset = addDataSet(domain);  
            Long dataSetId = dataset.getId();  
            addAgent(dataSetId);  
        } catch (Exception e) {  
            log.error("Failed to add TV channel demo data", e);  
        }  
    }  

    @Override  
    protected boolean checkNeedToRun() {  
        List<DomainResp> domainList = domainService.getDomainList();  
        for (DomainResp domainResp : domainList) {  
            if (domainResp.getBizName().equalsIgnoreCase("television")) {  
                log.info("Already exist domain:television, no need to run demo");  
                return false;  
            }  
        }  
        return true;  
    }  

    public DomainResp addDomain() {  
        DomainReq domainReq = new DomainReq();  
        domainReq.setName("TelevisionDomain");
        domainReq.setBizName("television");  
        domainReq.setParentId(0L);  
        domainReq.setViewers(Arrays.asList("admin", "tom", "jack"));  
        domainReq.setViewOrgs(Collections.singletonList("1"));  
        domainReq.setAdmins(Collections.singletonList("admin"));  
        domainReq.setAdminOrgs(Collections.emptyList());  
        return domainService.createDomain(domainReq, defaultUser);  
    }  

    public ModelResp addTvChannelModel(DomainResp domain, DatabaseResp database) throws Exception {  
        ModelReq modelReq = new ModelReq();  
        modelReq.setName("TVChannel");
        modelReq.setBizName("tv_channel");  
        modelReq.setDatabaseId(database.getId());  
        modelReq.setDomainId(domain.getId());  
        modelReq.setViewers(Arrays.asList("admin", "tom", "jack"));  
        modelReq.setViewOrgs(Collections.singletonList("1"));  
        modelReq.setAdmins(Collections.singletonList("admin"));  
        modelReq.setAdminOrgs(Collections.emptyList());  
        ModelDetail modelDetail = new ModelDetail();  
        List<Dimension> dimensions = new ArrayList<>();  
        modelDetail.setDimensions(dimensions);  

        dimensions.add(new Dimension("ChannelName", "series_name", DimensionType.categorical, 1));
        dimensions.add(new Dimension("Country", "Country", DimensionType.categorical, 1));  
        dimensions.add(new Dimension("Language", "Language", DimensionType.categorical, 1));  
        dimensions.add(new Dimension("Content", "Content", DimensionType.categorical, 1));  
        dimensions.add(new Dimension("PixelAspectRatio", "Pixel_aspect_ratio_PAR", DimensionType.categorical, 1));
        dimensions.add(new Dimension("HighDefinitionTV", "Hight_definition_TV", DimensionType.categorical, 1));
        dimensions.add(new Dimension("PayPerView", "Pay_per_view_PPV", DimensionType.categorical, 1));
        dimensions.add(new Dimension("PackageOption", "Package_Option", DimensionType.categorical, 1));

        List<Identify> identifiers = new ArrayList<>();  
        identifiers.add(new Identify("ChannelID", IdentifyType.primary.name(), "id"));
        modelDetail.setIdentifiers(identifiers);  

        List<Measure> measures = new ArrayList<>();  
        modelDetail.setMeasures(measures);  

        modelDetail.setQueryType("sql_query");  
        modelDetail.setSqlQuery("SELECT * FROM tv_channel");  
        modelReq.setModelDetail(modelDetail);  
        ModelResp tvChannelModel = modelService.createModel(modelReq, defaultUser);  

        enableDimensionValue(getDimension("series_name", tvChannelModel));  
        enableDimensionValue(getDimension("Country", tvChannelModel));  
        enableDimensionValue(getDimension("Content", tvChannelModel));  

        return tvChannelModel;  
    }  

    public ModelResp addCartoonModel(DomainResp domain, DatabaseResp database) throws Exception {  
        ModelReq modelReq = new ModelReq();  
        modelReq.setName("Cartoon");  
        modelReq.setBizName("cartoon");  
        modelReq.setDatabaseId(database.getId());  
        modelReq.setDomainId(domain.getId());  
        modelReq.setViewers(Arrays.asList("admin", "tom", "jack"));  
        modelReq.setViewOrgs(Collections.singletonList("1"));  
        modelReq.setAdmins(Collections.singletonList("admin"));  
        modelReq.setAdminOrgs(Collections.emptyList());  
        ModelDetail modelDetail = new ModelDetail();  
        List<Dimension> dimensions = new ArrayList<>();  
        modelDetail.setDimensions(dimensions);  

        dimensions.add(new Dimension("Title", "Title", DimensionType.categorical, 1));  
        dimensions.add(new Dimension("DirectedBy", "Directed_by", DimensionType.categorical, 1));
        dimensions.add(new Dimension("WrittenBy", "Written_by", DimensionType.categorical, 1));
        dimensions.add(new Dimension("OriginalAirDate", "Original_air_date", DimensionType.time, 1));
        dimensions.add(new Dimension("ProductionCode", "Production_code", DimensionType.categorical, 1));

        List<Identify> identifiers = new ArrayList<>();  
        identifiers.add(new Identify("CartoonID", IdentifyType.primary.name(), "id"));
        identifiers.add(new Identify("ChannelID", IdentifyType.foreign.name(), "Channel"));
        modelDetail.setIdentifiers(identifiers);  

        List<Measure> measures = new ArrayList<>();  
        modelDetail.setMeasures(measures);  

        modelDetail.setQueryType("sql_query");  
        modelDetail.setSqlQuery("SELECT * FROM cartoon");  
        modelReq.setModelDetail(modelDetail);  
        ModelResp cartoonModel = modelService.createModel(modelReq, defaultUser);  

        enableDimensionValue(getDimension("Title", cartoonModel));  
        enableDimensionValue(getDimension("Directed_by", cartoonModel));  

        return cartoonModel;  
    }  

    public ModelResp addTvSeriesModel(DomainResp domain, DatabaseResp database) throws Exception {  
        ModelReq modelReq = new ModelReq();  
        modelReq.setName("TVSeries");
        modelReq.setBizName("tv_series");  
        modelReq.setDatabaseId(database.getId());  
        modelReq.setDomainId(domain.getId());  
        modelReq.setViewers(Arrays.asList("admin", "tom", "jack"));  
        modelReq.setViewOrgs(Collections.singletonList("1"));  
        modelReq.setAdmins(Collections.singletonList("admin"));  
        modelReq.setAdminOrgs(Collections.emptyList());  
        ModelDetail modelDetail = new ModelDetail();  
        List<Dimension> dimensions = new ArrayList<>();  
        modelDetail.setDimensions(dimensions);

        dimensions.add(new Dimension("Episode", "Episode", DimensionType.categorical, 1));  
        dimensions.add(new Dimension("AirDate", "Air_Date", DimensionType.time, 1));
        dimensions.add(new Dimension("18-49 Rating Share", "18_49_Rating_Share", DimensionType.categorical, 1));  

        List<Identify> identifiers = new ArrayList<>();  
        identifiers.add(new Identify("SeriesID", IdentifyType.primary.name(), "id"));
        identifiers.add(new Identify("ChannelID", IdentifyType.foreign.name(), "Channel"));
        modelDetail.setIdentifiers(identifiers);  

        List<Measure> measures = new ArrayList<>();  
        measures.add(new Measure("Rating", "Rating", AggOperatorEnum.AVG.name(), 1));  
        measures.add(new Measure("Share", "Share", AggOperatorEnum.AVG.name(), 1));  
        measures.add(new Measure("WeeklyRank", "Weekly_Rank", AggOperatorEnum.MIN.name(), 1));
        measures.add(new Measure("Viewers", "Viewers_m", AggOperatorEnum.SUM.name(), 1));
        modelDetail.setMeasures(measures);  

        modelDetail.setQueryType("sql_query");  
        modelDetail.setSqlQuery("SELECT * FROM tv_series");  
        modelReq.setModelDetail(modelDetail);  
        return modelService.createModel(modelReq, defaultUser);  
    }  

    public DataSetResp addDataSet(DomainResp domain) {  
        DataSetReq dataSetReq = new DataSetReq();  
        dataSetReq.setName("TVChannelDataset");
        dataSetReq.setBizName("TelevisionData");  
        dataSetReq.setDomainId(domain.getId());  
        dataSetReq.setDescription("Dataset for TV Channels, Cartoons and TV Series");  
        dataSetReq.setAdmins(Lists.newArrayList("admin"));  

        List<DataSetModelConfig> dataSetModelConfigs = getDataSetModelConfigs(domain.getId());  
        DataSetDetail dataSetDetail = new DataSetDetail();  
        dataSetDetail.setDataSetModelConfigs(dataSetModelConfigs);  
        dataSetReq.setDataSetDetail(dataSetDetail);  
        dataSetReq.setTypeEnum(TypeEnums.DATASET);  

        QueryConfig queryConfig = new QueryConfig();  
        AggregateTypeDefaultConfig aggregateTypeDefaultConfig = new AggregateTypeDefaultConfig();  
        TimeDefaultConfig timeDefaultConfig = new TimeDefaultConfig();  
        timeDefaultConfig.setTimeMode(TimeMode.LAST);  
        timeDefaultConfig.setUnit(1);  
        aggregateTypeDefaultConfig.setTimeDefaultConfig(timeDefaultConfig);  
        queryConfig.setAggregateTypeDefaultConfig(aggregateTypeDefaultConfig);  
        dataSetReq.setQueryConfig(queryConfig);  

        return dataSetService.save(dataSetReq, defaultUser);  
    }  

    public void addModelRela(DomainResp domain, ModelResp fromModel, ModelResp toModel,  
            String joinField) {  
        List<JoinCondition> joinConditions = Lists.newArrayList();  
        joinConditions.add(new JoinCondition(joinField, "id", FilterOperatorEnum.EQUALS));  
        ModelRela modelRelaReq = new ModelRela();  
        modelRelaReq.setDomainId(domain.getId());  
        modelRelaReq.setFromModelId(fromModel.getId());  
        modelRelaReq.setToModelId(toModel.getId());  
        modelRelaReq.setJoinType("inner join");  
        modelRelaReq.setJoinConditions(joinConditions);  
        modelRelaService.save(modelRelaReq, defaultUser);  
    }  

    private Agent addAgent(Long dataSetId) {  
        Agent agent = new Agent();  
        agent.setName(AGENT_NAME);  
        agent.setDescription("Help you analyze data about TV channels, cartoons, and TV series");  
        agent.setStatus(1);  
        agent.setEnableSearch(1);  
        agent.setExamples(  
                Lists.newArrayList("All cartoon shows on a specific channel", "Top three TV series by rating", "What English language channels are available", "Who directed a specific cartoon"));  
        ToolConfig toolConfig = new ToolConfig();  

        // configure tools  
        DatasetTool datasetTool = new DatasetTool();  
        datasetTool.setId("1");  
        datasetTool.setType(AgentToolType.DATASET);  
        datasetTool.setDataSetIds(Lists.newArrayList(dataSetId));  
        toolConfig.getTools().add(datasetTool);  
        agent.setToolConfig(JSONObject.toJSONString(toolConfig));  

        // configure chat apps  
        Map<String, ChatApp> chatAppConfig =  
                Maps.newHashMap(ChatAppManager.getAllApps(AppModule.CHAT));  
        chatAppConfig.values().forEach(app -> app.setChatModelId(demoChatModel.getId()));  
        agent.setChatAppConfig(chatAppConfig);  

        return agentService.createAgent(agent, defaultUser);  
    }  
}  