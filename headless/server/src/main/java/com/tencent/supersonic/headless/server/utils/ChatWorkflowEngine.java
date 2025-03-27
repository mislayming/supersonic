package com.tencent.supersonic.headless.server.utils;

import com.tencent.supersonic.common.util.ContextUtils;
import com.tencent.supersonic.headless.api.pojo.SchemaElement;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.api.pojo.enums.ChatWorkflowState;
import com.tencent.supersonic.headless.api.pojo.request.SemanticQueryReq;
import com.tencent.supersonic.headless.api.pojo.response.ParseResp;
import com.tencent.supersonic.headless.api.pojo.response.SemanticTranslateResp;
import com.tencent.supersonic.headless.chat.ChatQueryContext;
import com.tencent.supersonic.headless.chat.corrector.SemanticCorrector;
import com.tencent.supersonic.headless.chat.mapper.SchemaMapper;
import com.tencent.supersonic.headless.chat.parser.SemanticParser;
import com.tencent.supersonic.headless.chat.query.QueryManager;
import com.tencent.supersonic.headless.chat.query.SemanticQuery;
import com.tencent.supersonic.headless.server.facade.service.SemanticLayerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ChatWorkflowEngine {

    private static final Logger keyPipelineLog = LoggerFactory.getLogger("keyPipeline");
    private final List<SchemaMapper> schemaMappers = CoreComponentFactory.getSchemaMappers();
    private final List<SemanticParser> semanticParsers = CoreComponentFactory.getSemanticParsers();
    private final List<SemanticCorrector> semanticCorrectors =
            CoreComponentFactory.getSemanticCorrectors();

    public void start(ChatWorkflowState initialState, ChatQueryContext queryCtx,
            ParseResp parseResult) {
        queryCtx.setChatWorkflowState(initialState);
        while (queryCtx.getChatWorkflowState() != ChatWorkflowState.FINISHED) {
            switch (queryCtx.getChatWorkflowState()) {
                case MAPPING:
                    performMapping(queryCtx);
                    if (queryCtx.getMapInfo().isEmpty()) {
                        parseResult.setState(ParseResp.ParseState.FAILED);
                        parseResult.setErrorMsg(
                                "No semantic entities can be mapped against user question.");
                        queryCtx.setChatWorkflowState(ChatWorkflowState.FINISHED);
                    } else {
                        queryCtx.setChatWorkflowState(ChatWorkflowState.PARSING);
                    }
                    break;
                case PARSING:
                    performParsing(queryCtx);
                    if (queryCtx.getCandidateQueries().isEmpty()) {
                        parseResult.setState(ParseResp.ParseState.FAILED);
                        parseResult.setErrorMsg("No semantic queries can be parsed out.");
                        queryCtx.setChatWorkflowState(ChatWorkflowState.FINISHED);
                    } else {
                        List<SemanticParseInfo> parseInfos = queryCtx.getCandidateQueries().stream()
                                .map(SemanticQuery::getParseInfo).collect(Collectors.toList());
                        parseResult.setSelectedParses(parseInfos);
                        if (queryCtx.needSQL()) {
                            queryCtx.setChatWorkflowState(ChatWorkflowState.CORRECTING);
                        } else {
                            parseResult.setState(ParseResp.ParseState.COMPLETED);
                            queryCtx.setChatWorkflowState(ChatWorkflowState.FINISHED);
                        }
                    }
                    break;
                case CORRECTING:
                    performCorrecting(queryCtx);
                    queryCtx.setChatWorkflowState(ChatWorkflowState.TRANSLATING);
                    break;
                case TRANSLATING:
                    long start = System.currentTimeMillis();
                    performTranslating(queryCtx, parseResult);
                    parseResult.getParseTimeCost().setSqlTime(System.currentTimeMillis() - start);
                    queryCtx.setChatWorkflowState(ChatWorkflowState.FINISHED);
                    break;
                default:
                    if (parseResult.getState().equals(ParseResp.ParseState.PENDING)) {
                        parseResult.setState(ParseResp.ParseState.COMPLETED);
                    }
                    queryCtx.setChatWorkflowState(ChatWorkflowState.FINISHED);
                    break;
            }
        }
    }

    private void performMapping(ChatQueryContext queryCtx) {
        keyPipelineLog.info("mapping stage:");
        if (Objects.isNull(queryCtx.getMapInfo())
                || MapUtils.isEmpty(queryCtx.getMapInfo().getDataSetElementMatches())) {
            schemaMappers.forEach(mapper -> {
                mapper.map(queryCtx);
                String tpl =
                        " dataset: {0} | {1}, model: {2} | {3} | {4} , detect: {5} | {6} , similarity: {7} ";

                String msg = queryCtx.getMapInfo().getDataSetElementMatches().values().stream()
                        .flatMap(List::stream).map(t -> {
                            SchemaElement el = t.getElement();
                            return MessageFormat.format(tpl, el.getDataSetId(), el.getDataSetName(), // dataset
                                    el.getModel(), el.getBizName(), el.getType(), // model
                                    t.getDetectWord(), t.getWord(), t.getSimilarity());
                        }).collect(Collectors.joining(",", "[", "]"));
                // queryCtx.getMapInfo().getDataSetElementMatches()
                keyPipelineLog.info("\t {} mapping -> {}", mapper.getClass().getSimpleName(), msg);
            });
        }
    }

    private void performParsing(ChatQueryContext queryCtx) {
        keyPipelineLog.info("parsing stage:");
        semanticParsers.forEach(parser -> {
            parser.parse(queryCtx);
            keyPipelineLog.info("\t {} parsing -> {}", parser.getClass().getSimpleName(),
                    queryCtx.getCandidateQueries().stream()
                            .map(t -> t.getParseInfo().getSqlInfo().getParsedS2SQL()).toList());
        });
    }

    private void performCorrecting(ChatQueryContext queryCtx) {
        keyPipelineLog.info("correcting stage:");
        List<SemanticQuery> candidateQueries = queryCtx.getCandidateQueries();
        if (CollectionUtils.isNotEmpty(candidateQueries)) {
            int max = candidateQueries.size();
            int i = 1;
            for (SemanticQuery semanticQuery : candidateQueries) {
                for (SemanticCorrector corrector : semanticCorrectors) {
                    String cls = corrector.getClass().getSimpleName();
                    corrector.correct(queryCtx, semanticQuery.getParseInfo());
                    keyPipelineLog.info("\t {} correcting[{}/{}] | {} -> {}", cls, i, max,
                            queryCtx.getChatWorkflowState(),
                            semanticQuery.getParseInfo().getSqlInfo().getCorrectedS2SQL());
                    if (!ChatWorkflowState.CORRECTING.equals(queryCtx.getChatWorkflowState())) {
                        break;
                    }
                }
                i++;
            }
        }
    }

    private void performTranslating(ChatQueryContext queryCtx, ParseResp parseResult) {
        keyPipelineLog.info("translating stage:");
        List<SemanticParseInfo> semanticParseInfos = queryCtx.getCandidateQueries().stream()
                .map(SemanticQuery::getParseInfo).toList();
        List<String> errorMsg = new ArrayList<>();
        if (StringUtils.isNotBlank(parseResult.getErrorMsg())) {
            errorMsg.add(parseResult.getErrorMsg());
        }

        int max = semanticParseInfos.size();
        AtomicInteger i = new AtomicInteger(1);

        semanticParseInfos.forEach(parseInfo -> {
            try {
                SemanticQuery semanticQuery = QueryManager.createQuery(parseInfo.getQueryMode());
                if (Objects.isNull(semanticQuery)) {
                    return;
                }
                semanticQuery.setParseInfo(parseInfo);
                SemanticQueryReq semanticQueryReq = semanticQuery.buildSemanticQueryReq();
                SemanticLayerService queryService = ContextUtils.getBean(SemanticLayerService.class);

                SemanticTranslateResp explain =
                        queryService.translate(semanticQueryReq, queryCtx.getRequest().getUser());
                if (explain.isOk()) {
                    parseInfo.getSqlInfo().setQuerySQL(explain.getQuerySQL());
                    parseResult.setState(ParseResp.ParseState.COMPLETED);
                } else {
                    parseResult.setState(ParseResp.ParseState.FAILED);
                }
                if (StringUtils.isNotBlank(explain.getErrMsg())) {
                    errorMsg.add(explain.getErrMsg());
                }


                keyPipelineLog.info("\t translating - {}[{}/{}] -> {}", parseResult.getState(), i,
                        max, StringUtils.normalizeSpace(parseInfo.getSqlInfo().getQuerySQL()));
                // log.info(
                // "SqlInfoProcessor results:\n"
                // + "Parsed S2SQL: {}\nCorrected S2SQL: {}\nQuery SQL: {}",
                // StringUtils.normalizeSpace(parseInfo.getSqlInfo().getParsedS2SQL()),
                // StringUtils.normalizeSpace(parseInfo.getSqlInfo().getCorrectedS2SQL()),
                // StringUtils.normalizeSpace(parseInfo.getSqlInfo().getQuerySQL()));
            } catch (Exception e) {
                log.warn("get sql info failed:{}", parseInfo, e);
                errorMsg.add(String.format("S2SQL:%s %s", parseInfo.getSqlInfo().getParsedS2SQL(),
                        e.getMessage()));
            }

            i.getAndIncrement();
        });
        if (!errorMsg.isEmpty()) {
            parseResult.setErrorMsg(String.join("\n", errorMsg));
        }
    }
}
