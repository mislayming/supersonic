package com.tencent.supersonic.headless.core.translator;

import com.tencent.supersonic.common.calcite.*;
import com.tencent.supersonic.common.pojo.enums.EngineType;
import com.tencent.supersonic.headless.core.pojo.Ontology;
import com.tencent.supersonic.headless.core.pojo.QueryStatement;
import com.tencent.supersonic.headless.core.pojo.SqlQuery;
import com.tencent.supersonic.headless.core.translator.optimizer.QueryOptimizer;
import com.tencent.supersonic.headless.core.translator.parser.QueryParser;
import com.tencent.supersonic.headless.core.translator.parser.calcite.RuntimeOptions;
import com.tencent.supersonic.headless.core.translator.parser.calcite.S2CalciteSchema;
import com.tencent.supersonic.headless.core.translator.parser.calcite.SchemaBuilder;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.SemanticNode;
import com.tencent.supersonic.headless.core.utils.ComponentFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.schema.Table;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@Slf4j
public class DefaultSemanticTranslator implements SemanticTranslator {

    private static final Logger keyPipelineLog = LoggerFactory.getLogger("keyPipeline");

    public void translate(QueryStatement queryStatement) {
        if (queryStatement.isTranslated()) {
            return;
        }
        try {
            for (QueryParser parser : ComponentFactory.getQueryParsers()) {
                if (parser.accept(queryStatement)) {
                    log.debug("QueryConverter accept [{}]", parser.getClass().getName());
                    parser.parse(queryStatement);
                    String innerSQL = StringUtils.replace(queryStatement.getSql(), "\n", " ");
                    String querySQL = StringUtils.replace(queryStatement.getSqlQuery().getSql(), "\n", " ");
                    String placeholder = "                         ";
                    keyPipelineLog.info(
                            "\t\t {} translate parser[{}] -> \n{}\t\t\t\t - querySQL: {} \n{}\t\t\t\t - innerSQL: {}",
                            parser.getClass().getSimpleName(), queryStatement.isOk(), placeholder,
                            querySQL, placeholder, innerSQL);
                    if (queryStatement.getStatus() != 0) {
                        break;
                    }
                } else {
                    keyPipelineLog.info("\t\t {} translate parser[{}] didn't accept",
                            parser.getClass().getSimpleName(), queryStatement.isOk());
                }
            }
            if (!queryStatement.isOk()) {
                throw new Exception(String.format("parse ontology table [%s] error [%s]",
                        queryStatement.getSqlQuery().getTable(), queryStatement.getErrMsg()));
            }

            mergeOntologyQuery(queryStatement);

            if (StringUtils.isNotBlank(queryStatement.getSqlQuery().getSimplifiedSql())) {
                queryStatement.setSql(queryStatement.getSqlQuery().getSimplifiedSql());
            }
            if (StringUtils.isBlank(queryStatement.getSql())) {
                throw new RuntimeException("parse exception: " + queryStatement.getErrMsg());
            }

            for (QueryOptimizer optimizer : ComponentFactory.getQueryOptimizers()) {
                if (optimizer.accept(queryStatement)) {
                    optimizer.rewrite(queryStatement);
                    keyPipelineLog.info("\t\t {} translate optimizer -> {}",
                            optimizer.getClass().getSimpleName(),
                            StringUtils.replace(queryStatement.getSql(), "\n", " "));
                } else {
                    keyPipelineLog.info("\t\t {} translate optimizer didn't accept",
                            optimizer.getClass().getSimpleName());
                }
            }
            log.info("translated query SQL: [{}]",
                    StringUtils.normalizeSpace(queryStatement.getSql()));
        } catch (Exception e) {
            queryStatement.setErrMsg(e.getMessage());
            keyPipelineLog.error("Failed to translate query [{}]", e.getMessage(), e);
        }
    }

    private void mergeOntologyQuery(QueryStatement queryStatement) throws Exception {
        SqlQuery sqlQuery = queryStatement.getSqlQuery();
        String ontologyInnerTable = sqlQuery.getTable();
        String ontologyQuerySql = SqlIdentifierQuoteUtil.addQuotesToSql(sqlQuery.getSql());
        String ontologyInnerSql = SqlIdentifierQuoteUtil.addQuotesToSql(queryStatement.getSql());

        List<Pair<String, String>> tables = new ArrayList<>();
        tables.add(Pair.of(ontologyInnerTable, ontologyInnerSql));
        if (sqlQuery.isSupportWith()) {
            EngineType engineType = queryStatement.getOntology().getDatabaseType();
            if (!SqlMergeWithUtils.hasWith(engineType, ontologyQuerySql)) {
                String withSql = "with " + tables.stream()
                        .map(t -> String.format("%s as (%s)", t.getLeft(), t.getRight()))
                        .collect(Collectors.joining(",")) + "\n" + ontologyQuerySql;
                queryStatement.setSql(withSql);
            } else {
                List<String> withTableList =
                        tables.stream().map(Pair::getLeft).collect(Collectors.toList());
                List<String> withSqlList =
                        tables.stream().map(Pair::getRight).collect(Collectors.toList());
                String mergeSql = SqlMergeWithUtils.mergeWith(engineType, ontologyQuerySql,
                        withSqlList, withTableList);
                queryStatement.setSql(mergeSql);
            }
        } else {
            for (Pair<String, String> tb : tables) {
                ontologyQuerySql = StringUtils.replace(ontologyQuerySql, tb.getLeft(),
                        "(" + tb.getRight() + ") " + (sqlQuery.isWithAlias() ? "" : tb.getLeft()),
                        -1);
            }
            queryStatement.setSql(ontologyQuerySql);
        }


        String schemaQualify = "DATASET_" + queryStatement.getDataSetId();
        queryStatement.setSql(replaceQualify(queryStatement, schemaQualify, queryStatement.getOntology().getDatabaseType()));
    }


    private String replaceQualify(QueryStatement statement, String schemaQualify, EngineType engineType) {
        SemanticSqlDialect dialect = SqlDialectFactory.getSqlDialect(engineType);

        String quoteString = dialect.getIdentifierQuoteString();

        // 确保引号字符串不为空，默认使用反引号
        if (quoteString == null || quoteString.isEmpty()) {
            quoteString = "`";
        }

        String schemaField = quoteString + schemaQualify + quoteString + ".";
        return StringUtils.replace(statement.getSql(), schemaField, "");
    }

}
