package com.tencent.supersonic.headless.core.adaptor.db;

import com.google.common.collect.Lists;
import com.tencent.supersonic.common.pojo.Constants;
import com.tencent.supersonic.common.pojo.enums.TimeDimensionEnum;
import com.tencent.supersonic.headless.api.pojo.DBColumn;
import com.tencent.supersonic.headless.api.pojo.enums.FieldType;
import com.tencent.supersonic.headless.core.pojo.ConnectInfo;
import com.tencent.supersonic.headless.core.pojo.Ontology;
import com.tencent.supersonic.headless.core.pojo.QueryStatement;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Metric;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;
import org.apache.commons.collections.CollectionUtils;

import java.util.*;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
public class H2Adaptor extends BaseDbAdaptor {

    @Override
    public String getDateFormat(String dateType, String dateFormat, String column) {
        if (dateFormat.equalsIgnoreCase(Constants.DAY_FORMAT_INT)) {
            if (TimeDimensionEnum.MONTH.name().equalsIgnoreCase(dateType)) {
                return "FORMATDATETIME(PARSEDATETIME(%s, 'yyyyMMdd'),'yyyy-MM')".replace("%s",
                        column);
            } else if (TimeDimensionEnum.WEEK.name().equalsIgnoreCase(dateType)) {
                return "DATE_TRUNC('week',%s)".replace("%s", column);
            } else {
                return "FORMATDATETIME(PARSEDATETIME(%s, 'yyyyMMdd'),'yyyy-MM-dd')".replace("%s",
                        column);
            }
        } else if (dateFormat.equalsIgnoreCase(Constants.DAY_FORMAT)) {
            if (TimeDimensionEnum.MONTH.name().equalsIgnoreCase(dateType)) {
                return "FORMATDATETIME(PARSEDATETIME(%s, 'yyyy-MM-dd'),'yyyy-MM') ".replace("%s",
                        column);
            } else if (TimeDimensionEnum.WEEK.name().equalsIgnoreCase(dateType)) {
                return "DATE_TRUNC('week',%s)".replace("%s", column);
            } else {
                return column;
            }
        }
        return column;
    }

    protected ResultSet getResultSet(String schemaName, DatabaseMetaData metaData)
            throws SQLException {
        return metaData.getTables(schemaName, null, null, new String[] {"TABLE", "VIEW"});
    }

    public List<DBColumn> getColumns(ConnectInfo connectInfo, String schemaName, String tableName)
            throws SQLException {
        List<DBColumn> dbColumns = Lists.newArrayList();
        DatabaseMetaData metaData = getDatabaseMetaData(connectInfo);
        ResultSet columns = metaData.getColumns(schemaName, null, tableName, null);
        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            String dataType = columns.getString("TYPE_NAME");
            String remarks = columns.getString("REMARKS");
            FieldType fieldType = classifyColumnType(dataType);
            dbColumns.add(new DBColumn(columnName, dataType, remarks, fieldType));
        }
        return dbColumns;
    }

    @Override
    public String rewriteSql(QueryStatement qstate) {

        try {
            // 解析SQL语句
            Statement statement = CCJSqlParserUtil.parse(qstate.getSql());
            
            // 创建一个用于收集修改后SQL的StringBuilder
            StringBuilder buffer = new StringBuilder();
            
            // 创建一个自定义的ExpressionDeParser来处理表达式
            ExpressionDeParser expressionDeParser = new ExpressionDeParser() {
                
                // 定义需要处理的聚合函数名称
                private final Set<String> aggregateFunctions = Set.of("AVG", "SUM", "MAX", "MIN", "COUNT");
                
                @Override
                public void visit(Function function) {
                    // 检查是否是我们要处理的聚合函数
                    if (aggregateFunctions.contains(function.getName().toUpperCase())) {
                        // 获取函数参数
                        ExpressionList<Expression> parameters = function.getParameters();
                        if (CollectionUtils.isNotEmpty(parameters)) {


                            // 获取第一个参数
                            Expression param = parameters.get(0);
                            if(param instanceof Column column) {
                                // TODO 这个 type 并不是数据类型，而是指指标是否是原子指标之类的。。。。 \
                                //  整体设计上有交大的问题，需要大调整。 这里算了，不管三七十一都直接加吧 ，仅限 H2
                                /*
                                Ontology ontology = qstate.getOntology();
                                Metric metric = ontology.getMetrics().stream().filter(t -> t.getName().equalsIgnoreCase(column.getColumnName())).findAny().orElse(null);
                                String type = metric.getType();
                                */

                                CastExpression cast = new CastExpression();
                                cast.setLeftExpression(column);
                                cast.setColDataType(new ColDataType("DECIMAL"));  // 使用正确的setColDataType方法

                                // 替换原参数为CAST表达式
                                parameters.set(0, cast);
                            }
                        }
                    }

                    super.visit(function);
                }
            };
            
            // 创建SelectDeParser，并传入自定义的ExpressionDeParser
            SelectDeParser selectDeParser = new SelectDeParser(expressionDeParser, buffer);
            expressionDeParser.setSelectVisitor(selectDeParser);
            expressionDeParser.setBuffer(buffer);

            // 创建StatementDeParser并使用它来重写语句
            StatementDeParser statementDeParser = new StatementDeParser(expressionDeParser, selectDeParser, buffer);
            statement.accept(statementDeParser);
            
            String rewrittenSql = buffer.toString();
            
            log.info("Rewritten SQL: {}", rewrittenSql);
            return rewrittenSql;
        } catch (JSQLParserException e) {
            log.error("Failed to parse SQL for rewriting, returning original SQL", e);
            return qstate.getSql();
        }
    }

    public static void main(String[] args) {
        H2Adaptor adaptor = new H2Adaptor();
        String sql = """
                WITH
                  "t_1" AS (
                    SELECT
                      "cartoon0"."Production_code",
                      "tv_series0"."Weekly_Rank",
                      "tv_series0"."Rating",
                      "tv_series0"."Viewers_m",
                      "tv_series0"."Share"
                    FROM
                      "tv_series" AS "tv_series0"
                      INNER JOIN (
                        "cartoon" AS "cartoon0"
                        INNER JOIN "tv_channel" AS "tv_channel0" ON "cartoon0"."Channel" = "tv_channel0"."id"
                      ) ON "tv_series0"."Channel" = "tv_channel0"."id"
                  ),
                  "_agg" AS (
                    SELECT
                      "Production_code",
                      AVG("Rating") AS "_平均评分"
                    FROM
                      "t_1"
                    GROUP BY
                      "Production_code"
                  )
                SELECT
                  "Production_code"
                FROM
                  "_agg"
                ORDER BY
                  "_平均评分" DESC
                LIMIT
                  3
                """;

        QueryStatement statement = new QueryStatement();
        statement.setSql(sql);
        adaptor.rewriteSql(statement);
    }
}
