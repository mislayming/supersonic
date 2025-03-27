package com.tencent.supersonic.common.calcite;

import com.tencent.supersonic.common.pojo.enums.EngineType;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserDefaultVisitor;
import net.sf.jsqlparser.parser.CCJSqlParserTreeConstants;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.SimpleNode;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.calcite.sql.parser.impl.SqlParserImplConstants;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static net.sf.jsqlparser.parser.CCJSqlParserTreeConstants.*;

/**
 * SQL标识符引号处理工具类 用于自动为SQL中的关键字添加适当的引号
 */
@Slf4j
public class SqlIdentifierQuoteUtil {

    /** SQL关键字集合，从Calcite解析器中提取 */
    private static final Set<String> KEYWORDS = Arrays.stream(SqlParserImplConstants.tokenImage)
            .map(v -> v.replace("\"", "")).collect(Collectors.toSet());

    public static String addQuotesToSql(String sql) {
        return addQuotesToSql(sql, EngineType.POSTGRESQL);
    }


    /**
     * 为SQL中的关键字添加引号
     * 
     * @param sql 原始SQL字符串
     * @param engineType 数据库引擎类型
     * @return 处理后的SQL字符串
     */
    public static String addQuotesToSql(String sql, EngineType engineType) {
        String quoteChar = SqlDialectFactory.getSqlDialect(engineType).getIdentifierQuoteString();
        return addQuotesToSql(sql, quoteChar);
    }

    /**
     * 为SQL中的关键字添加指定的引号字符
     * 
     * @param sql 原始SQL字符串
     * @param quoteChar 引号字符
     * @return 处理后的SQL字符串
     */
    public static String addQuotesToSql(String sql, String quoteChar) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }

        try {
            CCJSqlParser parser = CCJSqlParserUtil.newParser(sql);
            Statement stmt = parser.Statement();
            parser.getASTRoot().jjtAccept(new CCJSqlParserDefaultVisitor() {
                @Override
                public Object visit(SimpleNode node, Object data) {
                    Object value = node.jjtGetValue();
                    if (node.getId() == JJTTABLENAME) {
                        Table table = ((Table) value);
                        table.setName(wrapIfKeyword(table.getName(), quoteChar));
                    } else if (node.getId() == JJTCOLUMN) {
                        handleColumnExpression((Expression) value, quoteChar);
                    } else if (node.getId() == JJTSELECTITEM) {
                        SelectItem<?> selectItem = (SelectItem<?>) value;
                        handleColumnExpression(selectItem.getExpression(), quoteChar);
                        if (selectItem.getAlias() != null) {
                            handleAlias(selectItem.getAlias(), quoteChar);
                        }
                    }
                    return super.visit(node, data);
                }
            }, null);
            return stmt.toString();
        } catch (Exception e) {
            log.warn("SQL引号处理失败，返回原始SQL: {}", e.getMessage());
            if (log.isDebugEnabled()) {
                log.debug("原始SQL: {}", sql);
                log.debug("异常详情:", e);
            }
            return sql;
        }
    }

    /**
     * 为关键字添加引号
     */
    private static String wrapIfKeyword(String name, String quoteChar) {
        if (name == null || name.isEmpty()) {
            return name;
        }

        // 检查名称是否已经被引号包围
        if (name.startsWith(quoteChar) && name.endsWith(quoteChar)) {
            return name; // 已经有引号，不再添加
        }

        if (KEYWORDS.contains(name.toUpperCase())) {
            return quoteChar + name + quoteChar;
        }
        return name;
    }

    /**
     * 处理列表达式
     */
    private static void handleColumnExpression(Expression expression, String quoteChar) {
        if (expression instanceof Column column) {
            column.setColumnName(wrapIfKeyword(column.getColumnName(), quoteChar));
        } else if (expression instanceof CastExpression castExpression) {
            // 仅处理Cast表达式中的列引用，不做类型转换
            if (castExpression.getLeftExpression() instanceof Column) {
                handleColumnExpression(castExpression.getLeftExpression(), quoteChar);
            }
        }
    }

    /**
     * 处理别名
     */
    private static void handleAlias(Alias alias, String quoteChar) {
        alias.setName(wrapIfKeyword(alias.getName(), quoteChar));
    }
}
