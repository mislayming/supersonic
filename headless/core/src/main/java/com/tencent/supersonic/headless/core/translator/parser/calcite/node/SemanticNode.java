package com.tencent.supersonic.headless.core.translator.parser.calcite.node;

import com.tencent.supersonic.common.calcite.Configuration;
import com.tencent.supersonic.common.calcite.SemanticSqlDialect;
import com.tencent.supersonic.common.calcite.SqlDialectFactory;
import com.tencent.supersonic.common.pojo.enums.EngineType;
import com.tencent.supersonic.headless.core.translator.parser.calcite.FilterToGroupScanRule;
import com.tencent.supersonic.headless.core.translator.parser.calcite.S2CalciteSchema;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.Litmus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** model item node */
@Slf4j
public abstract class SemanticNode {

    public static Set<SqlKind> AGGREGATION_KIND = new HashSet<>();
    public static Set<String> AGGREGATION_FUNC = new HashSet<>();
    public static List<String> groupHints =
            new ArrayList<>(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"));

    static {
        AGGREGATION_KIND.add(SqlKind.AVG);
        AGGREGATION_KIND.add(SqlKind.COUNT);
        AGGREGATION_KIND.add(SqlKind.SUM);
        AGGREGATION_KIND.add(SqlKind.MAX);
        AGGREGATION_KIND.add(SqlKind.MIN);
        AGGREGATION_KIND.add(SqlKind.OTHER_FUNCTION); // more
        AGGREGATION_FUNC.add("sum");
        AGGREGATION_FUNC.add("count");
        AGGREGATION_FUNC.add("max");
        AGGREGATION_FUNC.add("avg");
        AGGREGATION_FUNC.add("min");
    }

    public static SqlNode parse(String expression, SqlValidatorScope scope, EngineType engineType)
            throws Exception {
        SqlValidatorWithHints sqlValidatorWithHints = Configuration.getSqlValidatorWithHints(
                scope.getValidator().getCatalogReader().getRootSchema(), engineType);

        // 尝试直接解析表达式
        try {
            SqlParser sqlParser =
                    SqlParser.create(expression, Configuration.getParserConfig(engineType));
            SqlNode sqlNode = sqlParser.parseExpression();
            scope.validateExpr(sqlNode);
            return sqlNode;
        } catch (Exception e) {
            log.debug("Failed to parse expression [{}] directly: {}", expression, e.getMessage());

            // 如果直接解析失败，尝试处理可能的关键字问题
            String processedExpression =
                    processKeywordsInExpression(expression, sqlValidatorWithHints, engineType);

            if (!expression.equals(processedExpression)) {
                // 如果表达式被修改，尝试使用处理后的表达式解析
                try {
                    log.debug("Trying with processed expression: {}", processedExpression);
                    SqlParser sqlParser = SqlParser.create(processedExpression,
                            Configuration.getParserConfig(engineType));
                    SqlNode sqlNode = sqlParser.parseExpression();
                    scope.validateExpr(sqlNode);
                    return sqlNode;
                } catch (Exception ex) {
                    log.debug("Failed to parse processed expression [{}]: {}", processedExpression,
                            ex.getMessage());
                    // 如果处理后的表达式仍然失败，继续尝试原始方法
                }
            }

            // 尝试简单的字段处理方式
            // 检查是否包含 '.' 符号，表示是 alias.field 格式
            if (expression.contains(".")) {
                String[] parts = expression.split("\\.", 2);
                String alias = parts[0];
                String field = parts[1];

                // 检查字段名是否是关键字
                if (Configuration.getSqlAdvisor(sqlValidatorWithHints, engineType)
                        .getReservedAndKeyWords().contains(field.toUpperCase())) {
                    // 获取当前引擎的SQL方言和标识符引号
                    SemanticSqlDialect dialect = SqlDialectFactory.getSqlDialect(engineType);

                    String quoteString = dialect.getIdentifierQuoteString();

                    // 确保引号字符串不为空，默认使用反引号
                    if (quoteString == null || quoteString.isEmpty()) {
                        quoteString = "`";
                    }

                    String escapedExpression = alias + "." + quoteString + field + quoteString;

                    // 尝试解析转义后的表达式
                    try {
                        SqlParser sqlParser = SqlParser.create(escapedExpression,
                                Configuration.getParserConfig(engineType));
                        SqlNode sqlNode = sqlParser.parseExpression();
                        scope.validateExpr(sqlNode);
                        return sqlNode;
                    } catch (Exception ex) {
                        log.debug("Failed to parse escaped expression [{}]: {}", escapedExpression,
                                ex.getMessage());
                        // 如果仍然失败，继续尝试其他方法
                    }
                }
            }
            // 检查整个表达式是否是关键字
            else {
                // 获取当前引擎的SQL方言和标识符引号
                SemanticSqlDialect dialect = SqlDialectFactory.getSqlDialect(engineType);
                String quoteString = dialect.getIdentifierQuoteString();

                // 确保引号字符串不为空，默认使用反引号
                if (quoteString == null || quoteString.isEmpty()) {
                    quoteString = "`";
                }

                String escapedExpression = quoteString + expression + quoteString;

                // 尝试解析转义后的表达式
                try {
                    SqlParser sqlParser = SqlParser.create(escapedExpression,
                            Configuration.getParserConfig(engineType));
                    SqlNode sqlNode = sqlParser.parseExpression();
                    scope.validateExpr(sqlNode);
                    return sqlNode;
                } catch (Exception ex) {
                    log.debug("Failed to parse escaped expression [{}]: {}", escapedExpression,
                            ex.getMessage());
                    // 如果仍然失败，继续尝试其他方法
                }
            }

            // 如果所有尝试都失败，抛出原始异常
            log.error("Failed to parse expression [{}] after all attempts: {}", expression,
                    e.getMessage());
            throw e;
        }
    }

    /**
     * 处理表达式中的关键字 这个方法使用正则表达式识别可能的标识符和字段引用，然后检查它们是否是关键字并添加适当的转义
     */
    private static String processKeywordsInExpression(String expression,
            SqlValidatorWithHints validator, EngineType engineType) {

        // TODO 多半有 bug，expr 可能要结合指标表达式，但是现在暂时不管吧。。。。

        // 不处理空表达式
        if (StringUtils.isBlank(expression)) {
            return expression;
        }

        // 获取关键字列表
        Set<String> keywords = new HashSet<>(
                Configuration.getSqlAdvisor(validator, engineType).getReservedAndKeyWords());

        // 获取当前引擎的SQL方言和标识符引号
        SemanticSqlDialect dialect = SqlDialectFactory.getSqlDialect(engineType);
        String quoteString = dialect.getIdentifierQuoteString();

        // 确保引号字符串不为空，默认使用反引号
        if (quoteString == null || quoteString.isEmpty()) {
            quoteString = "`";
        }

        // 标记是否发生了任何变化
        boolean changed = false;

        // 1. 处理 tableName.fieldName 形式的字段引用
        // 正则表达式用于捕获 identifier.identifier 格式
        java.util.regex.Pattern tableFieldPattern =
                java.util.regex.Pattern.compile("([a-zA-Z0-9_]+)\\.([a-zA-Z0-9_]+)");
        java.util.regex.Matcher tableFieldMatcher = tableFieldPattern.matcher(expression);

        // 保存找到的匹配项及其位置
        List<int[]> matches = new ArrayList<>();
        while (tableFieldMatcher.find()) {
            String tableName = tableFieldMatcher.group(1);
            String fieldName = tableFieldMatcher.group(2);

            // 检查字段名是否是关键字
            if (keywords.contains(fieldName.toUpperCase())) {
                matches.add(new int[] {tableFieldMatcher.start(), tableFieldMatcher.end(),
                                tableFieldMatcher.start(2), tableFieldMatcher.end(2)});
            }
        }

        // 从后往前处理匹配项，以避免位置偏移问题
        if (!matches.isEmpty()) {
            matches.sort((a, b) -> Integer.compare(b[0], a[0]));

            // 应用转义
            String workingExpr = expression;
            for (int[] match : matches) {
                int fieldStart = match[2];
                int fieldEnd = match[3];

                String fieldName = workingExpr.substring(fieldStart, fieldEnd);
                String escapedField = quoteString + fieldName + quoteString;

                workingExpr = workingExpr.substring(0, fieldStart) + escapedField
                        + workingExpr.substring(fieldEnd);

                changed = true;
            }

            // 如果有变更，返回处理后的表达式
            if (changed) {
                return workingExpr;
            }
        }

        // 2. 处理函数调用中的关键字字段
        // 函数模式，例如 FUNCTION(arg1, arg2, ...)
        java.util.regex.Pattern funcPattern =
                java.util.regex.Pattern.compile("([a-zA-Z0-9_]+)\\s*\\(([^\\)]+)\\)");
        java.util.regex.Matcher funcMatcher = funcPattern.matcher(expression);

        while (funcMatcher.find()) {
            String funcName = funcMatcher.group(1);
            String argsStr = funcMatcher.group(2);

            // 递归处理函数参数
            String processedArgs = processKeywordsInExpression(argsStr, validator, engineType);

            if (!argsStr.equals(processedArgs)) {
                // 替换原始表达式中的函数参数
                expression = expression.substring(0, funcMatcher.start(2)) + processedArgs
                        + expression.substring(funcMatcher.end(2));

                changed = true;

                // 重新开始匹配，因为表达式已经改变
                funcMatcher = funcPattern.matcher(expression);
            }
        }

        // 3. 处理CASE表达式中的关键字
        // 这里只做一个简单的CASE WHEN pattern处理示例
        java.util.regex.Pattern casePattern = java.util.regex.Pattern.compile(
                "CASE\\s+WHEN\\s+(.+?)\\s+THEN\\s+(.+?)(?:\\s+ELSE\\s+(.+?))?\\s+END",
                java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher caseMatcher = casePattern.matcher(expression);

        while (caseMatcher.find()) {
            String whenCondition = caseMatcher.group(1);
            String thenExpr = caseMatcher.group(2);
            String elseExpr = caseMatcher.groupCount() > 2 ? caseMatcher.group(3) : null;

            // 递归处理WHEN条件和THEN表达式
            String processedWhen =
                    processKeywordsInExpression(whenCondition, validator, engineType);
            String processedThen = processKeywordsInExpression(thenExpr, validator, engineType);
            String processedElse =
                    elseExpr != null ? processKeywordsInExpression(elseExpr, validator, engineType)
                            : null;

            boolean caseChanged =
                    !whenCondition.equals(processedWhen) || !thenExpr.equals(processedThen)
                            || (elseExpr != null && !elseExpr.equals(processedElse));

            if (caseChanged) {
                // 重建CASE表达式
                StringBuilder newCaseExpr = new StringBuilder();
                newCaseExpr.append("CASE WHEN ").append(processedWhen).append(" THEN ")
                        .append(processedThen);

                if (processedElse != null) {
                    newCaseExpr.append(" ELSE ").append(processedElse);
                }

                newCaseExpr.append(" END");

                // 替换原始表达式中的CASE表达式
                expression = expression.substring(0, caseMatcher.start()) + newCaseExpr.toString()
                        + expression.substring(caseMatcher.end());

                changed = true;

                // 重新开始匹配，因为表达式已经改变
                caseMatcher = casePattern.matcher(expression);
            }
        }

        // 4. 处理括号中的表达式
        java.util.regex.Pattern parenPattern =
                java.util.regex.Pattern.compile("\\(([^\\(\\)]+)\\)");
        java.util.regex.Matcher parenMatcher = parenPattern.matcher(expression);

        while (parenMatcher.find()) {
            String innerExpr = parenMatcher.group(1);

            // 递归处理括号内的表达式
            String processedInner = processKeywordsInExpression(innerExpr, validator, engineType);

            if (!innerExpr.equals(processedInner)) {
                // 替换原始表达式中的括号内容
                expression = expression.substring(0, parenMatcher.start(1)) + processedInner
                        + expression.substring(parenMatcher.end(1));

                changed = true;

                // 重新开始匹配，因为表达式已经改变
                parenMatcher = parenPattern.matcher(expression);
            }
        }

        // 如果没有找到任何需要处理的关键字，返回原始表达式
        return expression;
    }

    public static SqlNode buildAs(String asName, SqlNode sqlNode) throws Exception {
        SqlAsOperator sqlAsOperator = new SqlAsOperator();
        SqlIdentifier sqlIdentifier = new SqlIdentifier(asName, SqlParserPos.ZERO);
        return new SqlBasicCall(sqlAsOperator,
                new ArrayList<>(Arrays.asList(sqlNode, sqlIdentifier)), SqlParserPos.ZERO);
    }

    public static String getSql(SqlNode sqlNode, EngineType engineType) {

        // 创建自定义SqlWriterConfig确保使用正确的引号
        UnaryOperator<SqlWriterConfig> sqlWriterConfigUnaryOperator =
                (c) -> Configuration.getSqlWriterConfig(engineType);

        // 获取SQL字符串
        return sqlNode.toSqlString(sqlWriterConfigUnaryOperator).getSql();
    }

    public static boolean isNumeric(String expr) {
        return StringUtils.isNumeric(expr);
    }

    public static List<SqlNode> expand(SqlNode sqlNode, SqlValidatorScope scope) throws Exception {
        if (!isIdentifier(sqlNode)) {
            List<SqlNode> sqlNodeList = new ArrayList<>();
            expand(sqlNode, sqlNodeList);
            return sqlNodeList;
        }
        return new ArrayList<>(Arrays.asList(sqlNode));
    }

    public static void expand(SqlNode sqlNode, List<SqlNode> sqlNodeList) {
        if (sqlNode instanceof SqlIdentifier) {
            sqlNodeList.add(sqlNode);
            return;
        }
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            for (SqlNode sqlNo : sqlBasicCall.getOperandList()) {
                expand(sqlNo, sqlNodeList);
            }
        }
    }

    public static boolean isIdentifier(SqlNode sqlNode) {
        return sqlNode instanceof SqlIdentifier;
    }

    public static SqlNode getAlias(SqlNode sqlNode, SqlValidatorScope scope) throws Exception {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (sqlBasicCall.getKind().equals(SqlKind.AS)
                    && sqlBasicCall.getOperandList().size() > 1) {
                return sqlBasicCall.getOperandList().get(1);
            }
        }
        if (sqlNode instanceof SqlIdentifier) {
            return sqlNode;
        }
        return null;
    }

    public static Set<String> getSelect(SqlNode sqlNode) {
        SqlNode table = getTable(sqlNode);
        if (table instanceof SqlSelect) {
            SqlSelect tableSelect = (SqlSelect) table;
            return tableSelect.getSelectList().stream()
                    .map(s -> (s instanceof SqlIdentifier) ? ((SqlIdentifier) s).names.get(0)
                            : (((s instanceof SqlBasicCall) && s.getKind().equals(SqlKind.AS))
                                    ? ((SqlBasicCall) s).getOperandList().get(1).toString()
                                    : ""))
                    .collect(Collectors.toSet());
        }
        return new HashSet<>();
    }

    public static SqlNode getTable(SqlNode sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (sqlBasicCall.getOperator().getKind().equals(SqlKind.AS)) {
                if (sqlBasicCall.getOperandList().get(0) instanceof SqlSelect) {
                    SqlSelect table = (SqlSelect) sqlBasicCall.getOperandList().get(0);
                    return table;
                }
            }
        }
        return sqlNode;
    }

    private static void sqlVisit(SqlNode sqlNode, Map<String, Object> parseInfo) {
        SqlKind kind = sqlNode.getKind();
        switch (kind) {
            case SELECT:
                queryVisit(sqlNode, parseInfo);
                break;
            case AS:
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                if (sqlBasicCall.getOperandList().get(0).getKind().equals(SqlKind.IDENTIFIER)) {
                    addTableName(sqlBasicCall.getOperandList().get(0).toString(),
                            sqlBasicCall.getOperandList().get(1).toString(), parseInfo);
                } else {
                    sqlVisit(sqlBasicCall.getOperandList().get(0), parseInfo);
                }
                break;
            case JOIN:
                SqlJoin sqlJoin = (SqlJoin) sqlNode;
                sqlVisit(sqlJoin.getLeft(), parseInfo);
                sqlVisit(sqlJoin.getRight(), parseInfo);
                SqlBasicCall condition = (SqlBasicCall) sqlJoin.getCondition();
                if (Objects.nonNull(condition)) {
                    condition.getOperandList().stream()
                            .forEach(c -> addTagField(c.toString(), parseInfo, ""));
                }
                break;
            case UNION:
                ((SqlBasicCall) sqlNode).getOperandList().forEach(node -> {
                    sqlVisit(node, parseInfo);
                });
                break;
            case WITH:
                SqlWith sqlWith = (SqlWith) sqlNode;
                sqlVisit(sqlWith.body, parseInfo);
                break;
            default:
                break;
        }
    }

    private static void queryVisit(SqlNode select, Map<String, Object> parseInfo) {
        if (select == null) {
            return;
        }
        SqlSelect sqlSelect = (SqlSelect) select;
        SqlNodeList selectList = sqlSelect.getSelectList();
        selectList.getList().forEach(list -> {
            fieldVisit(list, parseInfo, "");
        });
        fromVisit(sqlSelect.getFrom(), parseInfo);
        if (sqlSelect.hasWhere()) {
            whereVisit((SqlBasicCall) sqlSelect.getWhere(), parseInfo);
        }
        if (sqlSelect.hasOrderBy()) {
            fieldVisit(sqlSelect.getOrderList(), parseInfo, "");
        }
        SqlNodeList group = sqlSelect.getGroup();
        if (group != null) {
            group.forEach(groupField -> {
                if (groupHints.contains(groupField.toString())) {
                    int groupIdx = Integer.valueOf(groupField.toString()) - 1;
                    if (selectList.getList().size() > groupIdx) {
                        fieldVisit(selectList.get(groupIdx), parseInfo, "");
                    }
                } else {
                    fieldVisit(groupField, parseInfo, "");
                }
            });
        }
    }

    private static void whereVisit(SqlBasicCall where, Map<String, Object> parseInfo) {
        if (where == null) {
            return;
        }
        if (where.operandCount() == 2 && where.operand(0).getKind().equals(SqlKind.IDENTIFIER)
                && where.operand(1).getKind().equals(SqlKind.LITERAL)) {
            fieldVisit(where.operand(0), parseInfo, "");
            return;
        }
        // 子查询
        if (where.operandCount() == 2 && (where.operand(0).getKind().equals(SqlKind.IDENTIFIER)
                && (where.operand(1).getKind().equals(SqlKind.SELECT)
                        || where.operand(1).getKind().equals(SqlKind.ORDER_BY)))) {
            fieldVisit(where.operand(0), parseInfo, "");
            sqlVisit((SqlNode) (where.operand(1)), parseInfo);
            return;
        }
        if (CollectionUtils.isNotEmpty(where.getOperandList())
                && where.operand(0).getKind().equals(SqlKind.IDENTIFIER)) {
            fieldVisit(where.operand(0), parseInfo, "");
        }
        if (where.operandCount() >= 2 && where.operand(1).getKind().equals(SqlKind.IDENTIFIER)) {
            fieldVisit(where.operand(1), parseInfo, "");
        }
        if (CollectionUtils.isNotEmpty(where.getOperandList())
                && where.operand(0) instanceof SqlBasicCall) {
            whereVisit(where.operand(0), parseInfo);
        }
        if (where.operandCount() >= 2 && where.operand(1) instanceof SqlBasicCall) {
            whereVisit(where.operand(1), parseInfo);
        }
    }

    private static void fieldVisit(SqlNode field, Map<String, Object> parseInfo, String func) {
        if (field == null) {
            return;
        }
        SqlKind kind = field.getKind();
        // System.out.println(kind);
        // aggfunction
        if (AGGREGATION_KIND.contains(kind)) {
            SqlOperator sqlCall = ((SqlCall) field).getOperator();
            if (AGGREGATION_FUNC.contains(sqlCall.toString().toLowerCase())) {
                List<SqlNode> operandList = ((SqlBasicCall) field).getOperandList();
                for (int i = 0; i < operandList.size(); i++) {
                    fieldVisit(operandList.get(i), parseInfo, sqlCall.toString().toUpperCase());
                }
                return;
            }
        }
        if (kind.equals(SqlKind.IDENTIFIER)) {
            addTagField(field.toString(), parseInfo, func);
            return;
        }
        if (kind.equals(SqlKind.AS)) {
            List<SqlNode> operandList1 = ((SqlBasicCall) field).getOperandList();
            SqlNode left = operandList1.get(0);
            fieldVisit(left, parseInfo, "");
            return;
        }
        if (field instanceof SqlBasicCall) {
            List<SqlNode> operandList = ((SqlBasicCall) field).getOperandList();
            for (int i = 0; i < operandList.size(); i++) {
                fieldVisit(operandList.get(i), parseInfo, "");
            }
        }
        if (field instanceof SqlNodeList) {
            ((SqlNodeList) field).getList().forEach(node -> {
                fieldVisit(node, parseInfo, "");
            });
        }
    }

    private static void addTagField(String exp, Map<String, Object> parseInfo, String func) {

        if (!parseInfo.containsKey(Constants.SQL_PARSER_FIELD)) {
            parseInfo.put(Constants.SQL_PARSER_FIELD, new HashMap<>());
        }
        Map<String, Set<String>> fields =
                (Map<String, Set<String>>) parseInfo.get(Constants.SQL_PARSER_FIELD);

        if (Pattern.matches("(?i)[a-z\\d_\\.]+", exp)) {
            if (exp.contains(".")) {
                String[] res = exp.split("\\.");
                if (!fields.containsKey(res[0])) {
                    fields.put(res[0], new HashSet<>());
                }
                fields.get(res[0]).add(res[1]);
            } else {
                if (!fields.containsKey("")) {
                    fields.put("", new HashSet<>());
                }
                fields.get("").add(exp);
            }
        }
    }

    private static void fromVisit(SqlNode from, Map<String, Object> parseInfo) {
        SqlKind kind = from.getKind();
        switch (kind) {
            case IDENTIFIER:
                SqlIdentifier sqlIdentifier = (SqlIdentifier) from;
                addTableName(sqlIdentifier.toString(), "", parseInfo);
                break;
            case AS:
                SqlBasicCall sqlBasicCall = (SqlBasicCall) from;
                SqlNode selectNode0 = sqlBasicCall.getOperandList().get(0);
                SqlNode selectNode1 = sqlBasicCall.getOperandList().get(1);
                if (!SqlKind.UNION.equals(selectNode0.getKind())) {
                    if (!SqlKind.SELECT.equals(selectNode0.getKind())) {
                        addTableName(selectNode0.toString(), selectNode1.toString(), parseInfo);
                    }
                }
                sqlVisit(selectNode0, parseInfo);
                break;
            case JOIN:
                SqlJoin sqlJoin = (SqlJoin) from;
                sqlVisit(sqlJoin.getLeft(), parseInfo);
                sqlVisit(sqlJoin.getRight(), parseInfo);
                SqlBasicCall condition = (SqlBasicCall) sqlJoin.getCondition();
                if (Objects.nonNull(condition)) {
                    condition.getOperandList().stream()
                            .forEach(c -> addTagField(c.toString(), parseInfo, ""));
                }
                break;
            case SELECT:
                sqlVisit(from, parseInfo);
                break;
            default:
                break;
        }
    }

    private static void addTableName(String exp, String alias, Map<String, Object> parseInfo) {
        if (exp.indexOf(" ") > 0) {
            return;
        }
        if (!parseInfo.containsKey(Constants.SQL_PARSER_TABLE)) {
            parseInfo.put(Constants.SQL_PARSER_TABLE, new HashMap<>());
        }
        Map<String, Set<String>> dbTbs =
                (Map<String, Set<String>>) parseInfo.get(Constants.SQL_PARSER_TABLE);
        if (!dbTbs.containsKey(alias)) {
            dbTbs.put(alias, new HashSet<>());
        }
        dbTbs.get(alias).add(exp);
    }

    public static Map<String, Object> getDbTable(SqlNode sqlNode) {
        Map<String, Object> parseInfo = new HashMap<>();
        sqlVisit(sqlNode, parseInfo);
        return parseInfo;
    }

    public static SqlNode optimize(SqlValidatorScope scope, S2CalciteSchema schema, SqlNode sqlNode,
            EngineType engineType) {
        try {
            HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
            SemanticSqlDialect sqlDialect = SqlDialectFactory.getSqlDialect(engineType);
            hepProgramBuilder.addRuleInstance(
                    new FilterToGroupScanRule(FilterToGroupScanRule.DEFAULT, schema));

            hepProgramBuilder.addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE);
            hepProgramBuilder.addRuleInstance(CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS);
            hepProgramBuilder.addRuleInstance(CoreRules.AGGREGATE_PROJECT_STAR_TABLE);
            hepProgramBuilder.addRuleInstance(CoreRules.AGGREGATE_MERGE);
            hepProgramBuilder.addRuleInstance(CoreRules.AGGREGATE_REMOVE);
            hepProgramBuilder.addRuleInstance(CoreRules.AGGREGATE_JOIN_JOIN_REMOVE);
            hepProgramBuilder.addRuleInstance(CoreRules.AGGREGATE_JOIN_REMOVE);
            hepProgramBuilder.addRuleInstance(CoreRules.CALC_MERGE);
            hepProgramBuilder.addRuleInstance(CoreRules.CALC_REMOVE);
            hepProgramBuilder.addRuleInstance(CoreRules.CALC_REDUCE_EXPRESSIONS);
            hepProgramBuilder.addRuleInstance(CoreRules.CALC_SPLIT);
            hepProgramBuilder.addRuleInstance(CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS);
            hepProgramBuilder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
            hepProgramBuilder.addRuleInstance(CoreRules.FILTER_MERGE);
            hepProgramBuilder.addRuleInstance(CoreRules.FILTER_CALC_MERGE);
            hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_AGGREGATE_MERGE);
            hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_CALC_MERGE);
            hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_JOIN_JOIN_REMOVE);
            hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_JOIN_REMOVE);
            hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_MERGE);
            hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_MULTI_JOIN_MERGE);
            hepProgramBuilder.addRuleInstance(CoreRules.PROJECT_REMOVE);
            hepProgramBuilder.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH);
            hepProgramBuilder.addRuleInstance(CoreRules.JOIN_ASSOCIATE);



            RelOptPlanner relOptPlanner = new HepPlanner(hepProgramBuilder.build());
            RelToSqlConverter converter = new RelToSqlConverter(sqlDialect);
            SqlValidator sqlValidator = Configuration.getSqlValidator(
                    scope.getValidator().getCatalogReader().getRootSchema(), engineType);
            SqlToRelConverter sqlToRelConverter = Configuration.getSqlToRelConverter(scope,
                    sqlValidator, relOptPlanner, engineType);
            RelNode sqlRel =
                    sqlToRelConverter.convertQuery(sqlValidator.validate(sqlNode), false, true).rel;
            log.debug("RelNode optimize {}",
                    SemanticNode.getSql(converter.visitRoot(sqlRel).asStatement(), engineType));
            relOptPlanner.setRoot(sqlRel);
            RelNode relNode = relOptPlanner.findBestExp();
            return converter.visitRoot(relNode).asStatement();
        } catch (Exception e) {
            log.error("optimize error {}", e);
        }
        return null;
    }

    public static SqlBinaryOperator getBinaryOperator(String val) {
        if (val.equals("=")) {
            return SqlStdOperatorTable.EQUALS;
        }
        if (val.equals(">")) {
            return SqlStdOperatorTable.GREATER_THAN;
        }
        if (val.equals(">=")) {
            return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
        }
        if (val.equals("<")) {
            return SqlStdOperatorTable.LESS_THAN;
        }
        if (val.equals("<=")) {
            return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
        }
        if (val.equals("!=")) {
            return SqlStdOperatorTable.NOT_EQUALS;
        }
        return SqlStdOperatorTable.EQUALS;
    }

    public static SqlLiteral getJoinSqlLiteral(String joinType) {
        if (Objects.nonNull(joinType) && !joinType.isEmpty()) {
            if (joinType.toLowerCase().contains(JoinType.INNER.lowerName)) {
                return SqlLiteral.createSymbol(JoinType.INNER, SqlParserPos.ZERO);
            }
            if (joinType.toLowerCase().contains(JoinType.LEFT.lowerName)) {
                return SqlLiteral.createSymbol(JoinType.LEFT, SqlParserPos.ZERO);
            }
            if (joinType.toLowerCase().contains(JoinType.RIGHT.lowerName)) {
                return SqlLiteral.createSymbol(JoinType.RIGHT, SqlParserPos.ZERO);
            }
            if (joinType.toLowerCase().contains(JoinType.FULL.lowerName)) {
                return SqlLiteral.createSymbol(JoinType.FULL, SqlParserPos.ZERO);
            }
        }
        return SqlLiteral.createSymbol(JoinType.INNER, SqlParserPos.ZERO);
    }

    public static List<SqlNode> deduplicateNode(List<SqlNode> listNode) { // List<SqlNode>去重
        if (listNode == null) {
            return null;
        }
        List<SqlNode> uniqueElements = new ArrayList<>();
        for (SqlNode element : listNode) {
            if (!containsElement(uniqueElements, element)) {
                uniqueElements.add(element);
            }
        }
        return uniqueElements;
    }

    private static boolean containsElement(List<SqlNode> list, SqlNode element) { // 检查List<SqlNode>中是否含有某element
        for (SqlNode i : list) {
            if (i.equalsDeep(element, Litmus.IGNORE)) {
                return true;
            }
        }
        return false;
    }

}
