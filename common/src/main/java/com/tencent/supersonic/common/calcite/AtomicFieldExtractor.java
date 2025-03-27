package com.tencent.supersonic.common.calcite;

import com.tencent.supersonic.common.pojo.enums.EngineType;
import lombok.Getter;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.util.*;

public class AtomicFieldExtractor {

    /**
     * 从SQL中提取真实表和字段信息的结果类
     */
    @Getter
    public static class ExtractResult {
        /**
         * -- GETTER --
         *  获取表-字段映射
         *
         * @return 每个真实表及其包含的字段映射
         */
        private final Map<String, Set<String>> tableToFields;
        /**
         * -- GETTER --
         *  获取所有真实表
         *
         * @return 所有真实表的集合
         */
        private final Set<String> realTables;
        /**
         * -- GETTER --
         *  获取所有派生表和别名
         *
         * @return 所有派生表和别名的集合
         */
        private final Set<String> derivedTables;
        
        public ExtractResult(Map<String, Set<String>> tableToFields, Set<String> realTables, Set<String> derivedTables) {
            this.tableToFields = tableToFields;
            this.realTables = realTables;
            this.derivedTables = derivedTables;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            
            sb.append("=== 提取的真实表字段 ===\n");
            tableToFields.forEach((table, fields) -> {
                sb.append("表名: ").append(table).append("\n");
                for (String field : fields) {
                    sb.append("  - ").append(field).append("\n");
                }
            });
            
            sb.append("\n=== 所有真实表 ===\n");
            realTables.forEach(table -> sb.append(table).append("\n"));
            
            sb.append("\n=== 所有派生表和别名 ===\n");
            derivedTables.forEach(table -> sb.append(table).append("\n"));
            
            return sb.toString();
        }
    }
    
    /**
     * 从SQL中提取真实表、派生表和字段信息
     * 
     * @param sql 要分析的SQL语句
     * @return 包含提取结果的ExtractResult对象
     * @throws Exception 如果SQL解析失败
     */
    public static ExtractResult extractFromSql(String sql) throws Exception {
        sql = SqlIdentifierQuoteUtil.addQuotesToSql(sql, EngineType.MYSQL);
        // 使用Calcite 1.37.0的配置，指定MySQL方言
        SqlParser.Config config = SqlParser.Config.DEFAULT
                .withLex(Lex.MYSQL)
                .withConformance(SqlConformanceEnum.MYSQL_5);

        // 创建解析器并解析SQL
        SqlParser parser = SqlParser.create(sql, config);
        SqlNode sqlNode = parser.parseQuery();

        // 创建并使用字段提取访问者
        FieldExtractorVisitor visitor = new FieldExtractorVisitor();
        sqlNode.accept(visitor);
        
        // 创建并返回结果对象
        return new ExtractResult(
                new HashMap<>(visitor.getTableToFields()),  // 创建新的Map避免修改原始数据
                new HashSet<>(visitor.getRealTables()),     // 创建新的Set避免修改原始数据
                new HashSet<>(visitor.getDerivedTables())   // 创建新的Set避免修改原始数据
        );
    }

    public static void main(String[] args) {
        // 测试SQL
        String sql = "WITH RECURSIVE 国家频道统计 AS (" +
                "SELECT Language, Country, COUNT(ChannelID) AS 频道数量 " +
                "FROM TVChannelDataset GROUP BY Country " +
                "UNION ALL " +
                "SELECT 'Global', COUNT(*) FROM TVChannelDataset" +
                ") " +
                "SELECT ChannelID, " +
                "CASE WHEN rating > 4.5 THEN premium_price * 1.1 ELSE regular_price END AS price, " +
                "JSON_EXTRACT(metadata, '$.category') as category, " +
                "custom_function(viewers, IF(subscriber_count > 1000, subscriber_count, 0)) AS engagement, " +
                "(SELECT MAX(launch_date) FROM ChannelHistory WHERE ChannelHistory.cid = TVChannelDataset.ChannelID) AS last_updated, " +
                "ROW_NUMBER() OVER (PARTITION BY category ORDER BY viewers DESC) AS category_rank " +
                "FROM TVChannelDataset " +
                "LEFT JOIN ChannelPricing ON TVChannelDataset.ChannelID = ChannelPricing.channel_id " +
                "WHERE Country IN (SELECT Country FROM 国家频道统计 WHERE 频道数量 > 2) " +
                "AND (subscriber_count > 1000 OR EXISTS (" +
                "   SELECT 1 FROM PremiumChannels " +
                "   WHERE PremiumChannels.id = TVChannelDataset.ChannelID " +
                "   AND is_featured = TRUE" +
                ")) " +
                "GROUP BY ChannelID " +
                "HAVING AVG(viewers) > 5000 " +
                "ORDER BY category, viewers DESC " +
                "LIMIT 100";

        try {
            // 使用新方法提取结果
            ExtractResult result = extractFromSql(sql);
            
            // 输出结果
            System.out.println(result);
            
        } catch (Exception e) {
            System.err.println("SQL解析错误: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 字段提取访问者 - 针对Calcite 1.37.0优化
     */
    static class FieldExtractorVisitor extends SqlBasicVisitor<Void> {
        // 真实表集合
        private final Set<String> realTables = new HashSet<>();
        
        // 派生表和别名集合
        private final Set<String> derivedTables = new HashSet<>();
        
        // 表与字段的映射关系
        private final Map<String, Set<String>> tableToFields = new HashMap<>();
        
        // 当前上下文信息
        private final Stack<SqlNode> contextStack = new Stack<>();
        
        // 记录字段的上下文来源
        private final Map<String, String> fieldToContext = new HashMap<>();
        
        // 当前节点的父节点类型
        private SqlKind currentParentKind = null;

        public Set<String> getRealTables() {
            return realTables;
        }
        
        public Set<String> getDerivedTables() {
            return derivedTables;
        }

        public Map<String, Set<String>> getTableToFields() {
            return tableToFields;
        }

        @Override
        public Void visit(SqlCall call) {
            // 添加当前节点到上下文栈
            contextStack.push(call);
            
            // 记录当前父节点类型
            SqlKind prevParentKind = currentParentKind;
            currentParentKind = call.getKind();
            
            // 首先收集表名和别名
            collectTableAndAliasNames(call);

            // 根据SQL节点类型分别处理
            if (call instanceof SqlSelect) {
                visitSelect((SqlSelect) call);
            } else if (call instanceof SqlBasicCall) {
                visitBasicCall((SqlBasicCall) call);
            } else if (call instanceof SqlWith) {
                visitWith((SqlWith) call);
            } else if (call instanceof SqlJoin) {
                visitJoin((SqlJoin) call);
            } else if (call instanceof SqlOrderBy) {
                visitOrderBy((SqlOrderBy) call);
            } else {
                // 处理其他类型的调用
                for (SqlNode operand : call.getOperandList()) {
                    if (operand != null) {
                        operand.accept(this);
                    }
                }
            }
            
            // 恢复父节点类型
            currentParentKind = prevParentKind;
            
            // 移除当前节点
            contextStack.pop();

            return null;
        }

        @Override
        public Void visit(SqlIdentifier id) {
            if (!isTableOrAlias(id)) {
                // 这是一个字段标识符
                if (id.names.size() > 1) {
                    // 对于限定标识符如table.column，获取表名和字段名
                    String tableName = id.names.get(0);
                    String fieldName = id.names.get(id.names.size() - 1);
                    
                    // 检查是否是真实表的字段
                    if (realTables.contains(tableName)) {
                        addTableField(tableName, fieldName);
                    }
                } else {
                    // 对于非限定标识符，尝试确定它属于哪个表
                    resolveSingleIdentifierTable(id);
                }
            }
            return null;
        }

        /**
         * 尝试解析单一标识符的表
         */
        private void resolveSingleIdentifierTable(SqlIdentifier id) {
            String fieldName = id.getSimple();
            
            // 检查上下文，尝试确定字段所属的表
            if (!contextStack.isEmpty()) {
                SqlNode currentContext = contextStack.peek();
                
                if (currentContext instanceof SqlSelect) {
                    SqlSelect select = (SqlSelect) currentContext;
                    
                    // 如果FROM子句是单一表，可以直接关联
                    if (select.getFrom() instanceof SqlIdentifier) {
                        String tableName = ((SqlIdentifier) select.getFrom()).toString();
                        if (realTables.contains(tableName)) {
                            addTableField(tableName, fieldName);
                        }
                    } 
                    // 如果FROM子句是JOIN，需要分析JOIN的表
                    else if (select.getFrom() instanceof SqlJoin) {
                        resolveFieldInJoin((SqlJoin) select.getFrom(), fieldName);
                    }
                    // 处理其他情况...
                }
            }
            
            // 如果无法确定表，将字段添加到"未知表"类别
            if (!fieldToContext.containsKey(fieldName)) {
                fieldToContext.put(fieldName, "未知表");
            }
        }
        
        /**
         * 尝试在JOIN中解析字段所属的表
         */
        private void resolveFieldInJoin(SqlJoin join, String fieldName) {
            // 这里可以实现更复杂的逻辑来确定字段属于JOIN中的哪个表
            // 简化起见，我们先把字段添加到所有可能的真实表中
            Set<String> possibleTables = new HashSet<>();
            collectTablesFromJoin(join, possibleTables);
            
            for (String tableName : possibleTables) {
                if (realTables.contains(tableName)) {
                    addTableField(tableName, fieldName);
                }
            }
        }
        
        /**
         * 从JOIN中收集所有表
         */
        private void collectTablesFromJoin(SqlJoin join, Set<String> tables) {
            collectTablesFromNode(join.getLeft(), tables);
            collectTablesFromNode(join.getRight(), tables);
        }
        
        /**
         * 从节点中收集表
         */
        private void collectTablesFromNode(SqlNode node, Set<String> tables) {
            if (node instanceof SqlIdentifier) {
                String tableName = ((SqlIdentifier) node).toString();
                // 只添加真实表，不添加派生表
                if (!derivedTables.contains(tableName)) {
                    tables.add(tableName);
                }
            } else if (node instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) node;
                if (call.getKind() == SqlKind.AS && call.operandCount() >= 2) {
                    collectTablesFromNode(call.operand(0), tables);
                }
            } else if (node instanceof SqlJoin) {
                collectTablesFromJoin((SqlJoin) node, tables);
            }
        }

        /**
         * 添加表-字段映射
         */
        private void addTableField(String tableName, String fieldName) {
            // 只为真实表添加字段
            if (!derivedTables.contains(tableName)) {
                tableToFields.computeIfAbsent(tableName, k -> new HashSet<>()).add(fieldName);
                fieldToContext.put(fieldName, tableName);
            }
        }

        /**
         * 访问SELECT节点
         */
        private void visitSelect(SqlSelect select) {
            // 处理FROM子句
            if (select.getFrom() != null) {
                select.getFrom().accept(this);
            }
            
            // 处理SELECT列表
            if (select.getSelectList() != null) {
                for (SqlNode node : select.getSelectList()) {
                    node.accept(this);
                }
            }

            // 处理WHERE子句
            if (select.getWhere() != null) {
                select.getWhere().accept(this);
            }

            // 处理GROUP BY子句
            if (select.getGroup() != null) {
                for (SqlNode node : select.getGroup()) {
                    node.accept(this);
                }
            }

            // 处理HAVING子句
            if (select.getHaving() != null) {
                select.getHaving().accept(this);
            }

            // 处理窗口定义
            if (select.getWindowList() != null && select.getWindowList().size() > 0) {
                for (SqlNode node : select.getWindowList()) {
                    node.accept(this);
                }
            }

            // 处理ORDER BY子句
            if (select.getOrderList() != null && select.getOrderList().size() > 0) {
                for (SqlNode node : select.getOrderList()) {
                    node.accept(this);
                }
            }

            // 处理OFFSET/FETCH (LIMIT)
            if (select.getOffset() != null) {
                select.getOffset().accept(this);
            }
            if (select.getFetch() != null) {
                select.getFetch().accept(this);
            }
        }

        /**
         * 访问基本调用节点
         */
        private void visitBasicCall(SqlBasicCall call) {
            switch (call.getKind()) {
                case AS:
                    // 处理AS表达式
                    if (call.operandCount() >= 2) {
                        // 检查AS是否用于表别名
                        boolean isTableAlias = isInFromClause();
                        
                        // 处理左侧表达式
                        call.operand(0).accept(this);
                        
                        // 处理右侧别名
                        if (call.operand(1) instanceof SqlIdentifier) {
                            String alias = ((SqlIdentifier) call.operand(1)).getSimple();
                            
                            // 只有表别名才添加到derivedTables
                            if (isTableAlias) {
                                derivedTables.add(alias);
                            }
                        }
                    }
                    break;

                case CASE:
                    // 处理CASE表达式
                    visitCase(call);
                    break;
                case UNION:
                case INTERSECT:
                case EXCEPT:
                    // 集合操作
                    for (SqlNode operand : call.getOperandList()) {
                        operand.accept(this);
                    }
                    break;

                case OVER:
                    // 窗口函数
                    visitOver(call);
                    break;

                case OTHER_FUNCTION:
                    // 处理各种函数
                    visitFunction(call);
                    break;

                default:
                    // 处理其他所有基本调用
                    for (SqlNode operand : call.getOperandList()) {
                        if (operand != null) {
                            operand.accept(this);
                        }
                    }
                    break;
            }
        }

        /**
         * 判断当前是否在FROM子句中处理
         */
        private boolean isInFromClause() {
            if (contextStack.size() < 2) {
                return false;
            }
            
            // 查找上下文栈中的SELECT节点
            for (int i = contextStack.size() - 2; i >= 0; i--) {
                SqlNode node = contextStack.get(i);
                if (node instanceof SqlSelect) {
                    SqlSelect select = (SqlSelect) node;
                    SqlNode from = select.getFrom();
                    
                    // 检查当前节点是否是FROM子句的一部分
                    if (isNodePartOf(contextStack.peek(), from)) {
                        return true;
                    }
                }
            }
            
            return false;
        }
        
        /**
         * 判断节点是否是另一个节点的一部分
         */
        private boolean isNodePartOf(SqlNode target, SqlNode container) {
            if (container == target) {
                return true;
            }
            
            if (container instanceof SqlCall) {
                SqlCall call = (SqlCall) container;
                for (SqlNode operand : call.getOperandList()) {
                    if (operand != null && isNodePartOf(target, operand)) {
                        return true;
                    }
                }
            }
            
            return false;
        }

        /**
         * 访问WITH节点
         */
        private void visitWith(SqlWith with) {
            // 处理WITH项定义
            for (SqlNode withItem : with.withList) {
                if (withItem instanceof SqlWithItem) {
                    SqlWithItem item = (SqlWithItem) withItem;
                    // 记录WITH子句名称作为派生表
                    String withTableName = item.name.getSimple();
                    derivedTables.add(withTableName);
                    
                    // 移除可能被错误添加到真实表的WITH表
                    realTables.remove(withTableName);
                    
                    // 递归处理WITH子句的查询
                    item.query.accept(this);
                }
            }

            // 处理主体查询
            with.body.accept(this);
        }

        /**
         * 访问JOIN节点
         */
        private void visitJoin(SqlJoin join) {
            // 处理左表
            join.getLeft().accept(this);

            // 处理右表
            join.getRight().accept(this);

            // 处理JOIN条件
            if (join.getCondition() != null) {
                join.getCondition().accept(this);
            }
        }

        /**
         * 访问ORDER BY节点
         */
        private void visitOrderBy(SqlOrderBy orderBy) {
            // 处理查询部分
            orderBy.query.accept(this);

            // 处理ORDER BY表达式
            for (SqlNode orderItem : orderBy.orderList) {
                orderItem.accept(this);
            }

            // 处理OFFSET/FETCH
            if (orderBy.offset != null) {
                orderBy.offset.accept(this);
            }
            if (orderBy.fetch != null) {
                orderBy.fetch.accept(this);
            }
        }

        /**
         * 访问CASE表达式
         */
        private void visitCase(SqlBasicCall caseCall) {
            // Calcite 1.37.0中CASE表达式是SqlBasicCall
            for (SqlNode operand : caseCall.getOperandList()) {
                if (operand != null) {
                    operand.accept(this);
                }
            }
        }

        /**
         * 访问OVER窗口函数
         */
        private void visitOver(SqlBasicCall overCall) {
            // 处理函数部分
            overCall.operand(0).accept(this);

            // 处理窗口规范
            SqlNode windowSpec = overCall.operand(1);
            if (windowSpec instanceof SqlWindow) {
                SqlWindow window = (SqlWindow) windowSpec;

                // 处理PARTITION BY子句
                if (window.getPartitionList() != null) {
                    for (SqlNode partitionItem : window.getPartitionList()) {
                        partitionItem.accept(this);
                    }
                }

                // 处理ORDER BY子句
                if (window.getOrderList() != null) {
                    for (SqlNode orderItem : window.getOrderList()) {
                        orderItem.accept(this);
                    }
                }
            }
        }

        /**
         * 访问函数调用
         */
        private void visitFunction(SqlBasicCall functionCall) {
            String funcName = functionCall.getOperator().getName();

            // MySQL特有函数的特殊处理
            if (isMySqlSpecialFunction(funcName)) {
                // MySQL特殊函数，需要检查所有参数
                for (SqlNode operand : functionCall.getOperandList()) {
                    if (operand != null) {
                        operand.accept(this);
                    }
                }
            } else {
                // 其他函数的通用处理
                for (SqlNode operand : functionCall.getOperandList()) {
                    if (operand != null) {
                        operand.accept(this);
                    }
                }
            }
        }

        /**
         * 检查是否是MySQL特有的需要特殊处理的函数
         */
        private boolean isMySqlSpecialFunction(String funcName) {
            // MySQL特有函数列表
            return funcName.equalsIgnoreCase("IF") ||
                    funcName.equalsIgnoreCase("IFNULL") ||
                    funcName.equalsIgnoreCase("NULLIF") ||
                    funcName.equalsIgnoreCase("COALESCE") ||
                    funcName.equalsIgnoreCase("JSON_EXTRACT") ||
                    funcName.equalsIgnoreCase("JSON_CONTAINS") ||
                    funcName.equalsIgnoreCase("JSON_CONTAINS_PATH") ||
                    funcName.equalsIgnoreCase("JSON_ARRAY") ||
                    funcName.equalsIgnoreCase("JSON_OBJECT") ||
                    funcName.equalsIgnoreCase("DATE_FORMAT") ||
                    funcName.equalsIgnoreCase("STR_TO_DATE") ||
                    funcName.equalsIgnoreCase("DATEDIFF") ||
                    funcName.equalsIgnoreCase("SUBSTRING_INDEX") ||
                    funcName.equalsIgnoreCase("GROUP_CONCAT");
        }

        /**
         * 收集表名和别名
         */
        private void collectTableAndAliasNames(SqlNode node) {
            if (node instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) node;
                collectFromItem(select.getFrom());
            } else if (node instanceof SqlJoin) {
                SqlJoin join = (SqlJoin) node;
                collectFromItem(join.getLeft());
                collectFromItem(join.getRight());
            } else if (node instanceof SqlWithItem) {
                SqlWithItem withItem = (SqlWithItem) node;
                // 将WITH子句项添加到派生表
                String withTableName = withItem.name.getSimple();
                derivedTables.add(withTableName);
                // 确保WITH表不在realTables中
                realTables.remove(withTableName);
            } else if (node instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) node;
                if (call.getKind() == SqlKind.AS && call.operandCount() >= 2) {
                    // 检查是否在FROM子句中
                    boolean isTableAlias = isInFromClause();
                    
                    // AS表达式，右侧是别名
                    if (call.operand(1) instanceof SqlIdentifier) {
                        String alias = ((SqlIdentifier) call.operand(1)).getSimple();
                        // 只有在FROM子句中的AS才是表别名
                        if (isTableAlias) {
                            derivedTables.add(alias);
                        }
                    }

                    // 左侧可能是表名或子查询
                    if (call.operand(0) instanceof SqlIdentifier && isTableAlias) {
                        // 如果左侧是标识符，且在FROM子句中，可能是真实表
                        String tableName = ((SqlIdentifier) call.operand(0)).toString();
                        // 确保不是WITH表
                        if (!derivedTables.contains(tableName)) {
                            realTables.add(tableName);
                        }
                    } else {
                        // 如果左侧是子查询，递归处理
                        collectFromItem(call.operand(0));
                    }
                }
            }
        }

        /**
         * 收集FROM项中的表名和别名
         */
        private void collectFromItem(SqlNode fromItem) {
            if (fromItem == null) {
                return;
            }

            if (fromItem instanceof SqlIdentifier) {
                // 简单表名
                String tableName = ((SqlIdentifier) fromItem).toString();
                // 确保不是WITH表
                if (!derivedTables.contains(tableName)) {
                    realTables.add(tableName);
                }
            } else if (fromItem instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) fromItem;
                if (call.getKind() == SqlKind.AS && call.operandCount() >= 2) {
                    // table AS alias 形式
                    if (call.operand(0) instanceof SqlIdentifier) {
                        // 真实表名
                        String tableName = ((SqlIdentifier) call.operand(0)).toString();
                        // 确保不是WITH表
                        if (!derivedTables.contains(tableName)) {
                            realTables.add(tableName);
                        }
                    }
                    if (call.operand(1) instanceof SqlIdentifier) {
                        // 别名添加到派生表
                        derivedTables.add(((SqlIdentifier) call.operand(1)).getSimple());
                    }

                    // 递归检查，以防是子查询AS别名
                    collectTableAndAliasNames(call.operand(0));
                }
            } else if (fromItem instanceof SqlJoin) {
                // JOIN语句
                SqlJoin join = (SqlJoin) fromItem;
                collectFromItem(join.getLeft());
                collectFromItem(join.getRight());
            } else {
                // 子查询或其他复杂FROM项
                collectTableAndAliasNames(fromItem);
            }
        }

        /**
         * 判断标识符是否是表名或别名
         */
        private boolean isTableOrAlias(SqlIdentifier id) {
            // 如果是限定标识符(如table.column)，检查第一部分是否是表名或别名
            if (id.names.size() > 1) {
                return false; // 多部分标识符不是表或别名本身，而是表的字段
            }

            // 单一标识符，检查是否是表名或别名
            String name = id.toString();
            return realTables.contains(name) || derivedTables.contains(name);
        }
    }
}  