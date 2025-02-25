package com.tencent.supersonic.headless.core.translator.parser.calcite.render;

import com.tencent.supersonic.common.pojo.enums.EngineType;
import com.tencent.supersonic.headless.core.pojo.JoinRelation;
import com.tencent.supersonic.headless.core.pojo.OntologyQuery;
import com.tencent.supersonic.headless.core.translator.parser.calcite.S2CalciteSchema;
import com.tencent.supersonic.headless.core.translator.parser.calcite.TableView;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.AggFunctionNode;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.DataModelNode;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.FilterNode;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.IdentifyNode;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.MetricNode;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.SemanticNode;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Constants;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.DataModel;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Dimension;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Identify;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Materialization;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Metric;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 处理多数据源join的渲染器
 * 主要职责是构建和处理多表join的SQL语句
 */
@Slf4j
public class JoinRender extends Renderer {

    @Override
    public void render(OntologyQuery metricCommand, List<DataModel> dataModels,
            SqlValidatorScope scope, S2CalciteSchema schema, boolean nonAgg) throws Exception {

        // 1. 处理where条件中的字段
        String queryWhere = metricCommand.getWhere();
        EngineType engineType = schema.getOntology().getDatabaseType();
        Set<String> whereFields = new HashSet<>();
        List<String> fieldWhere = new ArrayList<>();
        if (queryWhere != null && !queryWhere.isEmpty()) {
            SqlNode sqlNode = SemanticNode.parse(queryWhere, scope, engineType);
            FilterNode.getFilterField(sqlNode, whereFields);
            fieldWhere = whereFields.stream().collect(Collectors.toList());
        }

        // 2. 获取查询涉及的所有维度和度量
        Set<String> queryAllDimension = new HashSet<>();
        Set<String> measures = new HashSet<>();
        DataModelNode.getQueryDimensionMeasure(schema.getOntology(), metricCommand,
                queryAllDimension, measures);

        // 3. 初始化视图
        TableView innerView = new TableView();
        TableView filterView = new TableView();
        Map<String, SqlNode> innerSelect = new HashMap<>();
        Set<String> filterDimension = new HashSet<>();

        // 4. 构建join关系图
        JoinGraph joinGraph = buildJoinGraph(dataModels, schema);

        // 5. 构建join树
        SqlNode joinTree = buildJoinTree(
            joinGraph,
            dataModels,
            fieldWhere,
            measures,  // queryMetrics
            queryAllDimension,  // queryDimension
            queryWhere,
            scope,
            schema
        );

        // 6. 收集所有需要的字段
        Set<String> finalFields = new HashSet<>();  // 最终需要的字段
        Map<String, String> fieldToSourceMap = new HashMap<>();  // 字段到源表的映射

        // 6.1 处理维度字段
        for (String dimension : metricCommand.getDimensions()) {
            for (DataModel model : dataModels) {
                if (isDimensionBelongToModel(dimension, model)) {
                    String tableAlias = Constants.JOIN_TABLE_PREFIX + model.getName();
                    fieldToSourceMap.put(dimension, tableAlias);
                    finalFields.add(dimension);
                    break;
                }
            }
        }

        // 6.2 处理度量字段
        for (String metric : metricCommand.getMetrics()) {
            for (DataModel model : dataModels) {
                if (isMetricBelongToModel(metric, model)) {
                    String tableAlias = Constants.JOIN_TABLE_PREFIX + model.getName();
                    fieldToSourceMap.put(metric, tableAlias);
                    finalFields.add(metric);
                    break;
                }
            }
        }

        // 6.3 处理where条件中的字段
        for (String field : whereFields) {
            if (!fieldToSourceMap.containsKey(field)) {
                for (DataModel model : dataModels) {
                    if (isFieldBelongToModel(field, model)) {
                        String tableAlias = Constants.JOIN_TABLE_PREFIX + model.getName();
                        fieldToSourceMap.put(field, tableAlias);
                        finalFields.add(field);
                        break;
                    }
                }
            }
        }

        // 7. 构建最终的查询视图
        // 7.1 构建内部视图 - 只选择需要的字段，并正确处理别名
        for (String field : finalFields) {  // finalFields 本身就是 Set，确保了字段不重复
            String sourceTable = fieldToSourceMap.get(field);
            if (sourceTable != null) {
                String qualifiedField = sourceTable + "." + field;
                SqlNode fieldNode = SemanticNode.parse(qualifiedField, scope, engineType);
                innerView.getMeasure().add(SemanticNode.buildAs(field, fieldNode));
            }
        }
        innerView.setTable(joinTree);

        // 7.2 构建过滤视图
        filterView.setTable(SemanticNode.buildAs(Constants.JOIN_TABLE_OUT_PREFIX, innerView.build()));

        // 7.3 根据查询类型（聚合/非聚合）添加字段
        for (String field : finalFields) {  // 使用已经去重的字段集合
            SqlNode fieldNode = SemanticNode.parse(field, scope, engineType);
            if (nonAgg) {
                filterView.getMeasure().add(fieldNode);
            } else {
                if (metricCommand.getDimensions().contains(field)) {
                    filterView.getDimension().add(fieldNode);
                } else if (metricCommand.getMetrics().contains(field)) {
                    filterView.getMeasure().add(fieldNode);
                }
            }
        }

        super.tableView = filterView;
    }

    /**
     * 处理度量字段
     * @param innerSelect 内部select字段映射
     * @param filterView 过滤视图
     * @param queryMetrics 查询的度量集合
     * @param reqMetrics 请求的度量列表
     * @param dataModel 数据模型
     * @param sourceMeasure 源表度量集合
     * @param scope SQL验证范围
     * @param schema 计算模式
     * @param nonAgg 是否非聚合
     */
    private void doMetric(Map<String, SqlNode> innerSelect, TableView filterView,
            Set<String> queryMetrics, List<String> reqMetrics, DataModel dataModel,
            Set<String> sourceMeasure, SqlValidatorScope scope, S2CalciteSchema schema,
            boolean nonAgg) throws Exception {
        String alias = Constants.JOIN_TABLE_PREFIX + dataModel.getName();
        EngineType engineType = schema.getOntology().getDatabaseType();
        for (String m : reqMetrics) {
            // 检查度量是否匹配当前数据模型
            if (getMatchMetric(schema, sourceMeasure, m, queryMetrics)) {
                // 构建度量节点
                MetricNode metricNode = buildMetricNode(m, dataModel, scope, schema, nonAgg, alias);

                // 处理非聚合节点
                if (!metricNode.getNonAggNode().isEmpty()) {
                    for (String measure : metricNode.getNonAggNode().keySet()) {
                        innerSelect.put(measure, SemanticNode.buildAs(measure,
                                SemanticNode.parse(alias + "." + measure, scope, engineType)));
                    }
                }
                // 处理聚合函数
                if (metricNode.getAggFunction() != null && !metricNode.getAggFunction().isEmpty()) {
                    for (Map.Entry<String, String> entry : metricNode.getAggFunction().entrySet()) {
                        if (metricNode.getNonAggNode().containsKey(entry.getKey())) {
                            if (nonAgg) {
                                filterView.getMeasure().add(SemanticNode.buildAs(entry.getKey(),
                                        SemanticNode.parse(entry.getKey(), scope, engineType)));
                            } else {
                                filterView.getMeasure()
                                        .add(SemanticNode.buildAs(entry.getKey(),
                                                AggFunctionNode.build(entry.getValue(),
                                                        entry.getKey(), scope, engineType)));
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 处理维度字段
     * @param innerSelect 内部select字段映射
     * @param filterDimension 过滤维度集合
     * @param queryDimension 查询维度集合
     * @param reqDimensions 请求的维度列表
     * @param dataModel 数据模型
     * @param dimension 维度集合
     * @param scope SQL验证范围
     * @param schema 计算模式
     */
    private void doDimension(Map<String, SqlNode> innerSelect, Set<String> filterDimension,
            Set<String> queryDimension, List<String> reqDimensions, DataModel dataModel,
            Set<String> dimension, SqlValidatorScope scope, S2CalciteSchema schema)
            throws Exception {
        String alias = Constants.JOIN_TABLE_PREFIX + dataModel.getName();
        EngineType engineType = schema.getOntology().getDatabaseType();
        for (String d : reqDimensions) {
            // 检查维度是否匹配当前数据模型
            if (getMatchDimension(schema, dimension, dataModel, d, queryDimension)) {
                // 处理标识符维度
                if (d.contains(Constants.DIMENSION_IDENTIFY)) {
                    String[] identifyDimension = d.split(Constants.DIMENSION_IDENTIFY);
                    innerSelect.put(d, SemanticNode.buildAs(d, SemanticNode
                            .parse(alias + "." + identifyDimension[1], scope, engineType)));
                } else {
                    innerSelect.put(d, SemanticNode.buildAs(d,
                            SemanticNode.parse(alias + "." + d, scope, engineType)));
                }
                filterDimension.add(d);
            }
        }
    }

    /**
     * 获取查询维度
     * 从过滤维度中筛选出在查询维度或where条件字段中的维度
     */
    private Set<String> getQueryDimension(Set<String> filterDimension,
            Set<String> queryAllDimension, Set<String> whereFields) {
        return filterDimension.stream()
                .filter(d -> queryAllDimension.contains(d) || whereFields.contains(d))
                .collect(Collectors.toSet());
    }

    /**
     * 检查度量是否匹配
     * 判断度量是否存在于源表或是否为复合度量
     */
    private boolean getMatchMetric(S2CalciteSchema schema, Set<String> sourceMeasure, String m,
            Set<String> queryMetrics) {
        Optional<Metric> metric = schema.getMetrics().stream()
                .filter(mm -> mm.getName().equalsIgnoreCase(m)).findFirst();
        boolean isAdd = false;
        // 检查复合度量
        if (metric.isPresent()) {
            Set<String> metricMeasures = metric.get().getMetricTypeParams().getMeasures().stream()
                    .map(me -> me.getName()).collect(Collectors.toSet());
            if (sourceMeasure.containsAll(metricMeasures)) {
                isAdd = true;
            }
        }
        // 检查简单度量
        if (sourceMeasure.contains(m)) {
            isAdd = true;
        }
        if (isAdd && !queryMetrics.contains(m)) {
            queryMetrics.add(m);
        }
        return isAdd;
    }

    /**
     * 检查维度是否匹配
     * 判断维度是否存在于源表维度、标识符或schema定义的维度中
     */
    private boolean getMatchDimension(S2CalciteSchema schema, Set<String> sourceDimension,
            DataModel dataModel, String d, Set<String> queryDimension) {
        String oriDimension = d;
        boolean isAdd = false;
        // 处理标识符维度
        if (d.contains(Constants.DIMENSION_IDENTIFY)) {
            oriDimension = d.split(Constants.DIMENSION_IDENTIFY)[1];
        }
        // 检查源表维度
        if (sourceDimension.contains(oriDimension)) {
            isAdd = true;
        }
        // 检查标识符
        for (Identify identify : dataModel.getIdentifiers()) {
            if (identify.getName().equalsIgnoreCase(oriDimension)) {
                isAdd = true;
                break;
            }
        }
        // 检查schema定义的维度
        if (schema.getDimensions().containsKey(dataModel.getName())) {
            for (Dimension dim : schema.getDimensions().get(dataModel.getName())) {
                if (dim.getName().equalsIgnoreCase(oriDimension)) {
                    isAdd = true;
                }
            }
        }
        if (isAdd && !queryDimension.contains(oriDimension)) {
            queryDimension.add(oriDimension);
        }
        return isAdd;
    }

    /**
     * 获取表对象
     */
    private SqlNode getTable(TableView tableView, SqlValidatorScope scope) throws Exception {
        return SemanticNode.getTable(tableView.getTable());
    }

    /**
     * 构建Join节点
     * @param left 左表节点
     * @param leftTable 左表视图
     * @param tableView 右表视图
     * @param before 已处理的表别名映射
     * @param dataModel 数据模型
     * @param schema 计算模式
     * @param scope SQL验证范围
     */
    private SqlNode buildJoin(SqlNode left, TableView leftTable, TableView tableView,
            Map<String, String> before, DataModel dataModel, S2CalciteSchema schema,
            SqlValidatorScope scope) throws Exception {
        EngineType engineType = schema.getOntology().getDatabaseType();

        // 获取基本join条件
        SqlNode condition = getCondition(leftTable, tableView, dataModel, schema, scope, engineType);

        // 获取join类型
        SqlLiteral sqlLiteral = SemanticNode.getJoinSqlLiteral("");

        // 检查预定义join关系
        JoinRelation matchJoinRelation = getMatchJoinRelation(before, tableView, schema);
        SqlNode joinRelationCondition = null;
        if (!CollectionUtils.isEmpty(matchJoinRelation.getJoinCondition())) {
            sqlLiteral = SemanticNode.getJoinSqlLiteral(matchJoinRelation.getJoinType());
            joinRelationCondition = getCondition(matchJoinRelation, scope, engineType);
            condition = joinRelationCondition;
        }

        // 处理zipper join
        if (Materialization.TimePartType.ZIPPER.equals(leftTable.getDataModel().getTimePartType())
                || Materialization.TimePartType.ZIPPER
                        .equals(tableView.getDataModel().getTimePartType())) {
            SqlNode zipperCondition = getZipperCondition(leftTable, tableView, dataModel, schema, scope);
            if (Objects.nonNull(joinRelationCondition)) {
                condition = new SqlBasicCall(SqlStdOperatorTable.AND,
                        new ArrayList<>(Arrays.asList(zipperCondition, joinRelationCondition)),
                        SqlParserPos.ZERO, null);
            } else {
                condition = zipperCondition;
            }
        }

        // 构建join节点
        return new SqlJoin(SqlParserPos.ZERO, left,
                SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                sqlLiteral,
                SemanticNode.buildAs(tableView.getAlias(), getTable(tableView, scope)),
                SqlLiteral.createSymbol(JoinConditionType.ON, SqlParserPos.ZERO),
                condition);
    }

    /**
     * 获取匹配的Join关系
     * 从schema中查找匹配的预定义join关系
     */
    private JoinRelation getMatchJoinRelation(Map<String, String> before, TableView tableView,
            S2CalciteSchema schema) {
        JoinRelation matchJoinRelation = JoinRelation.builder().build();
        if (!CollectionUtils.isEmpty(schema.getJoinRelations())) {
            for (JoinRelation joinRelation : schema.getJoinRelations()) {
                // 检查正向join关系
                if (joinRelation.getRight().equalsIgnoreCase(tableView.getDataModel().getName())
                        && before.containsKey(joinRelation.getLeft())) {
                    matchJoinRelation.setJoinCondition(joinRelation.getJoinCondition().stream()
                            .map(r -> Triple.of(
                                    before.get(joinRelation.getLeft()) + "." + r.getLeft(),
                                    r.getMiddle(), tableView.getAlias() + "." + r.getRight()))
                            .collect(Collectors.toList()));
                    matchJoinRelation.setJoinType(joinRelation.getJoinType());
                }
                // 检查反向join关系
                else if (joinRelation.getLeft()
                        .equalsIgnoreCase(tableView.getDataModel().getName())
                        && before.containsKey(joinRelation.getRight())) {
                    matchJoinRelation.setJoinCondition(joinRelation.getJoinCondition().stream()
                            .map(r -> Triple.of(
                                    before.get(joinRelation.getRight()) + "." + r.getRight(),
                                    r.getMiddle(), tableView.getAlias() + "." + r.getLeft()))
                            .collect(Collectors.toList()));
                    matchJoinRelation.setJoinType(joinRelation.getJoinType());
                }
            }
        }
        return matchJoinRelation;
    }

    /**
     * 添加zipper字段
     * 为zipper类型的表添加时间范围字段
     */
    private void addZipperField(DataModel dataModel, List<String> fields) {
        if (Materialization.TimePartType.ZIPPER.equals(dataModel.getTimePartType())) {
            dataModel.getDimensions().stream()
                    .filter(d -> Constants.DIMENSION_TYPE_TIME.equalsIgnoreCase(d.getType()))
                    .forEach(t -> {
                        if (t.getName().startsWith(Constants.MATERIALIZATION_ZIPPER_END)
                                && !fields.contains(t.getName())) {
                            fields.add(t.getName());
                        }
                        if (t.getName().startsWith(Constants.MATERIALIZATION_ZIPPER_START)
                                && !fields.contains(t.getName())) {
                            fields.add(t.getName());
                        }
                    });
        }
    }

    /**
     * 获取zipper join条件
     * 构建时间范围的join条件
     */
    private SqlNode getZipperCondition(TableView left, TableView right, DataModel dataModel,
            S2CalciteSchema schema, SqlValidatorScope scope) throws Exception {
        // 检查是否同时为zipper类型
        if (Materialization.TimePartType.ZIPPER.equals(left.getDataModel().getTimePartType())
                && Materialization.TimePartType.ZIPPER
                        .equals(right.getDataModel().getTimePartType())) {
            throw new Exception("not support two zipper table");
        }

        SqlNode condition = null;
        // 获取时间维度
        Optional<Dimension> leftTime = left.getDataModel().getDimensions().stream()
                .filter(d -> Constants.DIMENSION_TYPE_TIME.equalsIgnoreCase(d.getType()))
                .findFirst();
        Optional<Dimension> rightTime = right.getDataModel().getDimensions().stream()
                .filter(d -> Constants.DIMENSION_TYPE_TIME.equalsIgnoreCase(d.getType()))
                .findFirst();

        if (leftTime.isPresent() && rightTime.isPresent()) {
            String startTime = "";
            String endTime = "";
            String dateTime = "";

            // 获取zipper表的时间范围字段
            Optional<Dimension> startTimeOp = (Materialization.TimePartType.ZIPPER
                    .equals(left.getDataModel().getTimePartType()) ? left : right).getDataModel()
                            .getDimensions().stream()
                            .filter(d -> Constants.DIMENSION_TYPE_TIME
                                    .equalsIgnoreCase(d.getType()))
                            .filter(d -> d.getName()
                                    .startsWith(Constants.MATERIALIZATION_ZIPPER_START))
                            .findFirst();
            Optional<Dimension> endTimeOp = (Materialization.TimePartType.ZIPPER
                    .equals(left.getDataModel().getTimePartType()) ? left : right).getDataModel()
                            .getDimensions().stream()
                            .filter(d -> Constants.DIMENSION_TYPE_TIME
                                    .equalsIgnoreCase(d.getType()))
                            .filter(d -> d.getName()
                                    .startsWith(Constants.MATERIALIZATION_ZIPPER_END))
                            .findFirst();

            if (startTimeOp.isPresent() && endTimeOp.isPresent()) {
                // 确定zipper表和普通表
                TableView zipper = Materialization.TimePartType.ZIPPER
                        .equals(left.getDataModel().getTimePartType()) ? left : right;
                TableView partMetric = Materialization.TimePartType.ZIPPER
                        .equals(left.getDataModel().getTimePartType()) ? right : left;
                Optional<Dimension> partTime = Materialization.TimePartType.ZIPPER
                        .equals(left.getDataModel().getTimePartType()) ? rightTime : leftTime;

                // 构建字段引用
                startTime = zipper.getAlias() + "." + startTimeOp.get().getName();
                endTime = zipper.getAlias() + "." + endTimeOp.get().getName();
                dateTime = partMetric.getAlias() + "." + partTime.get().getName();
            }

            EngineType engineType = schema.getOntology().getDatabaseType();

            // 构建条件: startTime <= dateTime AND dateTime < endTime
            ArrayList<SqlNode> operandList =
                    new ArrayList<>(Arrays.asList(SemanticNode.parse(endTime, scope, engineType),
                            SemanticNode.parse(dateTime, scope, engineType)));
            condition = new SqlBasicCall(SqlStdOperatorTable.AND,
                    new ArrayList<SqlNode>(Arrays.asList(
                            new SqlBasicCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                                    new ArrayList<SqlNode>(Arrays.asList(
                                            SemanticNode.parse(startTime, scope, engineType),
                                            SemanticNode.parse(dateTime, scope, engineType))),
                                    SqlParserPos.ZERO, null),
                            new SqlBasicCall(SqlStdOperatorTable.GREATER_THAN, operandList,
                                    SqlParserPos.ZERO, null))),
                    SqlParserPos.ZERO, null);
        }
        return condition;
    }

    /**
     * 获取基本的join条件
     * @param left 左表视图
     * @param right 右表视图
     * @param dataModel 数据模型
     * @param schema 计算模式
     * @param scope SQL验证范围
     * @param engineType 引擎类型
     */
    private SqlNode getCondition(TableView left, TableView right, DataModel dataModel,
            S2CalciteSchema schema, SqlValidatorScope scope, EngineType engineType)
            throws Exception {

        Set<String> selectLeft = SemanticNode.getSelect(left.getTable());
        Set<String> selectRight = SemanticNode.getSelect(right.getTable());
        selectLeft.retainAll(selectRight);  // 这里有bug，会导致集合为空，因为列名带schema前缀

        SqlNode condition = null;
        for (String s : selectLeft) {
            if (!SourceRender.isDimension(s, dataModel, schema)) {
                continue;
            }
            if (IdentifyNode.isForeign(s, left.getDataModel().getIdentifiers())) {
                if (!IdentifyNode.isPrimary(s, right.getDataModel().getIdentifiers())) {
                    continue;
                }
            }
            if (IdentifyNode.isForeign(s, right.getDataModel().getIdentifiers())) {
                if (!IdentifyNode.isPrimary(s, left.getDataModel().getIdentifiers())) {
                    continue;
                }
            }
            List<SqlNode> ons = new ArrayList<>();
            ons.add(SemanticNode.parse(left.getAlias() + "." + s, scope, engineType));
            ons.add(SemanticNode.parse(right.getAlias() + "." + s, scope, engineType));
            if (condition == null) {
                condition = new SqlBasicCall(SqlStdOperatorTable.EQUALS, ons, SqlParserPos.ZERO, null);
                continue;
            }
            SqlNode addCondition = new SqlBasicCall(SqlStdOperatorTable.EQUALS, ons, SqlParserPos.ZERO, null);
            condition = new SqlBasicCall(SqlStdOperatorTable.AND,
                    new ArrayList<>(Arrays.asList(condition, addCondition)), SqlParserPos.ZERO, null);
        }
        return condition;
    }

    /**
     * 获取预定义join关系的条件
     * @param joinRelation join关系定义
     * @param scope SQL验证范围
     * @param engineType 引擎类型
     */
    private SqlNode getCondition(JoinRelation joinRelation, SqlValidatorScope scope,
            EngineType engineType) throws Exception {
        SqlNode condition = null;
        for (Triple<String, String, String> triple : joinRelation.getJoinCondition()) {
            List<SqlNode> ons = new ArrayList<>();
            ons.add(SemanticNode.parse(triple.getLeft(), scope, engineType));
            ons.add(SemanticNode.parse(triple.getRight(), scope, engineType));
            if (condition == null) {
                condition = new SqlBasicCall(SemanticNode.getBinaryOperator(triple.getMiddle()), ons,
                        SqlParserPos.ZERO, null);
                continue;
            }
            SqlNode addCondition = new SqlBasicCall(SemanticNode.getBinaryOperator(triple.getMiddle()),
                    ons, SqlParserPos.ZERO, null);
            condition = new SqlBasicCall(SqlStdOperatorTable.AND,
                    new ArrayList<>(Arrays.asList(condition, addCondition)), SqlParserPos.ZERO, null);
        }
        return condition;
    }

    /**
     * 构建join关系图
     */
    private JoinGraph buildJoinGraph(List<DataModel> dataModels, S2CalciteSchema schema) {
        JoinGraph graph = new JoinGraph();

        // 从schema中的joinRelations构建图
        for (JoinRelation relation : schema.getOntology().getJoinRelations()) {
            graph.addEdge(relation.getLeft(), relation.getRight(), relation);
        }

        return graph;
    }

    /**
     * 根据join图构建SQL join树
     */
    private SqlNode buildJoinTree(JoinGraph graph, List<DataModel> dataModels,
            List<String> whereFields, Set<String> queryMetrics, Set<String> queryDimension,
            String whereCondition, SqlValidatorScope scope, S2CalciteSchema schema) throws Exception {

        // 收集所有join相关的字段
        Set<String> joinFields = new HashSet<>();
        for (DataModel model : dataModels) {
            for (Identify identify : model.getIdentifiers()) {
                joinFields.add(identify.getName());
            }
        }

        String startTable = dataModels.get(0).getName();
        Set<String> visited = new HashSet<>();
        Map<String, SqlNode> joinNodes = new HashMap<>();
        Map<String, TableView> tableViews = new HashMap<>();

        // 初始化第一个表
        TableView firstTable = buildTableView(
            dataModels.get(0),
            whereFields,
            queryMetrics,
            queryDimension,
            whereCondition,
            scope,
            schema,
            joinFields  // 传入join字段
        );
        tableViews.put(startTable, firstTable);
        joinNodes.put(startTable,
            SemanticNode.buildAs(firstTable.getAlias(), getTable(firstTable, scope)));
        visited.add(startTable);

        // 处理剩余的表
        for (DataModel model : dataModels.subList(1, dataModels.size())) {
            String tableName = model.getName();
            if (!visited.contains(tableName)) {
                String joinWith = findJoinPartner(tableName, visited, graph);
                if (joinWith == null) {
                    throw new RuntimeException("Cannot find join path for table: " + tableName);
                }

                // 构建新表的TableView
                TableView tableView = buildTableView(
                    model,
                    whereFields,
                    queryMetrics,
                    queryDimension,
                    whereCondition,
                    scope,
                    schema,
                    joinFields  // 传入join字段
                );
                tableViews.put(tableName, tableView);

                // 构建join
                TableView leftTable = tableViews.get(joinWith);
                SqlNode leftNode = joinNodes.get(joinWith);

                // 获取join条件
                Map<String, String> tableAliases = new HashMap<>();
                tableAliases.put(joinWith, leftTable.getAlias());

                // 1. 首先检查是否有预定义的join关系
                JoinRelation matchJoinRelation = getMatchJoinRelation(tableAliases, tableView, schema);
                SqlNode condition;
                SqlLiteral joinType;

                if (matchJoinRelation != null && !CollectionUtils.isEmpty(matchJoinRelation.getJoinCondition())) {
                    // 使用预定义的join关系
                    condition = getCondition(matchJoinRelation, scope, schema.getOntology().getDatabaseType());
                    joinType = SemanticNode.getJoinSqlLiteral(matchJoinRelation.getJoinType());
                } else {
                    // 使用默认的join条件（基于相同字段名）
                    condition = getCondition(leftTable, tableView, model, schema, scope,
                            schema.getOntology().getDatabaseType());
                    joinType = SemanticNode.getJoinSqlLiteral("");  // 默认INNER JOIN
                }

                // 2. 检查是否需要处理zipper join
                if (Materialization.TimePartType.ZIPPER.equals(leftTable.getDataModel().getTimePartType())
                        || Materialization.TimePartType.ZIPPER.equals(tableView.getDataModel().getTimePartType())) {
                    SqlNode zipperCondition = getZipperCondition(leftTable, tableView, model, schema, scope);
                    if (zipperCondition != null) {
                        condition = condition != null ?
                            new SqlBasicCall(SqlStdOperatorTable.AND,
                                new ArrayList<>(Arrays.asList(condition, zipperCondition)),
                                SqlParserPos.ZERO, null)
                            : zipperCondition;
                    }
                }

                // 构建join节点
                SqlNode join = new SqlJoin(
                    SqlParserPos.ZERO,
                    leftNode,
                    SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                    joinType,
                    SemanticNode.buildAs(tableView.getAlias(), getTable(tableView, scope)),
                    SqlLiteral.createSymbol(JoinConditionType.ON, SqlParserPos.ZERO),
                    condition
                );

                joinNodes.put(tableName, join);
                visited.add(tableName);
            }
        }

        return joinNodes.get(dataModels.get(dataModels.size() - 1).getName());
    }

    /**
     * 在已访问的表中找到可以join的表
     */
    private String findJoinPartner(String table, Set<String> visited, JoinGraph graph) {
        for (String visitedTable : visited) {
            if (graph.hasEdge(table, visitedTable)) {
                return visitedTable;
            }
        }
        return null;
    }

    /**
     * 为数据模型构建TableView
     */
    private TableView buildTableView(DataModel dataModel, List<String> whereFields,
            Set<String> queryMetrics, Set<String> queryDimension, String whereCondition,
            SqlValidatorScope scope, S2CalciteSchema schema,
            Set<String> joinFields) throws Exception {

        // 1. 收集所有需要的字段
        Set<String> neededFields = new HashSet<>();

        // 1.1 添加where字段
        neededFields.addAll(whereFields.stream()
            .filter(field -> isFieldBelongToModel(field, dataModel))
            .collect(Collectors.toList()));

        // 1.2 添加查询度量
        neededFields.addAll(queryMetrics.stream()
            .filter(metric -> isMetricBelongToModel(metric, dataModel))
            .collect(Collectors.toSet()));

        // 1.3 添加查询维度
        neededFields.addAll(queryDimension.stream()
            .filter(dim -> isDimensionBelongToModel(dim, dataModel))
            .collect(Collectors.toSet()));

        // 1.4 添加用于join的字段
        neededFields.addAll(joinFields.stream()
            .filter(field -> isFieldBelongToModel(field, dataModel))
            .collect(Collectors.toSet()));

        // 1.5 添加主键和外键
        for (Identify identify : dataModel.getIdentifiers()) {
            neededFields.add(identify.getName());
        }

        // 2. 使用所有需要的字段构建TableView
        TableView tableView = SourceRender.renderOne(
            "",                // prefix
            new ArrayList<>(neededFields),  // 包含所有需要的字段
            Collections.emptySet(),   // metrics在neededFields中已包含
            Collections.emptySet(),   // dimensions在neededFields中已包含
            whereCondition,    // where条件
            dataModel,         // 数据模型
            scope,            // SQL验证范围
            schema,           // 计算模式
            true             // non-agg
        );

        // 3. 设置表的别名和数据模型
        String alias = Constants.JOIN_TABLE_PREFIX + dataModel.getName();
        tableView.setAlias(alias);
        tableView.setDataModel(dataModel);

        return tableView;
    }

    /**
     * 检查字段是否属于指定的数据模型
     */
    private boolean isFieldBelongToModel(String field, DataModel model) {
        // 移除可能的表前缀
        String cleanField = field.contains(".") ?
            field.substring(field.lastIndexOf(".") + 1) : field;

        // 检查是否是该模型的维度
        boolean isDimension = model.getDimensions().stream()
            .anyMatch(dim -> dim.getName().equals(cleanField));

        // 检查是否是该模型的度量
        boolean isMeasure = model.getMeasures().stream()
            .anyMatch(measure -> measure.getName().equals(cleanField));

        return isDimension || isMeasure;
    }

    /**
     * 检查度量是否属于指定的数据模型
     */
    private boolean isMetricBelongToModel(String metric, DataModel model) {
        return model.getMeasures().stream()
            .anyMatch(measure -> measure.getName().equals(metric));
    }

    /**
     * 检查维度是否属于指定的数据模型
     */
    private boolean isDimensionBelongToModel(String dimension, DataModel model) {
        return model.getDimensions().stream()
            .anyMatch(dim -> dim.getName().equals(dimension));
    }
}

/**
 * Join关系图
 */
class JoinGraph {
    private Map<String, Set<String>> adjacencyList;
    private Map<String, JoinRelation> joinConditions;

    public JoinGraph() {
        // 在构造函数中初始化Map
        this.adjacencyList = new HashMap<>();
        this.joinConditions = new HashMap<>();
    }

    public void addEdge(String table1, String table2, JoinRelation relation) {
        // 确保两个表都有对应的Set
        adjacencyList.computeIfAbsent(table1, k -> new HashSet<>());
        adjacencyList.computeIfAbsent(table2, k -> new HashSet<>());

        // 添加双向边
        adjacencyList.get(table1).add(table2);
        adjacencyList.get(table2).add(table1);

        // 存储join条件
        joinConditions.put(table1 + "_" + table2, relation);
        joinConditions.put(table2 + "_" + table1, relation);
    }

    public boolean hasEdge(String table1, String table2) {
        Set<String> neighbors = adjacencyList.get(table1);
        return neighbors != null && neighbors.contains(table2);
    }

    public JoinRelation getJoinRelation(String table1, String table2) {
        return joinConditions.get(table1 + "_" + table2);
    }

    public Set<String> getNeighbors(String table) {
        return adjacencyList.getOrDefault(table, Collections.emptySet());
    }
}
