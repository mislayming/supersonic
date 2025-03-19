package com.tencent.supersonic.headless.core.translator.parser.calcite;

import com.google.common.collect.Sets;
import com.tencent.supersonic.common.calcite.Configuration;
import com.tencent.supersonic.common.pojo.enums.EngineType;
import com.tencent.supersonic.headless.api.pojo.enums.AggOption;
import com.tencent.supersonic.headless.api.pojo.enums.IdentifyType;
import com.tencent.supersonic.headless.api.pojo.response.DatabaseResp;
import com.tencent.supersonic.headless.api.pojo.response.DimSchemaResp;
import com.tencent.supersonic.headless.api.pojo.response.MetricSchemaResp;
import com.tencent.supersonic.headless.api.pojo.response.ModelResp;
import com.tencent.supersonic.headless.core.pojo.JoinRelation;
import com.tencent.supersonic.headless.core.pojo.Ontology;
import com.tencent.supersonic.headless.core.pojo.OntologyQuery;
import com.tencent.supersonic.headless.core.pojo.QueryStatement;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.DataModelNode;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.SemanticNode;
import com.tencent.supersonic.headless.core.translator.parser.calcite.render.FilterRender;
import com.tencent.supersonic.headless.core.translator.parser.calcite.render.OutputRender;
import com.tencent.supersonic.headless.core.translator.parser.calcite.render.Renderer;
import com.tencent.supersonic.headless.core.translator.parser.calcite.render.SourceRender;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class SqlBuilder3 {

    private final S2CalciteSchema schema;
    private SqlValidatorScope scope;

    public SqlBuilder3(S2CalciteSchema schema) {
        this.schema = schema;
    }

    public String buildOntologySql(QueryStatement queryStatement) throws Exception {
        OntologyQuery ontologyQuery = queryStatement.getOntologyQuery();
        if (ontologyQuery.getLimit() == null) {
            ontologyQuery.setLimit(0L);
        }

        Ontology ontology = queryStatement.getOntology();
        SqlValidatorScope scope = SchemaBuilder.getScope(schema);
        this.scope = scope;

        List<DataModel> dataModels = DataModelNode.getQueryDataModels(scope, schema, ontologyQuery);
        if (dataModels.isEmpty()) {
            throw new Exception("data model not found");
        }

        TableView tableView;
        if (!CollectionUtils.isEmpty(ontology.getJoinRelations()) && dataModels.size() > 1) {
            tableView = render(ontologyQuery, new HashSet<>(dataModels), scope, schema);
        } else {
            tableView = render(ontologyQuery, new HashSet<>(dataModels), scope, schema);
        }

        SqlNode parserNode = tableView.build();
        DatabaseResp database = queryStatement.getOntology().getDatabase();
        EngineType engineType = EngineType.fromString(database.getType());
        parserNode = optimizeParseNode(parserNode, engineType);
        return SemanticNode.getSql(parserNode, engineType);
    }

    private TableView render(OntologyQuery ontologyQuery, Set<DataModel> dataModels,
                             SqlValidatorScope scope, S2CalciteSchema schema) throws Exception {
        TableView outerTable = new TableView();
        EngineType engineType = EngineType.fromString(schema.getOntology().getDatabase().getType());

        // 收集所有数据模型的TableView
        Map<String, TableView> tableViewMap = new HashMap<>();
        for (DataModel dataModel : dataModels) {
            final Set<Dimension> queryDimensions = new HashSet<>();

            // 从ontology获取该模型下的所有维度和指标
            Ontology ontology = schema.getOntology();
            List<Dimension> allDimensions = ontology.getDimensionMap().getOrDefault(dataModel.getName(), Collections.emptyList());

            // 查找维度和指标
            for (Dimension dim : allDimensions) {
                if (ontologyQuery.getDimensions().contains(dim.getName()) ||
                        ontologyQuery.getDimensions().contains(dim.getBizName())) {
                    queryDimensions.add(dim);
                }
            }

            List<Metric> allMetrics = ontology.getMetrics().stream()
                    .filter(metric -> dataModel.getMeasures().stream()
                            .anyMatch(measure -> metric.getName().equals(measure.getName())))
                    .toList();
            final Set<Metric> queryMetrics = new HashSet<>(allMetrics);

            // 创建单个模型的TableView
            TableView tableView = renderOne(queryMetrics, queryDimensions, dataModel, scope, schema);
            String alias = Constants.JOIN_TABLE_PREFIX + dataModel.getName();
            tableView.setAlias(alias);

            List<String> primary = new ArrayList<>();
            for (Identify identify : dataModel.getIdentifiers()) {
                primary.add(identify.getName());
            }
            tableView.setPrimary(primary);
            tableView.setDataModel(dataModel);

            // 保存到映射中
            tableViewMap.put(dataModel.getName(), tableView);
        }

        // 使用线性Join构建表
        SqlNode joinTree = buildLinearJoin(tableViewMap, schema, scope);

        // 收集所有字段
        for (TableView view : tableViewMap.values()) {
            for (String field : view.getFields()) {
                outerTable.getSelect().add(
                        SemanticNode.parse(view.getAlias() + "." + field, scope, engineType)
                );
            }
        }

        outerTable.setTable(joinTree);
        return outerTable;
    }

    /**
     * 构建线性Join
     */
    private SqlNode buildLinearJoin(Map<String, TableView> tableViewMap, S2CalciteSchema schema,
                                    SqlValidatorScope scope) throws Exception {
        if (tableViewMap.isEmpty()) {
            return null;
        }

        EngineType engineType = EngineType.fromString(schema.getOntology().getDatabase().getType());
        List<String> modelNames = new ArrayList<>(tableViewMap.keySet());

        // 起始节点
        String firstModel = modelNames.get(0);
        TableView firstView = tableViewMap.get(firstModel);
        SqlNode result = SemanticNode.buildAs(firstView.getAlias(), getTable(firstView));

        // 线性连接剩余表
        for (int i = 1; i < modelNames.size(); i++) {
            String currentModel = modelNames.get(i);
            TableView currentView = tableViewMap.get(currentModel);
            String previousModel = modelNames.get(i-1);

            // 查找连接关系
            JoinRelation joinRelation = findJoinRelation(previousModel, currentModel, schema.getJoinRelations());

            // 构建Join条件
            SqlNode condition = null;
            if (joinRelation != null && !CollectionUtils.isEmpty(joinRelation.getJoinCondition())) {
                List<Triple<String, String, String>> joinConditions;

                if (joinRelation.getLeft().equals(previousModel)) {
                    joinConditions = joinRelation.getJoinCondition().stream()
                            .map(con -> Triple.of(
                                    tableViewMap.get(previousModel).getAlias() + "." + con.getLeft(),
                                    con.getMiddle(),
                                    currentView.getAlias() + "." + con.getRight()))
                            .collect(Collectors.toList());
                } else {
                    joinConditions = joinRelation.getJoinCondition().stream()
                            .map(con -> Triple.of(
                                    tableViewMap.get(previousModel).getAlias() + "." + con.getRight(),
                                    con.getMiddle(),
                                    currentView.getAlias() + "." + con.getLeft()))
                            .collect(Collectors.toList());
                }

                condition = buildJoinConditionFromTriples(joinConditions, scope, engineType);
            }

            // 如果没有找到条件，使用默认条件
            if (condition == null) {
                log.warn("无法找到与{}的JOIN条件，使用1=1作为默认", currentModel);
                List<SqlNode> constantOns = new ArrayList<>();
                constantOns.add(SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO));
                constantOns.add(SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO));
                condition = new SqlBasicCall(SqlStdOperatorTable.EQUALS, constantOns, SqlParserPos.ZERO, null);
            }

            // 创建JOIN
            SqlLiteral joinType = SemanticNode.getJoinSqlLiteral(
                    joinRelation != null ? joinRelation.getJoinType() : "");

            result = new SqlJoin(
                    SqlParserPos.ZERO,
                    result,
                    SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                    joinType,
                    SemanticNode.buildAs(currentView.getAlias(), getTable(currentView)),
                    SqlLiteral.createSymbol(JoinConditionType.ON, SqlParserPos.ZERO),
                    condition
            );
        }

        return result;
    }

    /**
     * 从三元组列表构建JOIN条件
     */
    private SqlNode buildJoinConditionFromTriples(List<Triple<String, String, String>> conditions,
                                                  SqlValidatorScope scope, EngineType engineType) throws Exception {
        SqlNode condition = null;
        for (Triple<String, String, String> con : conditions) {
            List<SqlNode> ons = new ArrayList<>();
            ons.add(SemanticNode.parse(con.getLeft(), scope, engineType));
            ons.add(SemanticNode.parse(con.getRight(), scope, engineType));

            SqlNode current = new SqlBasicCall(
                    SemanticNode.getBinaryOperator(con.getMiddle()),
                    ons,
                    SqlParserPos.ZERO,
                    null
            );

            if (condition == null) {
                condition = current;
            } else {
                condition = new SqlBasicCall(
                        SqlStdOperatorTable.AND,
                        new ArrayList<>(Arrays.asList(condition, current)),
                        SqlParserPos.ZERO,
                        null
                );
            }
        }

        return condition;
    }

    /**
     * 查找两个模型之间的JOIN关系
     */
    private JoinRelation findJoinRelation(String model1, String model2, List<JoinRelation> joinRelations) {
        if (CollectionUtils.isEmpty(joinRelations)) {
            return null;
        }

        for (JoinRelation relation : joinRelations) {
            if ((relation.getLeft().equals(model1) && relation.getRight().equals(model2)) ||
                    (relation.getLeft().equals(model2) && relation.getRight().equals(model1))) {
                return relation;
            }
        }

        return null;
    }

    private SqlNode getTable(TableView tableView) {
        return SemanticNode.getTable(tableView.getTable());
    }

    public static TableView renderOne(Set<Metric> queryMetrics,
                                      Set<Dimension> queryDimensions, DataModel dataModel, SqlValidatorScope scope,
                                      S2CalciteSchema schema) {
        TableView tableView = new TableView();
        EngineType engineType = EngineType.fromString(schema.getOntology().getDatabase().getType());
        Set<String> queryFields = tableView.getFields();
        if (Objects.nonNull(queryMetrics)) {
            queryMetrics.stream().forEach(m -> queryFields.add(m.getName()));
        }
        if (Objects.nonNull(queryDimensions)) {
            queryDimensions.stream().forEach(d -> queryFields.add(d.getExpr()));
        }

        try {
            for (String field : queryFields) {
                tableView.getSelect().add(SemanticNode.parse(field, scope, engineType));
            }
            tableView.setTable(DataModelNode.build(dataModel, scope));
        } catch (Exception e) {
            log.error("Failed to create sqlNode for data model {}", dataModel);
        }

        return tableView;
    }

    private SqlNode optimizeParseNode(SqlNode parserNode, EngineType engineType)
            throws SqlParseException {
        if (Objects.isNull(schema.getRuntimeOptions())
                || Objects.isNull(schema.getRuntimeOptions().getEnableOptimize())
                || !schema.getRuntimeOptions().getEnableOptimize()) {
            return parserNode;
        }

        SqlNode optimizeNode = null;
        SqlNode sqlNode = SqlParser.create(SemanticNode.getSql(parserNode, engineType),
                Configuration.getParserConfig(engineType)).parseStmt();
        if (Objects.nonNull(sqlNode)) {
            optimizeNode = SemanticNode.optimize(scope, schema, sqlNode, engineType);
        }

        if (Objects.nonNull(optimizeNode)) {
            return optimizeNode;
        }

        return parserNode;
    }

    /**
     * 检查字段是否是主键标识
     */
    private static boolean isPrimary(String name, List<Identify> identifies) {
        Optional<Identify> identify =
                identifies.stream().filter(i -> i.getName().equalsIgnoreCase(name)).findFirst();
        return identify.filter(value -> IdentifyType.primary.toString().equals(value.getType())).isPresent();
    }

    /**
     * 检查字段是否是外键标识
     */
    private static boolean isForeign(String name, List<Identify> identifies) {
        Optional<Identify> identify =
                identifies.stream().filter(i -> i.getName().equalsIgnoreCase(name)).findFirst();
        return identify.filter(value -> IdentifyType.foreign.toString().equals(value.getType())).isPresent();
    }
}
