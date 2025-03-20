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
import lombok.SneakyThrows;
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
import org.jgrapht.Graph;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.shortestpath.DijkstraShortestPath;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DefaultUndirectedGraph;
import com.tencent.supersonic.common.calcite.SemanticSqlDialect;
import com.tencent.supersonic.common.calcite.SqlDialectFactory;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

@Slf4j
public class SqlBuilder2 {

    private final S2CalciteSchema schema;
    private SqlValidatorScope scope;


    public SqlBuilder2(S2CalciteSchema schema) {
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
//        List<DataModel> dataModels = DataModelNode.getQueryDataModels(queryStatement);
        if (dataModels.isEmpty()) {
            throw new Exception("data model not found");
        }

        TableView tableView = null;
        if (!CollectionUtils.isEmpty(ontology.getJoinRelations()) && dataModels.size() > 1) {
            Set<DataModel> models = probeRelatedModels(dataModels, queryStatement.getOntology());
            tableView = render(ontologyQuery, models, scope, schema);
        } else {
            tableView = render(ontologyQuery, new HashSet<>(dataModels), scope, schema);
        }

        SqlNode parserNode = tableView.build();
        DatabaseResp database = queryStatement.getOntology().getDatabase();
        EngineType engineType = EngineType.fromString(database.getType());

        parserNode = optimizeParseNode(parserNode, engineType);
        
        String sql = SemanticNode.getSql(parserNode, engineType);
        log.debug("最终生成的SQL: {}", sql);
        
        return sql;
    }

    private TableView render(OntologyQuery ontologyQuery, Set<DataModel> dataModels,
                             SqlValidatorScope scope, S2CalciteSchema schema) throws Exception {
        TableView outerTable = new TableView();
        Map<String, SqlNode> outerSelect = new HashMap<>();
        
        // 收集所有数据模型的TableView
        Map<String, TableView> tableViewMap = new HashMap<>();
        for (DataModel dataModel : dataModels) {
            final Set<Dimension> queryDimensions = new HashSet<>();


            // 从ontology获取该模型下的所有维度和指标
            Ontology ontology = schema.getOntology();
            List<Dimension> allDimensions = ontology.getDimensionMap().getOrDefault(dataModel.getName(), Collections.emptyList());


            // 查找维度和指标（保持原有逻辑）
            for (Dimension dim : allDimensions) {
                if (ontologyQuery.getDimensions().contains(dim.getName()) || 
                    ontologyQuery.getDimensions().contains(dim.getBizName())) {
                    queryDimensions.add(dim);
                }
            }

            // TODO 最新版本，其实这里都重构过了，过来的对象实际上不知道属于哪个 model 的，我这里简单做了一个 filter, 重名或者 expr 都歇逼
            List<Metric> allMetrics = ontology.getMetrics().stream().filter(metric -> dataModel.getMeasures().stream().anyMatch(measure -> metric.getName().equals(measure.getName()))).toList();
            final Set<Metric> queryMetrics = new HashSet<>(allMetrics);
            /*
            for (Metric metric : allMetrics) {
                if (ontologyQuery.getMetrics().contains(metric.getName())) {
                    queryMetrics.add(metric);
                }
            }
            */

            List<String> primary = new ArrayList<>();
            for (Identify identify : dataModel.getIdentifiers()) {
                primary.add(identify.getName());
            }

            // 创建单个模型的TableView
            TableView tableView = renderOne(queryMetrics, queryDimensions, dataModel, scope, schema);
            String alias = Constants.JOIN_TABLE_PREFIX + dataModel.getName();
            tableView.setAlias(alias);
            tableView.setPrimary(primary);
            tableView.setDataModel(dataModel);
            
            // 保存到映射中
            tableViewMap.put(dataModel.getName(), tableView);
            
            // 收集所有字段用于最终输出
            for (String field : tableView.getFields()) {
                outerSelect.put(field, createSafeFieldNode(alias + "." + field, scope));
            }
        }
        
        // 构建JOIN树
        SqlNode joinTree = buildJoinTree(tableViewMap, schema, scope);
        
//        // 将所有字段添加到输出视图
//        for (Map.Entry<String, SqlNode> entry : outerSelect.entrySet()) {
//            outerTable.getSelect().add(entry.getValue());
//        }
        outerTable.setTable(joinTree);
        
        return outerTable;
    }

    /**
     * 构建JOIN树，支持复杂的JOIN结构
     */
    private SqlNode buildJoinTree(Map<String, TableView> tableViewMap, S2CalciteSchema schema, 
                                  SqlValidatorScope scope) throws Exception {
        if (tableViewMap.isEmpty()) {
            return null;
        }
        
        if (tableViewMap.size() == 1) {
            TableView singleView = tableViewMap.values().iterator().next();
            return SemanticNode.buildAs(singleView.getAlias(), getTable(singleView));
        }
        
        // 如果连接关系为空，回退到线性JOIN
        if (CollectionUtils.isEmpty(schema.getJoinRelations())) {
            return buildLinearJoin(tableViewMap, schema, scope);
        }
        
        // 构建JOIN图
        Map<String, Set<JoinInfo>> joinGraph = buildJoinGraph(tableViewMap.keySet(), schema.getJoinRelations());
        
        // 使用克鲁斯卡尔算法获取最小生成树
        List<JoinInfo> mstEdges = getMSTEdges(joinGraph, tableViewMap.keySet());
        
        // 如果没有找到足够的边来连接所有节点，回退到线性JOIN
        if (mstEdges.size() < tableViewMap.size() - 1) {
            log.warn("无法找到连接所有模型的生成树，回退到线性JOIN");
            return buildLinearJoin(tableViewMap, schema, scope);
        }
        
        // 执行JOIN操作
        return executeJoinPlan(mstEdges, tableViewMap, schema, scope);
    }

    /**
     * 定义JOIN信息类
     */
    private static class JoinInfo {
        String leftModel;
        String rightModel;
        JoinRelation relation;
        
        public JoinInfo(String leftModel, String rightModel, JoinRelation relation) {
            this.leftModel = leftModel;
            this.rightModel = rightModel;
            this.relation = relation;
        }
    }

    /**
     * 构建JOIN关系图
     */
    private Map<String, Set<JoinInfo>> buildJoinGraph(Set<String> modelNames, List<JoinRelation> joinRelations) {
        Map<String, Set<JoinInfo>> joinGraph = new HashMap<>();
        
        // 初始化图
        for (String model : modelNames) {
            joinGraph.put(model, new HashSet<>());
        }
        
        // 添加所有连接关系
        for (JoinRelation relation : joinRelations) {
            if (modelNames.contains(relation.getLeft()) && modelNames.contains(relation.getRight())) {
                JoinInfo info = new JoinInfo(relation.getLeft(), relation.getRight(), relation);
                joinGraph.get(relation.getLeft()).add(info);
                joinGraph.get(relation.getRight()).add(info);
            }
        }
        
        return joinGraph;
    }

    /**
     * 使用克鲁斯卡尔算法找出最小生成树边
     */
    private List<JoinInfo> getMSTEdges(Map<String, Set<JoinInfo>> joinGraph, Set<String> modelNames) {
        List<JoinInfo> result = new ArrayList<>();
        
        // 使用并查集记录已连接的组件
        Map<String, String> parent = new HashMap<>();
        for (String model : modelNames) {
            parent.put(model, model);
        }
        
        // 定义查找根节点函数
        BiFunction<Map<String, String>, String, String> find = new BiFunction<Map<String, String>, String, String>() {
            @Override
            public String apply(Map<String, String> parent, String x) {
                if (!parent.get(x).equals(x)) {
                    parent.put(x, this.apply(parent, parent.get(x)));
                }
                return parent.get(x);
            }
        };
        
        // 收集所有边
        Set<JoinInfo> allEdges = new HashSet<>();
        for (Set<JoinInfo> edges : joinGraph.values()) {
            allEdges.addAll(edges);
        }
        
        // 克鲁斯卡尔算法
        for (JoinInfo edge : allEdges) {
            String rootLeft = find.apply(parent, edge.leftModel);
            String rootRight = find.apply(parent, edge.rightModel);
            
            if (!rootLeft.equals(rootRight)) {
                result.add(edge);
                parent.put(rootLeft, rootRight);  // 合并两个连通分量
            }
            
            // 如果已经有足够的边连接所有节点，就退出
            if (result.size() == modelNames.size() - 1) {
                break;
            }
        }
        
        return result;
    }

    /**
     * 执行JOIN计划
     */
    private SqlNode executeJoinPlan(List<JoinInfo> joinEdges, Map<String, TableView> tableViewMap,
                                   S2CalciteSchema schema, SqlValidatorScope scope) throws Exception {
        // 如果没有edges，直接返回第一个表
        if (joinEdges.isEmpty()) {
            TableView singleView = tableViewMap.values().iterator().next();
            return SemanticNode.buildAs(singleView.getAlias(), getTable(singleView));
        }
        
        EngineType engineType = EngineType.fromString(schema.getOntology().getDatabase().getType());
        
        // 构建JOIN树
        // 我们使用一个Map来跟踪每个模型对应的SqlNode
        Map<String, SqlNode> sqlNodeMap = new HashMap<>();

        // 维护一个模型名到当前所在JOIN组的映射
        Map<String, String> modelToGroup = new HashMap<>();
        
        // 追踪每个模型的原始别名，用于构建JOIN条件
        Map<String, String> modelOriginalAlias = new HashMap<>();
        
        // 初始化每个模型的SqlNode
        for (Map.Entry<String, TableView> entry : tableViewMap.entrySet()) {
            String modelName = entry.getKey();
            TableView view = entry.getValue();
            
            // 先存储原始表，不添加别名
            sqlNodeMap.put(modelName, getTable(view));
            modelToGroup.put(modelName, modelName); // 初始时，每个模型自己是一个组
            
            // 记录原始表的别名
            modelOriginalAlias.put(modelName, view.getAlias());
        }

        // 依次处理每条边
        for (JoinInfo joinInfo : joinEdges) {
            // 获取最新的左右模型所在的组
            String leftGroup = modelToGroup.get(joinInfo.leftModel);
            String rightGroup = modelToGroup.get(joinInfo.rightModel);
            
            // 获取左右节点
            SqlNode leftNode = sqlNodeMap.get(leftGroup);
            SqlNode rightNode = sqlNodeMap.get(rightGroup);
            
            // 给原始表添加别名，JOIN结构不添加别名
            if (leftGroup.equals(joinInfo.leftModel)) {
                String alias = modelOriginalAlias.get(joinInfo.leftModel);
                leftNode = SemanticNode.buildAs(alias, leftNode);
            }
            
            if (rightGroup.equals(joinInfo.rightModel)) {
                String alias = modelOriginalAlias.get(joinInfo.rightModel);
                rightNode = SemanticNode.buildAs(alias, rightNode);
            }
            
            // 构建JOIN条件
            // 根据JOIN关系提取条件字段
            List<Triple<String, String, String>> joinConditions = new ArrayList<>();
            if (!CollectionUtils.isEmpty(joinInfo.relation.getJoinCondition())) {
                for (Triple<String, String, String> con : joinInfo.relation.getJoinCondition()) {
                    // 左表字段引用
                    String leftFieldRef;
                    String leftTableAlias = modelOriginalAlias.get(joinInfo.leftModel);
                    leftFieldRef = leftTableAlias + "." + con.getLeft();
                    
                    // 右表字段引用
                    String rightFieldRef;
                    String rightTableAlias = modelOriginalAlias.get(joinInfo.rightModel);
                    rightFieldRef = rightTableAlias + "." + con.getRight();
                    
                    joinConditions.add(Triple.of(leftFieldRef, con.getMiddle(), rightFieldRef));
                }
            } else {
                // 如果没有显式条件，使用1=1
                joinConditions.add(Triple.of("1", "=", "1"));
            }
            
            // 构建SQL条件节点
            SqlNode condition = buildJoinConditionFromTriples(joinConditions, scope);
            
            // 创建JOIN节点 - 不为JOIN结构添加别名
            SqlLiteral joinType = SemanticNode.getJoinSqlLiteral(joinInfo.relation.getJoinType());
            SqlNode joinNode = new SqlJoin(
                SqlParserPos.ZERO,
                leftNode,  // 已经处理过别名
                SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                joinType,
                rightNode,  // 已经处理过别名
                SqlLiteral.createSymbol(JoinConditionType.ON, SqlParserPos.ZERO),
                condition
            );

            // 创建新的JOIN组
            String newGroup = leftGroup + "_" + rightGroup;
            sqlNodeMap.put(newGroup, joinNode);
            
            // 移除原始节点，防止重复使用
            sqlNodeMap.remove(leftGroup);
            sqlNodeMap.remove(rightGroup);
            
            // 更新所有引用左右组的模型到新组
            for (String model : new HashSet<>(modelToGroup.keySet())) {
                if (modelToGroup.get(model).equals(leftGroup) || modelToGroup.get(model).equals(rightGroup)) {
                    modelToGroup.put(model, newGroup);
                }
            }
        }
        
        // 获取最终JOIN结果
        if (sqlNodeMap.size() != 1) {
            log.error("JOIN树结果异常，预期1个节点，实际有{}个: {}", 
                    sqlNodeMap.size(), String.join(", ", sqlNodeMap.keySet()));
            throw new Exception("构建JOIN树异常，最终节点数不为1");
        }
        
        SqlNode finalJoinNode = sqlNodeMap.values().iterator().next();
        
        // 创建显式字段列表，处理重复字段问题
        SqlNodeList selectItems = createSelectItemsForJoin(joinEdges, tableViewMap, modelOriginalAlias, scope);
        
        // 创建带明确字段列表的SELECT语句
        return new SqlSelect(
            SqlParserPos.ZERO,      // pos
            SqlNodeList.EMPTY,      // keywordList
            selectItems,            // selectList
            finalJoinNode,          // from
            null,                   // where
            null,                   // groupBy
            null,                   // having
            null,                   // windowDecls
            null,                   // qualify
            null,                   // orderBy
            null,                   // offset
            null,                   // fetch
            null                    // hints
        );
    }
    
    /**
     * 为JOIN创建字段列表，处理重名字段问题
     */
    private SqlNodeList createSelectItemsForJoin(List<JoinInfo> joinEdges, 
                                               Map<String, TableView> tableViewMap,
                                               Map<String, String> modelOriginalAlias,
                                               SqlValidatorScope scope) {
        // 创建显式字段列表，处理重复字段问题
        List<SqlNode> selectItems = new ArrayList<>();
        
        // 记录已添加字段，用于检测重复
        Set<String> addedFields = new HashSet<>();
        
        // 检查是否有JOIN结构，如果有，需要特殊处理字段选择
        if (joinEdges.isEmpty()) {
            // 单表情况，直接使用*
            selectItems.add(new SqlIdentifier("*", SqlParserPos.ZERO));
        } else {
            // 多表JOIN情况，按顺序处理每个表的字段
            
            // 确定字段的优先级顺序 - 使用JOIN顺序的逆序，即"主动JOIN其他表"的表优先
            List<String> modelPriorityOrder = new ArrayList<>();
            
            // 使用已有的JOIN边构建模型优先级
            Map<String, Integer> joinInitiativeCount = new HashMap<>();
            
            for (JoinInfo joinInfo : joinEdges) {
                // 计算每个表作为JOIN左侧的次数
                joinInitiativeCount.put(joinInfo.leftModel, 
                        joinInitiativeCount.getOrDefault(joinInfo.leftModel, 0) + 1);
            }
            
            // 按照主动JOIN次数排序，主动JOIN次数多的表优先
            List<Map.Entry<String, Integer>> sortedModels = 
                    new ArrayList<>(joinInitiativeCount.entrySet());
            sortedModels.sort(Map.Entry.<String, Integer>comparingByValue().reversed());
            
            for (Map.Entry<String, Integer> entry : sortedModels) {
                modelPriorityOrder.add(entry.getKey());
            }
            
            // 添加可能遗漏的表(被JOIN但没有主动JOIN其他表)
            for (String model : tableViewMap.keySet()) {
                if (!modelPriorityOrder.contains(model)) {
                    modelPriorityOrder.add(model);
                }
            }
            
            log.info("表字段优先级顺序: {}", modelPriorityOrder);
            

            for (String modelName : modelPriorityOrder) {
                TableView view = tableViewMap.get(modelName);
                if (view == null) {
                    continue;
                }
                
                String alias = modelOriginalAlias.get(modelName);
                
                for (String field : view.getFields()) {
                    if (addedFields.contains(field)) {
                        // 字段已存在，使用表别名作为前缀添加
                        SqlNode columnRef;
                        try {
                            // 使用安全方法创建字段引用
                            columnRef = createSafeFieldNode(alias + "." + field, scope);
                            
                            SqlNode aliasNode = new SqlIdentifier(alias + "_" + field, SqlParserPos.ZERO);
                            selectItems.add(
                                new SqlBasicCall(
                                    SqlStdOperatorTable.AS,
                                    new SqlNode[]{ columnRef, aliasNode },
                                    SqlParserPos.ZERO
                                )
                            );
                        } catch (Exception e) {
                            log.error("Error creating field reference for {}.{}: {}", 
                                    alias, field, e.getMessage());
                            // 出错时使用原始方法作为回退
                            SqlNode[] nodes = new SqlNode[]{
                                new SqlIdentifier(Arrays.asList(alias, field), SqlParserPos.ZERO),
                                new SqlIdentifier(alias + "_" + field, SqlParserPos.ZERO)
                            };
                            selectItems.add(
                                new SqlBasicCall(
                                    SqlStdOperatorTable.AS,
                                    nodes,
                                    SqlParserPos.ZERO
                                )
                            );
                        }
                    } else {
                        // 字段不存在，直接添加
                        try {
                            // 使用安全方法创建字段引用
                            SqlNode columnRef = createSafeFieldNode(alias + "." + field, scope);
                            selectItems.add(columnRef);
                        } catch (Exception e) {
                            log.error("Error creating field reference for {}.{}: {}", 
                                    alias, field, e.getMessage());
                            // 出错时使用原始方法作为回退
                            selectItems.add(
                                new SqlIdentifier(Arrays.asList(alias, field), SqlParserPos.ZERO)
                            );
                        }
                        addedFields.add(field);
                    }
                }
            }
        }
        
        return new SqlNodeList(selectItems, SqlParserPos.ZERO);
    }

    /**
     * 构建一个简单的线性JOIN（作为后备方案）
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
        
        // 记录已处理的模型别名
        Map<String, String> processedAliases = new HashMap<>();
        processedAliases.put(firstModel, firstView.getAlias());
        
        // 线性连接剩余表
        for (int i = 1; i < modelNames.size(); i++) {
            String currentModel = modelNames.get(i);
            TableView currentView = tableViewMap.get(currentModel);
            
            // 尝试找到连接条件
            SqlNode condition = null;
            for (String processedModel : processedAliases.keySet()) {
                TableView processedView = tableViewMap.get(processedModel);
                JoinRelation joinRelation = findJoinRelation(processedModel, currentModel, schema.getJoinRelations());
                
                if (joinRelation != null && !CollectionUtils.isEmpty(joinRelation.getJoinCondition())) {
                    List<Triple<String, String, String>> joinConditions;
                    
                    if (joinRelation.getLeft().equals(processedModel)) {
                        joinConditions = joinRelation.getJoinCondition().stream()
                            .map(con -> Triple.of(
                                processedAliases.get(processedModel) + "." + con.getLeft(),
                                con.getMiddle(),
                                currentView.getAlias() + "." + con.getRight()))
                            .collect(Collectors.toList());
                    } else {
                        joinConditions = joinRelation.getJoinCondition().stream()
                            .map(con -> Triple.of(
                                processedAliases.get(processedModel) + "." + con.getRight(),
                                con.getMiddle(),
                                currentView.getAlias() + "." + con.getLeft()))
                            .collect(Collectors.toList());
                    }
                    
                    condition = buildJoinConditionFromTriples(joinConditions, scope);
                    break;
                }
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
            SqlLiteral joinType = SemanticNode.getJoinSqlLiteral("");
            result = new SqlJoin(
                SqlParserPos.ZERO,
                result,
                SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                joinType,
                SemanticNode.buildAs(currentView.getAlias(), getTable(currentView)),
                SqlLiteral.createSymbol(JoinConditionType.ON, SqlParserPos.ZERO),
                condition
            );
            
            // 更新已处理模型
            processedAliases.put(currentModel, currentView.getAlias());
        }
        
        return result;
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

    @SneakyThrows
    public TableView renderOne(Set<Metric> queryMetrics,
                                      Set<Dimension> queryDimensions, DataModel dataModel, SqlValidatorScope scope,
                                      S2CalciteSchema schema) {
        TableView tableView = new TableView();

        Set<String> queryFields = tableView.getFields();
        if (Objects.nonNull(queryMetrics)) {
            // TODO Metric也是如此，bizname 呢？
            queryMetrics.forEach(m -> queryFields.add(m.getName()));
        }
        if (Objects.nonNull(queryDimensions)) {
            // TODO 这里就奇怪！！， exp 还是 bizname，还是 name
            queryDimensions.forEach(d -> queryFields.add(d.getExpr()));
        }

//        try {
//            for (String field : queryFields) {
//                // 直接使用字段名时，不需要处理别名和字段的组合
//                tableView.getSelect().add(createSafeFieldNode(field, scope));
//            }
            tableView.setTable(DataModelNode.build(dataModel, scope));
//        } catch (Exception e) {
//            log.error("Failed to create sqlNode for data model {}", dataModel);
//        }

        return tableView;
    }
    
    /**
     * 创建安全的字段引用，处理关键字等问题
     */
    private SqlNode createSafeFieldNode(String field, SqlValidatorScope scope) throws Exception {
        try {
            // SemanticNode.parse 现在已经能处理所有情况，包括复杂表达式中的关键字
            return SemanticNode.parse(field, scope, EngineType.fromString(schema.getOntology().getDatabase().getType()));
        } catch (Exception e) {
            // 记录错误并抛出异常
            log.error("Failed to parse field [{}]: {}", field, e.getMessage());
            throw e;
        }
    }


    private Set<DataModel> probeRelatedModels(List<DataModel> dataModels, Ontology ontology) {
        List<JoinRelation> joinRelations = ontology.getJoinRelations();
        
        // 如果没有连接关系或只有一个模型，直接返回
        if (CollectionUtils.isEmpty(joinRelations) || dataModels.size() <= 1) {
            return new HashSet<>(dataModels);
        }
        
        // 收集所有查询涉及的模型名称
        Set<String> queryModelNames = dataModels.stream()
                .map(DataModel::getName)
                .collect(Collectors.toSet());
        
        // 构建无向图
        Graph<String, DefaultEdge> graph = buildGraph(joinRelations);
        
        // 使用并查集找出所有连通分量
        Map<String, String> parent = new HashMap<>();
        // 初始化，每个模型是自己的根
        for (String model : queryModelNames) {
            parent.put(model, model);
        }
        
        // 添加图中的所有其他模型
        for (JoinRelation relation : joinRelations) {
            String leftModel = relation.getLeft();
            String rightModel = relation.getRight();
            
            if (!parent.containsKey(leftModel)) {
                parent.put(leftModel, leftModel);
            }
            if (!parent.containsKey(rightModel)) {
                parent.put(rightModel, rightModel);
            }
        }
        
        // 合并连通分量
        for (JoinRelation relation : joinRelations) {
            String leftRoot = findRoot(parent, relation.getLeft());
            String rightRoot = findRoot(parent, relation.getRight());
            
            if (!leftRoot.equals(rightRoot)) {
                parent.put(leftRoot, rightRoot);
            }
        }
        
        // 查找所有查询模型所在的连通分量
        Set<String> queryModelRoots = new HashSet<>();
        for (String queryModel : queryModelNames) {
            queryModelRoots.add(findRoot(parent, queryModel));
        }
        
        // 收集所有需要的模型
        Set<String> neededModelNames = new HashSet<>(queryModelNames);
        
        // 对于每个连通分量，如果包含查询模型，我们需要添加构建最小生成树所需的模型
        if (queryModelRoots.size() > 1) {
            // 多个连通分量：需要找到能连接这些分量的"桥接"模型
            log.info("查询涉及多个独立的模型组，尝试找到连接它们的桥接模型");
            neededModelNames.addAll(findPathModels(queryModelNames, graph, "桥接模型"));
        } else if (queryModelNames.size() > 1) {
            // 单个连通分量但有多个查询模型：使用最小生成树算法找出连接它们的必要模型
            log.info("查询涉及同一连通分量的多个模型，寻找最小生成树上的必要模型");
            neededModelNames.addAll(findPathModels(queryModelNames, graph, "MST必要模型"));
        }
        
        // 将模型名称转换回DataModel对象
        Map<String, DataModel> modelMap = ontology.getDataModelMap();
        Set<DataModel> result = neededModelNames.stream()
                .filter(modelMap::containsKey)
                .map(modelMap::get)
                .collect(Collectors.toSet());
        
        log.info("最终参与JOIN的模型: {}", result.stream()
                .map(DataModel::getName)
                .collect(Collectors.joining(", ")));
        
        return result;
    }

    /**
     * 查找并查集中元素的根
     */
    private String findRoot(Map<String, String> parent, String x) {
        if (!parent.containsKey(x)) {
            return x; // 如果元素不在并查集中，它就是自己的根
        }
        
        if (!parent.get(x).equals(x)) {
            parent.put(x, findRoot(parent, parent.get(x))); // 路径压缩
        }
        return parent.get(x);
    }

    /**
     * 查找连接模型所需的所有路径模型
     * @param queryModels 查询模型集合
     * @param graph 模型连接关系图
     * @param modelType 模型类型描述（用于日志）
     * @return 所有需要的模型名称
     */
    private Set<String> findPathModels(Set<String> queryModels, Graph<String, DefaultEdge> graph, String modelType) {
        Set<String> pathModels = new HashSet<>();
        
        // 使用最短路径算法找出连接模型的路径
        DijkstraShortestPath<String, DefaultEdge> dijkstra = new DijkstraShortestPath<>(graph);
        
        List<String> queryModelList = new ArrayList<>(queryModels);
        int pathCount = 0;
        
        for (int i = 0; i < queryModelList.size(); i++) {
            for (int j = i + 1; j < queryModelList.size(); j++) {
                String source = queryModelList.get(i);
                String target = queryModelList.get(j);
                
                GraphPath<String, DefaultEdge> path = dijkstra.getPath(source, target);
                if (path != null) {
                    // 收集路径上的所有顶点
                    List<String> vertexList = path.getVertexList();
                    pathModels.addAll(vertexList);
                    pathCount++;
                    
                    log.debug("找到从{}到{}的路径，包含{}个模型", 
                             source, target, vertexList.size());
                }
            }
        }
        
        log.info("为查询模型找到{}条路径，共需要{}个{}",
                pathCount, pathModels.size(), modelType);
        
        return pathModels;
    }

    private Graph<String, DefaultEdge> buildGraph(List<JoinRelation> joinRelations) {
        Graph<String, DefaultEdge> directedGraph = new DefaultUndirectedGraph<>(DefaultEdge.class);
        for (JoinRelation joinRelation : joinRelations) {
            directedGraph.addVertex(joinRelation.getLeft());
            directedGraph.addVertex(joinRelation.getRight());
            directedGraph.addEdge(joinRelation.getLeft(), joinRelation.getRight());
        }
        return directedGraph;
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
     * 从三元组列表构建JOIN条件
     */
    private SqlNode buildJoinConditionFromTriples(List<Triple<String, String, String>> conditions,
                                                SqlValidatorScope scope) throws Exception {

        SqlNode condition = null;
        for (Triple<String, String, String> con : conditions) {
            List<SqlNode> ons = new ArrayList<>();
            ons.add(createSafeFieldNode(con.getLeft(), scope));
            ons.add(createSafeFieldNode(con.getRight(), scope));
            
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


}
