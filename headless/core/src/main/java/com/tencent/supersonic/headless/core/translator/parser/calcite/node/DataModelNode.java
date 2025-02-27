package com.tencent.supersonic.headless.core.translator.parser.calcite.node;

import com.google.common.collect.Lists;
import com.tencent.supersonic.common.calcite.Configuration;
import com.tencent.supersonic.common.jsqlparser.SqlSelectHelper;
import com.tencent.supersonic.common.pojo.enums.EngineType;
import com.tencent.supersonic.headless.core.pojo.JoinRelation;
import com.tencent.supersonic.headless.core.pojo.Ontology;
import com.tencent.supersonic.headless.core.pojo.OntologyQuery;
import com.tencent.supersonic.headless.core.translator.parser.calcite.S2CalciteSchema;
import com.tencent.supersonic.headless.core.translator.parser.calcite.SchemaBuilder;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class DataModelNode extends SemanticNode {

    public static SqlNode build(DataModel dataModel, SqlValidatorScope scope) throws Exception {
        String sqlTable = "";
        if (dataModel.getSqlQuery() != null && !dataModel.getSqlQuery().isEmpty()) {
            sqlTable = dataModel.getSqlQuery();
        } else if (dataModel.getTableQuery() != null && !dataModel.getTableQuery().isEmpty()) {
            if (dataModel.getType().equalsIgnoreCase(EngineType.POSTGRESQL.getName())) {
                String fullTableName =
                        String.join(".public.", dataModel.getTableQuery().split("\\."));
                sqlTable = "select * from " + fullTableName;
            } else {
                sqlTable = "select * from " + dataModel.getTableQuery();
            }
        }
        if (sqlTable.isEmpty()) {
            throw new Exception("DataModelNode build error [tableSqlNode not found]");
        }
        SqlNode source = getTable(sqlTable, scope, EngineType.fromString(dataModel.getType()));
        addSchema(scope, dataModel, sqlTable);
        return buildAs(dataModel.getName(), source);
    }

    private static void addSchema(SqlValidatorScope scope, DataModel datasource, String table)
            throws Exception {
        Map<String, Set<String>> sqlTable = SqlSelectHelper.getFieldsWithSubQuery(table);
        for (Map.Entry<String, Set<String>> entry : sqlTable.entrySet()) {
            String tb = entry.getKey();
            String db = "";
            if (entry.getKey().indexOf(".") > 0) {
                db = entry.getKey().substring(0, entry.getKey().lastIndexOf("."));
                tb = entry.getKey().substring(entry.getKey().lastIndexOf(".") + 1);
            }
            addSchemaTable(scope, datasource, db, tb, entry.getValue());
        }
    }

    private static void addSchemaTable(SqlValidatorScope scope, DataModel datasource, String db,
            String tb, Set<String> fields) throws Exception {
        Set<String> dateInfo = new HashSet<>();
        Set<String> dimensions = new HashSet<>();
        Set<String> metrics = new HashSet<>();
        EngineType engineType = EngineType.fromString(datasource.getType());
        for (Dimension d : datasource.getDimensions()) {
            List<SqlNode> identifiers =
                    expand(SemanticNode.parse(d.getExpr(), scope, engineType), scope);
            identifiers.forEach(i -> dimensions.add(i.toString()));
            dimensions.add(d.getName());
        }
        for (Identify i : datasource.getIdentifiers()) {
            dimensions.add(i.getName());
        }
        for (Measure m : datasource.getMeasures()) {
            List<SqlNode> identifiers =
                    expand(SemanticNode.parse(m.getExpr(), scope, engineType), scope);
            identifiers.forEach(i -> {
                if (!dimensions.contains(i.toString())) {
                    metrics.add(i.toString());
                }
            });
            if (!dimensions.contains(m.getName())) {
                metrics.add(m.getName());
            }
        }
        for (String field : fields) {
            if (!metrics.contains(field) && !dimensions.contains(field)) {
                dimensions.add(field);
                log.info("add column {} {}", datasource.getName(), field);
            }
        }
        SchemaBuilder.addSourceView(scope.getValidator().getCatalogReader().getRootSchema(), db, tb,
                dateInfo, dimensions, metrics);
    }

    public static SqlNode buildExtend(DataModel datasource, Map<String, String> exprList,
            SqlValidatorScope scope) throws Exception {
        if (CollectionUtils.isEmpty(exprList)) {
            return build(datasource, scope);
        }
        EngineType engineType = EngineType.fromString(datasource.getType());
        SqlNode dataSet = new SqlBasicCall(new LateralViewExplodeNode(exprList),
                Arrays.asList(build(datasource, scope), new SqlNodeList(
                        getExtendField(exprList, scope, engineType), SqlParserPos.ZERO)),
                SqlParserPos.ZERO);
        return buildAs(datasource.getName() + Constants.DIMENSION_ARRAY_SINGLE_SUFFIX, dataSet);
    }

    public static List<SqlNode> getExtendField(Map<String, String> exprList,
            SqlValidatorScope scope, EngineType engineType) throws Exception {
        List<SqlNode> sqlNodeList = new ArrayList<>();
        for (String expr : exprList.keySet()) {
            sqlNodeList.add(parse(expr, scope, engineType));
            sqlNodeList.add(new SqlDataTypeSpec(
                    new SqlUserDefinedTypeNameSpec(expr + Constants.DIMENSION_ARRAY_SINGLE_SUFFIX,
                            SqlParserPos.ZERO),
                    SqlParserPos.ZERO));
        }
        return sqlNodeList;
    }

    private static SqlNode getTable(String sqlQuery, SqlValidatorScope scope, EngineType engineType)
            throws Exception {
        SqlParser sqlParser = SqlParser.create(sqlQuery, Configuration.getParserConfig(engineType));
        SqlNode sqlNode = sqlParser.parseQuery();
        scope.validateExpr(sqlNode);
        return sqlNode;
    }

    public static String getNames(List<DataModel> dataModelList) {
        return dataModelList.stream().map(DataModel::getName).collect(Collectors.joining("_"));
    }

    public static void getQueryDimensionMeasure(Ontology ontology, OntologyQuery queryParam,
            Set<String> queryDimensions, Set<String> queryMeasures) {
        queryDimensions.addAll(queryParam.getDimensions().stream()
                .map(d -> d.contains(Constants.DIMENSION_IDENTIFY)
                        ? d.split(Constants.DIMENSION_IDENTIFY)[1]
                        : d)
                .collect(Collectors.toSet()));
        Set<String> schemaMetricName =
                ontology.getMetrics().stream().map(Metric::getName).collect(Collectors.toSet());
        ontology.getMetrics().stream().filter(m -> queryParam.getMetrics().contains(m.getName()))
                .forEach(m -> m.getMetricTypeParams().getMeasures()
                        .forEach(mm -> queryMeasures.add(mm.getName())));
        queryParam.getMetrics().stream().filter(m -> !schemaMetricName.contains(m))
                .forEach(queryMeasures::add);
    }

    public static void mergeQueryFilterDimensionMeasure(Ontology ontology, OntologyQuery queryParam,
            Set<String> dimensions, Set<String> measures, SqlValidatorScope scope)
            throws Exception {
        EngineType engineType = ontology.getDatabaseType();
        if (Objects.nonNull(queryParam.getWhere()) && !queryParam.getWhere().isEmpty()) {
            Set<String> filterConditions = new HashSet<>();
            FilterNode.getFilterField(parse(queryParam.getWhere(), scope, engineType),
                    filterConditions);
            Set<String> queryMeasures = new HashSet<>(measures);
            Set<String> schemaMetricName =
                    ontology.getMetrics().stream().map(Metric::getName).collect(Collectors.toSet());
            for (String filterCondition : filterConditions) {
                if (schemaMetricName.contains(filterCondition)) {
                    ontology.getMetrics().stream()
                            .filter(m -> m.getName().equalsIgnoreCase(filterCondition))
                            .forEach(m -> m.getMetricTypeParams().getMeasures()
                                    .forEach(mm -> queryMeasures.add(mm.getName())));
                    continue;
                }
                dimensions.add(filterCondition);
            }
            measures.clear();
            measures.addAll(queryMeasures);
        }
    }

    public static List<DataModel> getQueryDataModels(SqlValidatorScope scope,
            S2CalciteSchema schema, OntologyQuery queryParam) throws Exception {
        Ontology ontology = schema.getOntology();
        // get query measures and dimensions
        Set<String> queryMeasures = new HashSet<>();
        Set<String> queryDimensions = new HashSet<>();
        getQueryDimensionMeasure(ontology, queryParam, queryDimensions, queryMeasures);
        mergeQueryFilterDimensionMeasure(ontology, queryParam, queryDimensions, queryMeasures,
                scope);

        // first, find the base model
        DataModel baseDataModel = findBaseModel(ontology, queryMeasures, queryDimensions);
        if (Objects.isNull(baseDataModel)) {
            throw new RuntimeException(
                    String.format("could not find matching dataModel, dimensions:%s, measures:%s",
                            queryDimensions, queryMeasures));
        }
        // if the base model matches all queried measures and dimensions, just return
        if (checkMatch(baseDataModel, queryMeasures, queryDimensions)) {
            log.debug("baseDataModel match all measures and dimensions");
            return Collections.singletonList(baseDataModel);
        }

        // second, traverse the ontology to find other related dataModels
        List<DataModel> relatedDataModels = findRelatedModelsByRelation(ontology, queryParam,
                baseDataModel, queryDimensions, queryMeasures);
        if (CollectionUtils.isEmpty(relatedDataModels)) {
            relatedDataModels = findRelatedModelsByIdentifier(ontology, baseDataModel,
                    queryDimensions, queryMeasures);
        }
        if (CollectionUtils.isEmpty(relatedDataModels)) {
            relatedDataModels = Collections.singletonList(baseDataModel);
        }

        log.debug("relatedDataModels {}", relatedDataModels);
        return relatedDataModels;
    }

    private static DataModel findBaseModel(Ontology ontology, Set<String> queryMeasures,
            Set<String> queryDimensions) {
        DataModel dataModel = null;
        // first, try to find the model with the most matching measures
        Map<String, Integer> dataModelMeasuresCount = new HashMap<>();
        for (Map.Entry<String, DataModel> entry : ontology.getDataModelMap().entrySet()) {
            Set<String> sourceMeasure = entry.getValue().getMeasures().stream()
                    .map(Measure::getName).collect(Collectors.toSet());
            sourceMeasure.retainAll(queryMeasures);
            dataModelMeasuresCount.put(entry.getKey(), sourceMeasure.size());
        }
        log.info("dataModelMeasureCount: [{}]", dataModelMeasuresCount);
        Optional<Map.Entry<String, Integer>> base =
                dataModelMeasuresCount.entrySet().stream().filter(e -> e.getValue() > 0)
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).findFirst();

        if (base.isPresent()) {
            dataModel = ontology.getDataModelMap().get(base.get().getKey());
        } else {
            // second, try to find the model with the most matching dimensions
            Map<String, Integer> dataModelDimCount = new HashMap<>();
            for (Map.Entry<String, List<Dimension>> entry : ontology.getDimensionMap().entrySet()) {
                Set<String> modelDimensions = entry.getValue().stream().map(Dimension::getName)
                        .collect(Collectors.toSet());
                modelDimensions.retainAll(queryDimensions);
                dataModelDimCount.put(entry.getKey(), modelDimensions.size());
            }
            log.info("dataModelDimCount: [{}]", dataModelDimCount);
            base = dataModelDimCount.entrySet().stream().filter(e -> e.getValue() > 0)
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).findFirst();
            if (base.isPresent()) {
                dataModel = ontology.getDataModelMap().get(base.get().getKey());
            }
        }

        return dataModel;
    }

    private static boolean checkMatch(DataModel baseDataModel, Set<String> queryMeasures,
            Set<String> queryDimension) {
        boolean isAllMatch = true;
        Set<String> baseMeasures = baseDataModel.getMeasures().stream().map(Measure::getName)
                .collect(Collectors.toSet());
        Set<String> baseDimensions = baseDataModel.getDimensions().stream().map(Dimension::getName)
                .collect(Collectors.toSet());
        baseDataModel.getIdentifiers().forEach(i -> baseDimensions.add(i.getName()));

        baseMeasures.retainAll(queryMeasures);
        if (baseMeasures.size() < queryMeasures.size()) {
            // check dimension again
            Set<String> dimensionMeasures = new HashSet<>();
            dimensionMeasures.addAll(baseDimensions);
            dimensionMeasures.retainAll(queryMeasures);
            if (baseMeasures.size() + dimensionMeasures.size() < queryMeasures.size()) {
                log.info("baseDataModel not match all measures");
                isAllMatch = false;
            }
            queryMeasures.removeAll(dimensionMeasures);
        }
        queryMeasures.removeAll(baseMeasures);

        baseDimensions.retainAll(queryDimension);
        if (baseDimensions.size() < queryDimension.size()) {
            log.debug("baseDataModel not match all dimensions");
            isAllMatch = false;
        }
        queryDimension.removeAll(baseDimensions);

        return isAllMatch;
    }

    private static List<DataModel> findRelatedModelsByRelation(Ontology ontology,
            OntologyQuery queryParam, DataModel baseDataModel, Set<String> queryDimensions,
            Set<String> queryMeasures) {
        Set<String> joinDataModelNames = new HashSet<>();
        List<DataModel> joinDataModels = new ArrayList<>();
        
        if (!CollectionUtils.isEmpty(ontology.getJoinRelations())) {
            // 构建完整的join关系图
            Map<String, Set<String>> joinGraph = buildJoinGraph(ontology.getJoinRelations());
            
            // 找到所有需要的表
            Set<String> requiredTables = findTablesWithRequiredData(ontology, queryMeasures, queryDimensions);
            
            // 对于每个必需的表,找到从baseModel到该表的所有中间表
            Set<String> allNeededTables = new HashSet<>();
            allNeededTables.add(baseDataModel.getName());
            
            for (String targetTable : requiredTables) {
                if (!targetTable.equals(baseDataModel.getName())) {
                    // 找到所有可能的路径
                    Set<String> intermediateTables = findAllIntermediateTables(
                        joinGraph, 
                        baseDataModel.getName(),
                        targetTable
                    );
                    allNeededTables.addAll(intermediateTables);
                }
            }
            
            joinDataModelNames.addAll(allNeededTables);
        }
        
        // 转换为DataModel列表
        if (!CollectionUtils.isEmpty(joinDataModelNames)) {
            return orderDataModels(joinDataModelNames, ontology);
        }
        
        return Lists.newArrayList();
    }

    // 找到两个表之间所有可能的中间表
    private static Set<String> findAllIntermediateTables(
            Map<String, Set<String>> joinGraph, 
            String start, 
            String end) {
        Set<String> result = new HashSet<>();
        Set<String> visited = new HashSet<>();
        findPathsDFS(joinGraph, start, end, visited, result);
        return result;
    }

    private static void findPathsDFS(
            Map<String, Set<String>> joinGraph,
            String current,
            String target,
            Set<String> visited,
            Set<String> allTables) {
        
        visited.add(current);
        allTables.add(current);
        
        if (current.equals(target)) {
            return;
        }
        
        Set<String> neighbors = joinGraph.getOrDefault(current, Collections.emptySet());
        for (String next : neighbors) {
            if (!visited.contains(next)) {
                findPathsDFS(joinGraph, next, target, visited, allTables);
            }
        }
    }

    // 构建表之间的关联关系图
    private static Map<String, Set<String>> buildJoinGraph(List<JoinRelation> relations) {
        Map<String, Set<String>> graph = new HashMap<>();

        for (JoinRelation relation : relations) {
            graph.computeIfAbsent(relation.getLeft(), k -> new HashSet<>())
                    .add(relation.getRight());
            graph.computeIfAbsent(relation.getRight(), k -> new HashSet<>())
                    .add(relation.getLeft());
        }

        return graph;
    }

    // 找到包含所需数据的表
    private static Set<String> findTablesWithRequiredData(Ontology ontology,
                                                          Set<String> measures, Set<String> dimensions) {
        Set<String> tables = new HashSet<>();

        // 检查每个数据模型
        for (Map.Entry<String, DataModel> entry : ontology.getDataModelMap().entrySet()) {
            DataModel model = entry.getValue();

            // 检查度量
            boolean hasMeasure = model.getMeasures().stream()
                    .map(Measure::getName)
                    .anyMatch(measures::contains);

            // 检查维度
            boolean hasDimension = model.getDimensions().stream()
                    .map(Dimension::getName)
                    .anyMatch(dimensions::contains);

            if (hasMeasure || hasDimension) {
                tables.add(entry.getKey());
            }
        }

        return tables;
    }

    private static List<DataModel> orderDataModels(Set<String> joinDataModelNames, Ontology ontology) {
        List<DataModel> orderedModels = new ArrayList<>();

        // 如果只有一个模型，直接返回
        if (joinDataModelNames.size() <= 1) {
            joinDataModelNames.forEach(name ->
                    orderedModels.add(ontology.getDataModelMap().get(name)));
            return orderedModels;
        }

        // 构建表之间的关联关系图
        Map<String, Set<JoinRelation>> joinGraph = new HashMap<>();
        for (JoinRelation relation : ontology.getJoinRelations()) {
            if (joinDataModelNames.contains(relation.getLeft()) &&
                    joinDataModelNames.contains(relation.getRight())) {
                joinGraph.computeIfAbsent(relation.getLeft(), k -> new HashSet<>()).add(relation);
                joinGraph.computeIfAbsent(relation.getRight(), k -> new HashSet<>()).add(relation);
            }
        }

        // 使用拓扑排序确定表的顺序
        Set<String> visited = new HashSet<>();
        Set<String> visiting = new HashSet<>();
        Stack<String> orderStack = new Stack<>();

        // 对每个未访问的表进行深度优先搜索
        for (String modelName : joinDataModelNames) {
            if (!visited.contains(modelName)) {
                if (!topologicalSort(modelName, joinGraph, visited, visiting, orderStack)) {
                    // 如果检测到循环依赖，使用默认顺序
                    return new ArrayList<>(joinDataModelNames.stream()
                            .map(name -> ontology.getDataModelMap().get(name))
                            .collect(Collectors.toList()));
                }
            }
        }

        // 从栈中取出排序后的表名并构建结果
        while (!orderStack.isEmpty()) {
            String modelName = orderStack.pop();
            orderedModels.add(ontology.getDataModelMap().get(modelName));
        }

        return orderedModels;
    }

    private static boolean topologicalSort(String current,
                                           Map<String, Set<JoinRelation>> joinGraph,
                                           Set<String> visited,
                                           Set<String> visiting,
                                           Stack<String> orderStack) {

        // 检测循环依赖
        if (visiting.contains(current)) {
            return false;
        }

        // 如果已经访问过，直接返回
        if (visited.contains(current)) {
            return true;
        }

        visiting.add(current);

        // 访问所有相邻的表
        Set<JoinRelation> relations = joinGraph.getOrDefault(current, new HashSet<>());
        for (JoinRelation relation : relations) {
            String next = relation.getLeft().equals(current) ?
                    relation.getRight() : relation.getLeft();

            if (!topologicalSort(next, joinGraph, visited, visiting, orderStack)) {
                return false;
            }
        }

        visiting.remove(current);
        visited.add(current);
        orderStack.push(current);

        return true;
    }

    private static List<DataModel> findRelatedModelsByIdentifier(Ontology ontology,
            DataModel baseDataModel, Set<String> queryDimension, Set<String> measures) {
        Set<String> baseIdentifiers = baseDataModel.getIdentifiers().stream().map(Identify::getName)
                .collect(Collectors.toSet());
        if (baseIdentifiers.isEmpty()) {
            return Collections.EMPTY_LIST;
        }

        Set<String> linkDataSourceName = new HashSet<>();
        List<DataModel> linkDataModels = new ArrayList<>();
        for (Map.Entry<String, DataModel> entry : ontology.getDataModelMap().entrySet()) {
            if (entry.getKey().equalsIgnoreCase(baseDataModel.getName())) {
                continue;
            }
            long identifierNum = entry.getValue().getIdentifiers().stream().map(Identify::getName)
                    .filter(baseIdentifiers::contains).count();
            if (identifierNum > 0) {
                boolean isMatch = false;
                if (!queryDimension.isEmpty()) {
                    Set<String> linkDimension = entry.getValue().getDimensions().stream()
                            .map(Dimension::getName).collect(Collectors.toSet());
                    entry.getValue().getIdentifiers().forEach(i -> linkDimension.add(i.getName()));
                    linkDimension.retainAll(queryDimension);
                    if (!linkDimension.isEmpty()) {
                        isMatch = true;
                    }
                }
                if (!measures.isEmpty()) {
                    Set<String> linkMeasure = entry.getValue().getMeasures().stream()
                            .map(Measure::getName).collect(Collectors.toSet());
                    linkMeasure.retainAll(measures);
                    if (!linkMeasure.isEmpty()) {
                        isMatch = true;
                    }
                }
                if (isMatch) {
                    linkDataSourceName.add(entry.getKey());
                }
            }
        }
        for (Map.Entry<String, List<Dimension>> entry : ontology.getDimensionMap().entrySet()) {
            if (!queryDimension.isEmpty()) {
                Set<String> linkDimension = entry.getValue().stream().map(Dimension::getName)
                        .collect(Collectors.toSet());
                linkDimension.retainAll(queryDimension);
                if (!linkDimension.isEmpty()) {
                    linkDataSourceName.add(entry.getKey());
                }
            }
        }
        for (String linkName : linkDataSourceName) {
            linkDataModels.add(ontology.getDataModelMap().get(linkName));
        }
        if (!CollectionUtils.isEmpty(linkDataModels)) {
            List<DataModel> all = new ArrayList<>();
            all.add(baseDataModel);
            all.addAll(linkDataModels);
            return all;
        }
        return Lists.newArrayList();
    }

}
