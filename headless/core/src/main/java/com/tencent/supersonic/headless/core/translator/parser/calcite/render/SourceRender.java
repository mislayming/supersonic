package com.tencent.supersonic.headless.core.translator.parser.calcite.render;

import com.tencent.supersonic.common.pojo.enums.EngineType;
import com.tencent.supersonic.headless.core.pojo.JoinRelation;
import com.tencent.supersonic.headless.core.pojo.OntologyQuery;
import com.tencent.supersonic.headless.core.translator.parser.calcite.S2CalciteSchema;
import com.tencent.supersonic.headless.core.translator.parser.calcite.TableView;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.*;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Constants;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.DataModel;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Dimension;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Identify;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Materialization;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Measure;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Metric;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.tencent.supersonic.headless.core.translator.parser.s2sql.Constants.DIMENSION_DELIMITER;

/** process the table dataSet from the defined data model schema */
@Slf4j
public class SourceRender extends Renderer {

    public static TableView renderOne(String alias, List<String> fieldWheres,
            Set<String> reqMetrics, Set<String> reqDimensions, String queryWhere,
            DataModel datasource, SqlValidatorScope scope, S2CalciteSchema schema, boolean nonAgg)
            throws Exception {

        TableView dataSet = new TableView();
        TableView output = new TableView();
        Set<String> queryMetrics = new HashSet<>(reqMetrics);
        Set<String> queryDimensions = new HashSet<>(reqDimensions);
        List<String> fieldWhere = new ArrayList<>(fieldWheres);
        Map<String, String> extendFields = new HashMap<>();
        if (!fieldWhere.isEmpty()) {
            Set<String> dimensions = new HashSet<>();
            Set<String> metrics = new HashSet<>();
            whereDimMetric(fieldWhere, queryMetrics, queryDimensions, datasource, schema,
                    dimensions, metrics);
            queryMetrics.addAll(metrics);
            queryDimensions.addAll(dimensions);
            mergeWhere(fieldWhere, dataSet, output, queryMetrics, queryDimensions, extendFields,
                    datasource, scope, schema, nonAgg);
        }
        addTimeDimension(datasource, queryDimensions);

        // 收集所有需要GROUP BY的字段
        Set<String> groupByFields = new HashSet<>();
        
        // 处理度量字段
        for (String metric : queryMetrics) {
            MetricNode metricNode =
                    buildMetricNode(metric, datasource, scope, schema, nonAgg, alias);
            if (!metricNode.getAggNode().isEmpty()) {
                metricNode.getAggNode().entrySet().stream()
                        .forEach(m -> {
                            output.getMeasure().add(m.getValue());
                            // 如果是非聚合字段，需要加入GROUP BY
                            if (!m.getValue().toString().contains("COUNT(") && 
                                !m.getValue().toString().contains("SUM(") &&
                                !m.getValue().toString().contains("AVG(") &&
                                !m.getValue().toString().contains("MAX(") &&
                                !m.getValue().toString().contains("MIN(")) {
                                groupByFields.add(m.getKey());
                            }
                        });
            }
            if (metricNode.getNonAggNode() != null) {
                metricNode.getNonAggNode().entrySet().stream()
                        .forEach(m -> {
                            dataSet.getMeasure().add(m.getValue());
                            groupByFields.add(m.getKey());
                        });
            }
            if (metricNode.getMeasureFilter() != null) {
                metricNode.getMeasureFilter().entrySet().stream()
                        .forEach(m -> dataSet.getFilter().add(m.getValue()));
            }
        }

        // 处理维度字段 - 所有维度字段都需要加入GROUP BY
        for (String dimension : queryDimensions) {
            buildDimension(dimension.contains(Constants.DIMENSION_IDENTIFY) ? dimension : "",
                    dimension.contains(Constants.DIMENSION_IDENTIFY)
                            ? dimension.split(Constants.DIMENSION_IDENTIFY)[1]
                            : dimension,
                    datasource, schema, nonAgg, extendFields, dataSet, output, scope);
            groupByFields.add(dimension);
        }

        // 处理join字段 - join字段也需要加入GROUP BY
        if (!CollectionUtils.isEmpty(schema.getJoinRelations())) {
            for (JoinRelation relation : schema.getJoinRelations()) {
                boolean isLeft = relation.getLeft().equals(datasource.getName());
                boolean isRight = relation.getRight().equals(datasource.getName());
                if (isLeft || isRight) {
                    for(Triple<String, String, String> triple : relation.getJoinCondition()) {
                        String field = isLeft ? triple.getLeft() : triple.getRight();
                        groupByFields.add(field);
                        if (!groupByFields.contains(field)) {
                            Optional<Measure> optionalMeasure = Renderer.getMeasureByName(field, datasource);

                            if(optionalMeasure.isPresent()) {
                                Measure measure = optionalMeasure.get();
                                SqlNode joinNode = MeasureNode.buildNonAgg("", measure, scope, schema.getOntology().getDatabaseType());
                                dataSet.getMeasure().add(joinNode);
                            }
                            else {
                                Optional<Dimension> optionalDimension = Renderer.getDimensionByName(field, datasource);
                                if(optionalDimension.isPresent()) {
                                    Dimension dimension = optionalDimension.get();
                                    SqlNode joinNode = DimensionNode.build(dimension, scope, schema.getOntology().getDatabaseType());
                                    dataSet.getDimension().add(joinNode);
                                }
                                else {
                                    Optional<Identify> optionalIdentify = Renderer.getIdentifyByName(field, datasource);
                                    Identify identify = optionalIdentify.get();
                                    SqlNode joinNode = IdentifyNode.build(identify, scope, schema.getOntology().getDatabaseType());
                                    dataSet.getMeasure().add(joinNode);
                                }
                            }
                        }
                    }
                }
            }
        }

        // 设置GROUP BY字段
        dataSet.setGroupByFields(new ArrayList<>(groupByFields));

        output.setMeasure(SemanticNode.deduplicateNode(output.getMeasure()));
        dataSet.setMeasure(SemanticNode.deduplicateNode(dataSet.getMeasure()));

        SqlNode tableNode = DataModelNode.buildExtend(datasource, extendFields, scope);
        dataSet.setTable(tableNode);
        output.setTable(
                SemanticNode.buildAs(Constants.DATASOURCE_TABLE_OUT_PREFIX + datasource.getName()
                        + "_" + UUID.randomUUID().toString().substring(32), dataSet.build()));
        return output;
    }

    private static void buildDimension(String alias, String dimension, DataModel datasource,
            S2CalciteSchema schema, boolean nonAgg, Map<String, String> extendFields,
            TableView dataSet, TableView output, SqlValidatorScope scope) throws Exception {
        List<Dimension> dimensionList = schema.getDimensions().get(datasource.getName());
        EngineType engineType = schema.getOntology().getDatabaseType();
        boolean isAdd = false;
        if (!CollectionUtils.isEmpty(dimensionList)) {
            for (Dimension dim : dimensionList) {
                if (!dim.getName().equalsIgnoreCase(dimension)) {
                    continue;
                }
                dataSet.getMeasure().add(DimensionNode.buildArray(dim, scope, engineType));
                addExtendFields(dim, extendFields);
                if (nonAgg) {
                    output.getMeasure().add(DimensionNode.buildName(dim, scope, engineType));
                    isAdd = true;
                    continue;
                }

                if ("".equals(alias)/*|| (alias != null && alias.equals(dim.getName()))*/) {
                    output.getDimension().add(DimensionNode.buildName(dim, scope, engineType));
                } else {
                    output.getDimension()
                            .add(DimensionNode.buildNameAs(alias, dim, scope, engineType));
                }
                isAdd = true;
                break;
            }
        }
        if (!isAdd) {
            Optional<Identify> identify = datasource.getIdentifiers().stream()
                    .filter(i -> i.getName().equalsIgnoreCase(dimension)).findFirst();
            if (identify.isPresent()) {
                if (nonAgg) {
                    dataSet.getMeasure()
                            .add(SemanticNode.parse(identify.get().getName(), scope, engineType));
                    output.getMeasure()
                            .add(SemanticNode.parse(identify.get().getName(), scope, engineType));
                } else {
                    dataSet.getMeasure()
                            .add(SemanticNode.parse(identify.get().getName(), scope, engineType));
                    output.getDimension()
                            .add(SemanticNode.parse(identify.get().getName(), scope, engineType));
                }
                isAdd = true;
            }
        }
        if (isAdd) {
            return;
        }
        Optional<Dimension> dimensionOptional = getDimensionByName(dimension, datasource);
        if (dimensionOptional.isPresent()) {
            dataSet.getMeasure()
                    .add(DimensionNode.buildArray(dimensionOptional.get(), scope, engineType));
            addExtendFields(dimensionOptional.get(), extendFields);
            if (nonAgg) {
                output.getMeasure()
                        .add(DimensionNode.buildName(dimensionOptional.get(), scope, engineType));
                return;
            }
            output.getDimension()
                    .add(DimensionNode.buildName(dimensionOptional.get(), scope, engineType));
        }
    }

    private static void addExtendFields(Dimension dimension, Map<String, String> extendFields) {
        if (dimension.getDataType().isArray()) {
            if (Objects.nonNull(dimension.getExt())
                    && dimension.getExt().containsKey(DIMENSION_DELIMITER)) {
                extendFields.put(dimension.getExpr(),
                        (String) dimension.getExt().get(DIMENSION_DELIMITER));
            } else {
                extendFields.put(dimension.getExpr(), "");
            }
        }
    }

    private static List<SqlNode> getWhereMeasure(List<String> fields, Set<String> queryMetrics,
            Set<String> queryDimensions, Map<String, String> extendFields, DataModel datasource,
            SqlValidatorScope scope, S2CalciteSchema schema, boolean nonAgg) throws Exception {
        Iterator<String> iterator = fields.iterator();
        List<SqlNode> whereNode = new ArrayList<>();
        EngineType engineType = schema.getOntology().getDatabaseType();
        while (iterator.hasNext()) {
            String cur = iterator.next();
            if (queryDimensions.contains(cur) || queryMetrics.contains(cur)) {
                iterator.remove();
            }
        }
        for (String where : fields) {
            List<Dimension> dimensionList = schema.getDimensions().get(datasource.getName());
            boolean isAdd = false;
            if (!CollectionUtils.isEmpty(dimensionList)) {
                for (Dimension dim : dimensionList) {
                    if (!dim.getName().equalsIgnoreCase(where)) {
                        continue;
                    }
                    whereNode.addAll(DimensionNode.expand(dim, scope, engineType));
                    isAdd = true;
                }
            }
            Optional<Identify> identify = getIdentifyByName(where, datasource);
            if (identify.isPresent()) {
                whereNode.add(IdentifyNode.build(identify.get(), scope, engineType));
                isAdd = true;
            }
            if (isAdd) {
                continue;
            }
            Optional<Dimension> dimensionOptional = getDimensionByName(where, datasource);
            if (dimensionOptional.isPresent()) {
                whereNode.add(DimensionNode.buildArray(dimensionOptional.get(), scope, engineType));
                addExtendFields(dimensionOptional.get(), extendFields);
            }
        }
        return whereNode;
    }

    private static void mergeWhere(List<String> fields, TableView dataSet, TableView outputSet,
            Set<String> queryMetrics, Set<String> queryDimensions, Map<String, String> extendFields,
            DataModel datasource, SqlValidatorScope scope, S2CalciteSchema schema, boolean nonAgg)
            throws Exception {
        List<SqlNode> whereNode = getWhereMeasure(fields, queryMetrics, queryDimensions,
                extendFields, datasource, scope, schema, nonAgg);
        dataSet.getMeasure().addAll(whereNode);
        // getWhere(outputSet,fields,queryMetrics,queryDimensions,datasource,scope,schema);
    }

    public static void whereDimMetric(List<String> fields, Set<String> queryMetrics,
            Set<String> queryDimensions, DataModel datasource, S2CalciteSchema schema,
            Set<String> dimensions, Set<String> metrics) {
        for (String field : fields) {
            if (queryDimensions.contains(field) || queryMetrics.contains(field)) {
                continue;
            }
            String filterField = field;
            if (field.contains(Constants.DIMENSION_IDENTIFY)) {
                filterField = field.split(Constants.DIMENSION_IDENTIFY)[1];
            }
            addField(filterField, field, datasource, schema, dimensions, metrics);
        }
    }

    private static void addField(String field, String oriField, DataModel datasource,
            S2CalciteSchema schema, Set<String> dimensions, Set<String> metrics) {
        Optional<Dimension> dimension = datasource.getDimensions().stream()
                .filter(d -> d.getName().equalsIgnoreCase(field)).findFirst();
        if (dimension.isPresent()) {
            dimensions.add(oriField);
            return;
        }
        Optional<Identify> identify = datasource.getIdentifiers().stream()
                .filter(i -> i.getName().equalsIgnoreCase(field)).findFirst();
        if (identify.isPresent()) {
            dimensions.add(oriField);
            return;
        }
        if (schema.getDimensions().containsKey(datasource.getName())) {
            Optional<Dimension> dataSourceDim = schema.getDimensions().get(datasource.getName())
                    .stream().filter(d -> d.getName().equalsIgnoreCase(field)).findFirst();
            if (dataSourceDim.isPresent()) {
                dimensions.add(oriField);
                return;
            }
        }
        Optional<Measure> metric = datasource.getMeasures().stream()
                .filter(m -> m.getName().equalsIgnoreCase(field)).findFirst();
        if (metric.isPresent()) {
            metrics.add(oriField);
            return;
        }
        Optional<Metric> datasourceMetric = schema.getMetrics().stream()
                .filter(m -> m.getName().equalsIgnoreCase(field)).findFirst();
        if (datasourceMetric.isPresent()) {
            Set<String> measures = datasourceMetric.get().getMetricTypeParams().getMeasures()
                    .stream().map(m -> m.getName()).collect(Collectors.toSet());
            if (datasource.getMeasures().stream().map(m -> m.getName()).collect(Collectors.toSet())
                    .containsAll(measures)) {
                metrics.add(oriField);
                return;
            }
        }
    }

    public static boolean isDimension(String name, DataModel datasource, S2CalciteSchema schema) {
        Optional<Dimension> dimension = datasource.getDimensions().stream()
                .filter(d -> d.getName().equalsIgnoreCase(name)).findFirst();
        if (dimension.isPresent()) {
            return true;
        }
        Optional<Identify> identify = datasource.getIdentifiers().stream()
                .filter(i -> i.getName().equalsIgnoreCase(name)).findFirst();
        if (identify.isPresent()) {
            return true;
        }
        if (schema.getDimensions().containsKey(datasource.getName())) {
            Optional<Dimension> dataSourceDim = schema.getDimensions().get(datasource.getName())
                    .stream().filter(d -> d.getName().equalsIgnoreCase(name)).findFirst();
            if (dataSourceDim.isPresent()) {
                return true;
            }
        }
        return false;
    }

    private static void addTimeDimension(DataModel dataModel, Set<String> queryDimension) {
        if (Materialization.TimePartType.ZIPPER.equals(dataModel.getTimePartType())) {
            Optional<Dimension> startTimeOp = dataModel.getDimensions().stream()
                    .filter(d -> Constants.DIMENSION_TYPE_TIME.equalsIgnoreCase(d.getType()))
                    .filter(d -> d.getName().startsWith(Constants.MATERIALIZATION_ZIPPER_START))
                    .findFirst();
            Optional<Dimension> endTimeOp = dataModel.getDimensions().stream()
                    .filter(d -> Constants.DIMENSION_TYPE_TIME.equalsIgnoreCase(d.getType()))
                    .filter(d -> d.getName().startsWith(Constants.MATERIALIZATION_ZIPPER_END))
                    .findFirst();
            if (startTimeOp.isPresent() && !queryDimension.contains(startTimeOp.get().getName())) {
                queryDimension.add(startTimeOp.get().getName());
            }
            if (endTimeOp.isPresent() && !queryDimension.contains(endTimeOp.get().getName())) {
                queryDimension.add(endTimeOp.get().getName());
            }
        } else {
            Optional<Dimension> timeOp = dataModel.getDimensions().stream()
                    .filter(d -> Constants.DIMENSION_TYPE_TIME.equalsIgnoreCase(d.getType()))
                    .findFirst();
            if (timeOp.isPresent() && !queryDimension.contains(timeOp.get().getName())) {
                queryDimension.add(timeOp.get().getName());
            }
        }
    }

    public void render(OntologyQuery ontologyQuery, List<DataModel> dataModels,
            SqlValidatorScope scope, S2CalciteSchema schema, boolean nonAgg) throws Exception {
        String queryWhere = ontologyQuery.getWhere();
        Set<String> whereFields = new HashSet<>();
        List<String> fieldWhere = new ArrayList<>();
        EngineType engineType = schema.getOntology().getDatabaseType();
        if (queryWhere != null && !queryWhere.isEmpty()) {
            SqlNode sqlNode = SemanticNode.parse(queryWhere, scope, engineType);
            FilterNode.getFilterField(sqlNode, whereFields);
            fieldWhere = whereFields.stream().collect(Collectors.toList());
        }
        if (dataModels.size() == 1) {
            DataModel dataModel = dataModels.get(0);
            super.tableView = renderOne("", fieldWhere, ontologyQuery.getMetrics(),
                    ontologyQuery.getDimensions(), ontologyQuery.getWhere(), dataModel, scope,
                    schema, nonAgg);
            return;
        }
        JoinRender joinRender = new JoinRender();
        joinRender.render(ontologyQuery, dataModels, scope, schema, nonAgg);
        super.tableView = joinRender.getTableView();
    }

    public static TableView buildTableView(DataModel model, List<String> whereFields,
            Set<String> queryMetrics, Set<String> queryDimensions, String whereCondition,
            SqlValidatorScope scope, S2CalciteSchema schema, Set<String> neededFields) throws Exception {
        
        TableView dataSet = new TableView();
        TableView output = new TableView();
        
        // 收集所有需要GROUP BY的字段
        Set<String> groupByFields = new HashSet<>();
        
        // 1. 处理度量字段
        for (String metric : queryMetrics) {
            if (isMetricBelongToModel(metric, model)) {
                MetricNode metricNode = buildMetricNode(metric, model, scope, schema, false, "");
                if (metricNode.getNonAggNode() != null) {
                    metricNode.getNonAggNode().entrySet().stream()
                        .forEach(m -> {
                            dataSet.getMeasure().add(m.getValue());
                            // 非聚合字段需要加入GROUP BY
                            groupByFields.add(m.getKey());
                        });
                }
            }
        }
        
        // 2. 处理维度字段
        for (String dimension : queryDimensions) {
            if (isDimensionBelongToModel(dimension, model)) {
                buildDimension("", dimension, model, schema, false, new HashMap<>(),
                        dataSet, output, scope);
                // 维度字段需要加入GROUP BY
                groupByFields.add(dimension);
            }
        }
        
        // 3. 处理join和where条件中需要的字段
        for (String field : neededFields) {
            if (isFieldBelongToModel(field, model)) {
                // 如果字段还没有被添加到SELECT中，添加它
                if (!isFieldInMeasure(field, dataSet.getMeasure())) {
                    SqlNode fieldNode = buildFieldNode(field, model, scope, schema);
                    dataSet.getMeasure().add(fieldNode);
                    // 这些字段也需要加入GROUP BY
                    groupByFields.add(field);
                }
            }
        }
        
        // 设置GROUP BY字段
        dataSet.getGroupByFields().addAll(groupByFields);
        
        // 构建基础表查询
        SqlNode tableNode = DataModelNode.buildExtend(model, new HashMap<>(), scope);
        dataSet.setTable(tableNode);
        
        return dataSet;
    }

    private static boolean isFieldInMeasure(String field, List<SqlNode> measures) {
        return measures.stream()
            .anyMatch(node -> node.toString().contains(field));
    }

    private static SqlNode buildFieldNode(String field, DataModel model, 
            SqlValidatorScope scope, S2CalciteSchema schema) throws Exception {
        // 先检查是否是度量
        Optional<Measure> measure = getMeasureByName(field, model);
        if (measure.isPresent()) {
            return MeasureNode.buildNonAgg("", measure.get(), scope, 
                    schema.getOntology().getDatabaseType());
        }
        
        // 再检查是否是维度
        Optional<Dimension> dimension = getDimensionByName(field, model);
        if (dimension.isPresent()) {
            return DimensionNode.build(dimension.get(), scope, 
                    schema.getOntology().getDatabaseType());
        }
        
        // 最后检查是否是标识符
        Optional<Identify> identify = getIdentifyByName(field, model);
        if (identify.isPresent()) {
            return IdentifyNode.build(identify.get(), scope, 
                    schema.getOntology().getDatabaseType());
        }
        
        throw new RuntimeException("Field " + field + " not found in model " + model.getName());
    }
}
