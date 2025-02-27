package com.tencent.supersonic.headless.core.translator.parser.calcite.render;

import com.tencent.supersonic.common.pojo.enums.EngineType;
import com.tencent.supersonic.headless.core.pojo.OntologyQuery;
import com.tencent.supersonic.headless.core.translator.parser.calcite.S2CalciteSchema;
import com.tencent.supersonic.headless.core.translator.parser.calcite.TableView;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.MeasureNode;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.MetricNode;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.SemanticNode;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.DataModel;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Dimension;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Identify;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Measure;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Metric;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** process TableView */
@Data
public abstract class Renderer {

    protected TableView tableView = new TableView();

    public static Optional<Dimension> getDimensionByName(String name, DataModel datasource) {
        return datasource.getDimensions().stream().filter(d -> d.getName().equalsIgnoreCase(name))
                .findFirst();
    }

    public static Optional<Measure> getMeasureByName(String name, DataModel datasource) {
        return datasource.getMeasures().stream().filter(mm -> mm.getName().equalsIgnoreCase(name))
                .findFirst();
    }

    public static Optional<Metric> getMetricByName(String name, S2CalciteSchema schema) {
        Optional<Metric> metric = schema.getMetrics().stream()
                .filter(m -> m.getName().equalsIgnoreCase(name)).findFirst();
        return metric;
    }

    public static Optional<Identify> getIdentifyByName(String name, DataModel datasource) {
        return datasource.getIdentifiers().stream().filter(i -> i.getName().equalsIgnoreCase(name))
                .findFirst();
    }

    public static MetricNode buildMetricNode(String metric, DataModel datasource,
            SqlValidatorScope scope, S2CalciteSchema schema, boolean nonAgg, String alias)
            throws Exception {
        Optional<Metric> metricOpt = getMetricByName(metric, schema);
        MetricNode metricNode = new MetricNode();
        EngineType engineType = EngineType.fromString(datasource.getType());
        if (metricOpt.isPresent()) {
            metricNode.setMetric(metricOpt.get());
            for (Measure m : metricOpt.get().getMetricTypeParams().getMeasures()) {
                Optional<Measure> measure = getMeasureByName(m.getName(), datasource);
                if (measure.isPresent()) {
                    metricNode.getNonAggNode().put(measure.get().getName(),
                            MeasureNode.buildNonAgg(alias, measure.get(), scope, engineType));
                    metricNode.getAggNode().put(measure.get().getName(),
                            MeasureNode.buildAgg(measure.get(), nonAgg, scope, engineType));
                    metricNode.getAggFunction().put(measure.get().getName(),
                            measure.get().getAgg());

                } else {
                    metricNode.getNonAggNode().put(m.getName(),
                            MeasureNode.buildNonAgg(alias, m, scope, engineType));
                    metricNode.getAggNode().put(m.getName(),
                            MeasureNode.buildAgg(m, nonAgg, scope, engineType));
                    metricNode.getAggFunction().put(m.getName(), m.getAgg());
                }
                if (m.getConstraint() != null && !m.getConstraint().isEmpty()) {
                    metricNode.getMeasureFilter().put(m.getName(),
                            SemanticNode.parse(m.getConstraint(), scope, engineType));
                }
            }
            return metricNode;
        }
        Optional<Measure> measure = getMeasureByName(metric, datasource);
        if (measure.isPresent()) {
            metricNode.getNonAggNode().put(measure.get().getName(),
                    MeasureNode.buildNonAgg(alias, measure.get(), scope, engineType));
            metricNode.getAggNode().put(measure.get().getName(),
                    MeasureNode.buildAgg(measure.get(), nonAgg, scope, engineType));
            metricNode.getAggFunction().put(measure.get().getName(), measure.get().getAgg());

            if (measure.get().getConstraint() != null && !measure.get().getConstraint().isEmpty()) {
                metricNode.getMeasureFilter().put(measure.get().getName(),
                        SemanticNode.parse(measure.get().getConstraint(), scope, engineType));
            }
        }
        return metricNode;
    }

    public static List<String> uniqList(List<String> list) {
        Set<String> tmp = new HashSet<>(list);
        return tmp.stream().collect(Collectors.toList());
    }

    public void setTable(SqlNode table) {
        tableView.setTable(table);
    }

    public SqlNode builder() {
        return tableView.build();
    }

    public SqlNode builderAs(String alias) throws Exception {
        return SemanticNode.buildAs(alias, tableView.build());
    }

    // 检查度量是否属于指定的数据模型
    protected static boolean isMetricBelongToModel(String metric, DataModel model) {
        if (model == null || CollectionUtils.isEmpty(model.getMeasures())) {
            return false;
        }
        return model.getMeasures().stream()
                .anyMatch(m -> m.getName().equals(metric));
    }

    // 检查维度是否属于指定的数据模型
    protected static boolean isDimensionBelongToModel(String dimension, DataModel model) {
        if (model == null || CollectionUtils.isEmpty(model.getDimensions())) {
            return false;
        }
        return model.getDimensions().stream()
                .anyMatch(d -> d.getName().equals(dimension));
    }

    // 检查字段是否属于指定的数据模型（可以是度量、维度或标识符）
    protected static boolean isFieldBelongToModel(String field, DataModel model) {
        // 检查是否是度量
        if (isMetricBelongToModel(field, model)) {
            return true;
        }

        // 检查是否是维度
        if (isDimensionBelongToModel(field, model)) {
            return true;
        }

        // 检查是否是标识符
        if (model != null && !CollectionUtils.isEmpty(model.getIdentifiers())) {
            return model.getIdentifiers().stream()
                    .anyMatch(i -> i.getName().equals(field));
        }

        return false;
    }

    public TableView getTableView() {
        return tableView;
    }

    public abstract void render(OntologyQuery metricCommand, List<DataModel> dataModels,
            SqlValidatorScope scope, S2CalciteSchema schema, boolean nonAgg) throws Exception;


    private static final Pattern SCHEMA_PATTERN = Pattern.compile("SCHEMA_[a-f0-9]{32}");
    private static final Pattern TABLE_ALIAS_PATTERN = Pattern.compile("(src\\d+_SCHEMA_[a-f0-9]{32})");
    private static final Pattern RESULT_ALIAS_PATTERN = Pattern.compile("(SCHEMA_[a-f0-9]{32}_SCHEMA_[a-f0-9]{32}.*?_\\d+)");
    private static final Pattern MDV_TABLE_PATTERN = Pattern.compile("__mdv_table_SCHEMA_[a-f0-9]{32}__");

    public static String simplifySQL(String sql) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }

        String result = sql;

        // 1. 收集所有模式ID并简化
        Map<String, String> schemaMap = new HashMap<>();
        Matcher schemaMatcher = SCHEMA_PATTERN.matcher(sql);
        int schemaCount = 1;
        while (schemaMatcher.find()) {
            String schema = schemaMatcher.group();
            if (!schemaMap.containsKey(schema)) {
                schemaMap.put(schema, "S" + schemaCount++);
            }
        }

        // 如果找到了模式ID，进行替换
        if (!schemaMap.isEmpty()) {
            // 2. 替换所有模式ID
            for (Map.Entry<String, String> entry : schemaMap.entrySet()) {
                result = result.replace(entry.getKey(), entry.getValue());
            }

            // 3. 简化表别名 (src1_SCHEMA_xxx -> src1_S1)
            Matcher aliasMatcher = TABLE_ALIAS_PATTERN.matcher(result);
            while (aliasMatcher.find()) {
                String alias = aliasMatcher.group();
                String simplified = alias;
                for (Map.Entry<String, String> entry : schemaMap.entrySet()) {
                    simplified = simplified.replace(entry.getKey(), entry.getValue());
                }
                result = result.replace(alias, simplified);
            }

            // 4. 简化结果集别名 (SCHEMA_xxx_SCHEMA_yyy_n -> S1_S2_n)
            Matcher resultMatcher = RESULT_ALIAS_PATTERN.matcher(result);
            while (resultMatcher.find()) {
                String alias = resultMatcher.group();
                String simplified = alias;
                for (Map.Entry<String, String> entry : schemaMap.entrySet()) {
                    simplified = simplified.replace(entry.getKey(), entry.getValue());
                }
                result = result.replace(alias, simplified);
            }

            // 5. 简化MDV表名
            Matcher mdvMatcher = MDV_TABLE_PATTERN.matcher(result);
            while (mdvMatcher.find()) {
                String tableName = mdvMatcher.group();
                String simplified = tableName;
                for (Map.Entry<String, String> entry : schemaMap.entrySet()) {
                    simplified = simplified.replace(entry.getKey(), entry.getValue());
                }
                result = result.replace(tableName, simplified);
            }
        }

        System.out.println(result);
        return result;
    }
}
