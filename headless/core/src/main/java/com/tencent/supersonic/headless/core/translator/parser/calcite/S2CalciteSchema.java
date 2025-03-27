package com.tencent.supersonic.headless.core.translator.parser.calcite;

import com.tencent.supersonic.headless.core.pojo.JoinRelation;
import com.tencent.supersonic.headless.core.pojo.Ontology;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.*;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Builder
@Slf4j
public class S2CalciteSchema extends AbstractSchema {

    private String schemaKey;

    private Ontology ontology;

    private RuntimeOptions runtimeOptions;

    @Override
    public Schema snapshot(SchemaVersion version) {
        return this;
    }

    public Map<String, DataModel> getDataModels() {
        return ontology.getDataModelMap();
    }

    public List<Metric> getMetrics() {
        return ontology.getMetrics();
    }

    public Map<String, List<Dimension>> getDimensions() {
        return ontology.getDimensionMap();
    }

    public List<JoinRelation> getJoinRelations() {
        return ontology.getJoinRelations();
    }
    


    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tableMap = new HashMap<>();
        for (Map.Entry<String, DataModel> entry : getDataModels().entrySet()) {
            tableMap.put(entry.getKey(), createTableFromDataModel(entry.getValue()));
        }
        return tableMap;
    }

    private Table createTableFromDataModel(DataModel dataModel) {

        String tableName = dataModel.getName();

        S2CalciteTable.Builder builder = S2CalciteTable.newBuilder(tableName);
        int fieldsAdded = 0;


        for(Identify identify : dataModel.getIdentifiers()) {
            builder.addField(identify.getName(), SqlTypeName.ANY);
            fieldsAdded++;
        }

        for (Dimension dimension : dataModel.getDimensions()) {
            builder.addField(dimension.getName(), SqlTypeName.ANY);
            fieldsAdded++;
        }
        
        // 获取此模型的所有指标
        for (Measure measure : dataModel.getMeasures()) {
            builder.addField(measure.getName(), SqlTypeName.ANY);
            fieldsAdded++;
        }
        
        // 如果没有添加任何字段，至少添加一个默认字段，避免表创建失败
        if (fieldsAdded == 0) {
            builder.addField("dummy", SqlTypeName.ANY);
        }
        
        return builder.withRowCount(1).build();
    }
}
