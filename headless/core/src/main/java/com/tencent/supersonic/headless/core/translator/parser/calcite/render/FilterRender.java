package com.tencent.supersonic.headless.core.translator.parser.calcite.render;

import com.tencent.supersonic.common.pojo.enums.EngineType;
import com.tencent.supersonic.headless.core.pojo.OntologyQuery;
import com.tencent.supersonic.headless.core.translator.parser.calcite.S2CalciteSchema;
import com.tencent.supersonic.headless.core.translator.parser.calcite.TableView;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.FilterNode;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.MetricNode;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.SemanticNode;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Constants;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.DataModel;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.Metric;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** process query specified filtering information */
public class FilterRender extends Renderer {

    @Override
    public void render(OntologyQuery metricCommand, List<DataModel> dataModels,
            SqlValidatorScope scope, S2CalciteSchema schema, boolean nonAgg) throws Exception {
        TableView tableView = super.tableView;
        SqlNode filterNode = null;
        EngineType engineType = schema.getOntology().getDatabaseType();

        // 只处理where条件
        if (metricCommand.getWhere() != null && !metricCommand.getWhere().isEmpty()) {
            filterNode = SemanticNode.parse(metricCommand.getWhere(), scope, engineType);
        }

        // 如果有where条件，创建过滤视图
        if (filterNode != null) {
            TableView filterView = new TableView();
            filterView.setTable(SemanticNode.buildAs(Constants.DATASOURCE_TABLE_FILTER_PREFIX,
                    tableView.build()));
            filterView.getFilter().add(filterNode);
            // 直接使用原表的字段，因为join已经处理好了字段重复的问题
            filterView.getMeasure().addAll(tableView.getMeasure());
            super.tableView = filterView;
        }
    }
}
