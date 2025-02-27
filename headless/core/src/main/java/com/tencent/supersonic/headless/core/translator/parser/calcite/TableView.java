package com.tencent.supersonic.headless.core.translator.parser.calcite;

import com.tencent.supersonic.headless.core.translator.parser.s2sql.DataModel;
import lombok.Data;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** basic query project */
@Data
public class TableView {

    private List<SqlNode> filter = new ArrayList<>();
    private List<SqlNode> dimension = new ArrayList<>();
    private List<SqlNode> measure = new ArrayList<>();
    private SqlNodeList order;
    private SqlNode fetch;
    private SqlNode offset;
    private SqlNode table;

    private String alias;
    private List<String> primary;
    private DataModel dataModel;
    private List<String> groupByFields = new ArrayList<>();

    public SqlNode build() {
        measure.addAll(dimension);
        
        // 构建GROUP BY子句
        SqlNodeList groupByNodeList = null;
        if (!groupByFields.isEmpty()) {
            List<SqlNode> groupByNodes = new ArrayList<>();
            for (String field : groupByFields) {
                groupByNodes.add(new SqlIdentifier(
                    Arrays.asList(field.split("\\.")),
                    SqlParserPos.ZERO));
            }
            groupByNodeList = new SqlNodeList(groupByNodes, SqlParserPos.ZERO);
        }

        // 构建维度列表
        SqlNodeList dimensionNodeList = null;
        if (dimension.size() > 0) {
            dimensionNodeList = new SqlNodeList(getGroup(dimension), SqlParserPos.ZERO);
        }

        // 构建过滤条件
        SqlNodeList filterNodeList = null;
        if (filter.size() > 0) {
            filterNodeList = new SqlNodeList(filter, SqlParserPos.ZERO);
        }

        if (measure.isEmpty()) {
            // 添加 SELECT *
            SqlIdentifier star = new SqlIdentifier(
                    List.of("*"),
                    SqlParserPos.ZERO
            );
            measure.add(star);
        }

        // 在SqlSelect构造函数中使用groupByNodeList
        return new SqlSelect(
            SqlParserPos.ZERO,
            null,  // selectList
            new SqlNodeList(measure, SqlParserPos.ZERO),  // from
            table,  // where
            filterNodeList,  // groupBy
            groupByNodeList,  // having
            null,  // windowDecls
            null,  // orderBy
            null,  // offset
            order,  // fetch
            offset,  // hints
            fetch,
            null
        );
    }

    private List<SqlNode> getGroup(List<SqlNode> sqlNodeList) {
        return sqlNodeList.stream()
                .map(s -> (s.getKind().equals(SqlKind.AS)
                        ? ((SqlBasicCall) s).getOperandList().get(0)
                        : s))
                .collect(Collectors.toList());
    }
}
