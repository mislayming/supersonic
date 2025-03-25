package com.tencent.supersonic.headless.core.translator.parser.calcite;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.DataModel;
import lombok.Data;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** basic query project */
@Data
public class TableView {

    private Set<String> fields = Sets.newHashSet();
    // private List<SqlNode> select = Lists.newArrayList();

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
                groupByNodes.add(
                        new SqlIdentifier(Arrays.asList(field.split("\\.")), SqlParserPos.ZERO));
            }
            groupByNodeList = new SqlNodeList(groupByNodes, SqlParserPos.ZERO);
        }



        // 构建过滤条件
        SqlNodeList filterNodeList = null;
        if (!filter.isEmpty()) {
            filterNodeList = new SqlNodeList(filter, SqlParserPos.ZERO);
        }

        if (measure.isEmpty()) {
            measure.add(SqlIdentifier.STAR);
        }


        // 在SqlSelect构造函数中使用groupByNodeList
        return new SqlSelect(SqlParserPos.ZERO, // pos
                null, // keywordList
                new SqlNodeList(measure, SqlParserPos.ZERO), // selectList
                table, // from
                filterNodeList, // where
                groupByNodeList, // groupBy
                null, // having
                null, // windowDecls
                null, // qualify
                order, // orderBy
                offset, // offset
                fetch, // fetch
                null // hints
        );
    }
}
