package com.tencent.supersonic.headless.chat.corrector;

import com.tencent.supersonic.common.jsqlparser.SqlSelectHelper;
import com.tencent.supersonic.common.jsqlparser.SqlValidHelper;
import com.tencent.supersonic.headless.api.pojo.SemanticParseInfo;
import com.tencent.supersonic.headless.chat.ChatQueryContext;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MDVSqlCorrector extends BaseSemanticCorrector {

    @Override
    public void doCorrect(ChatQueryContext chatQueryContext, SemanticParseInfo semanticParseInfo) {
        try {
            String correctS2SQL = semanticParseInfo.getSqlInfo().getCorrectedS2SQL();
            if (SqlValidHelper.isComplexSQL(correctS2SQL)) {
                return;
            }
            supplementGroupByFields(chatQueryContext, semanticParseInfo);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /***
     * 补齐Group By的字段，在SELECT中出现的字段，但是没有聚合，需要在GROUP BY中补齐。
     * 例子：SELECT 招标单, 招标单名称 FROM 招标模型 GROUP BY 招标单 HAVING COUNT(DISTINCT 邀请的供应商) < 3
     */
    private void supplementGroupByFields(ChatQueryContext chatQueryContext, SemanticParseInfo semanticParseInfo) {
        String sql = semanticParseInfo.getSqlInfo().getCorrectedS2SQL();
        Select select = SqlSelectHelper.getSelect(sql);
        if (select instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) select;
            GroupByElement groupBy = plainSelect.getGroupBy();
            if (groupBy != null) {
                ExpressionList groupByExpressionList = groupBy.getGroupByExpressionList();
                Set<String> existsColumnNames = (Set<String>) groupByExpressionList.stream()
                        .filter(v -> v instanceof Column).map(v -> ((Column) v).getColumnName()).collect(Collectors.toSet());
                List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
                for (SelectItem<?> selectItem : selectItems) {
                    if (selectItem.getExpression() instanceof Column) {
                        Column column = (Column) selectItem.getExpression();
                        if (!existsColumnNames.contains(column.getColumnName())) {
                            groupByExpressionList.add(column);
                        }
                    }
                }
            }
            semanticParseInfo.getSqlInfo().setCorrectedS2SQL(plainSelect.toString());
        }
    }
}
