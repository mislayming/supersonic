package com.tencent.supersonic.headless.core.translator.optimizer;

import com.tencent.supersonic.headless.core.pojo.QueryStatement;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component("MDVResultLimitOptimizer")
public class MDVResultLimitOptimizer extends ResultLimitOptimizer {

    @Override
    public boolean accept(QueryStatement queryStatement) {
        // 调用 SqlSelectHelper.hasLimit(queryStatement.getSql())解析SQL，假如存在Calcite优化后的BETWEEN ASYMMETRIC，jSQLParser解析会报错。
        // 一些场景下，会生成这种方式: FETCH FIRST 1 ROW ONLY，导致多拼接了LIMIT，SQL报错。
        String sql = queryStatement.getSql().toUpperCase();
        return !sql.contains("LIMIT") && !sql.contains("ROW ONLY");
    }
}
