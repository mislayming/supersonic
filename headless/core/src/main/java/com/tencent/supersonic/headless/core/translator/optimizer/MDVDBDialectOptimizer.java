package com.tencent.supersonic.headless.core.translator.optimizer;

import com.tencent.supersonic.headless.core.pojo.QueryStatement;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/***
 * 方言优化，Calcite优化后的SQL，存在between 转成 between asymmetric，尝试通过调整Calcite配置设置，没成功。
 */
@Slf4j
@Component("MDVDBDialectOptimizer")
public class MDVDBDialectOptimizer implements QueryOptimizer {

    @Override
    public boolean accept(QueryStatement queryStatement) {
        return true;
    }

    @Override
    public void rewrite(QueryStatement queryStatement) {
        queryStatement.setSql(handleDialect(queryStatement.getSql()));
        queryStatement.getSqlQuery().setSql(handleDialect(queryStatement.getSqlQuery().getSql()));
    }

    /***
     * Calcite优化之后，范围计算，会生成`BETWEEN ASYMMETRIC`，需要进行替换处理，否则数据库执行报错。
     */
    public static String handleDialect(String sql) {
        return sql.replaceAll("(?i)between asymmetric", "between");
    }
}
