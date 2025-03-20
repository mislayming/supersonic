package com.tencent.supersonic.headless.core.adaptor.db;

import com.tencent.supersonic.headless.core.pojo.QueryStatement;

public class DefaultDbAdaptor extends BaseDbAdaptor {

    @Override
    public String getDateFormat(String dateType, String dateFormat, String column) {
        return column;
    }

    @Override
    public String rewriteSql(QueryStatement statement) {
        return statement.getSql();
    }
}
