package com.tencent.supersonic.headless.core.adaptor.db;

import com.tencent.supersonic.headless.core.pojo.QueryStatement;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HanadbAdaptor extends DefaultDbAdaptor {

    @Override
    public String rewriteSql(QueryStatement qstate) {
        return qstate.getSql().replaceAll("`(.*?)`", "\"$1\"").replaceAll("\"([A-Z0-9_]+?)\"",
                "$1");
    }

}
