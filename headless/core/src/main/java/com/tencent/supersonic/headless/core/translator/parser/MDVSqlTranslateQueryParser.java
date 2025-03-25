// package com.tencent.supersonic.headless.core.translator.parser;
//
// import cn.hutool.core.util.StrUtil;
// import com.meicloud.mdv.supersonic.utils.SuperSonicSQLUtils;
// import com.tencent.supersonic.headless.core.pojo.QueryStatement;
// import lombok.SneakyThrows;
// import lombok.extern.slf4j.Slf4j;
// import net.sf.jsqlparser.parser.CCJSqlParser;
// import net.sf.jsqlparser.parser.CCJSqlParserUtil;
// import net.sf.jsqlparser.statement.Statement;
// import org.apache.commons.lang3.RegExUtils;
// import org.apache.commons.lang3.StringUtils;
// import org.springframework.stereotype.Component;
// import org.springframework.util.StopWatch;
//
// import java.util.HashMap;
// import java.util.Map;
// import java.util.stream.Collectors;
//
// @Slf4j
// @Component("MDVSqlTranslateQueryParser")
// public class MDVSqlTranslateQueryParser implements QueryParser {
//
// public static final String MDV_TABLE_NAME_FORMATE = "__mdv_table_%s__";
// public static final String MDV_TABLE_NAME_PATTERN = String.format(MDV_TABLE_NAME_FORMATE,
// "(?<schemaName>(INTERFACE_SCHEMA_.{32}|SCHEMA_.{32}))");
//
// @Override
// public boolean accept(QueryStatement queryStatement) {
// return StringUtils.isNotBlank(queryStatement.getSql()) && queryStatement.getSqlQuery() != null;
// }
//
// @Override
// public void parse(QueryStatement queryStatement) throws Exception {
// queryStatement.setSql(translatePhysicsSql(queryStatement.getSql()));
// queryStatement.getSqlQuery().setSql(correctSQL(queryStatement.getSqlQuery().getSql()));
// }
//
// /***
// * 转换成可执行的物理SQL
// * 说明: 表t1、t2处于同一个数据集，但是不一定是同一个数据库。（t1、t2存储的是schemaName）
// * 原有SQL:
// * (select t1.c1,t2.c2 from t1 left join t2 on t1.key = t2.key)
// * 替换表SQL:
// * t1 => (select xxx from calciteSchemaXXX.real_t1) as t1
// * t2 => (select xxx from calciteSchemaYYY.real_t2) as t2
// * 最终SQL:
// * (select t1.c1,t2.c2 from (select xxx from calciteSchemaXXX.real_t1) as t1 left join (select xxx
// from calciteSchemaYYY.real_t2) as t2 on t1.key = t2.key)
// *
// */
// @SneakyThrows
// public static String translatePhysicsSql(String sql) {
//
// sql = correctSQL(sql);
//
// // 模型信息: key=__mdv_table_SCHEMA_1da81b1aeba440259ed44dad2bbcaa0c__
// // value=SCHEMA_1da81b1aeba440259ed44dad2bbcaa0c
// Map<String, String> modelInfo = RegExUtils.findAll(MDV_TABLE_NAME_PATTERN, sql, 0).stream()
// .collect(Collectors.toMap(v -> v, v -> getSchemaName(v), (o, n) -> n));
// Map<String, String> modelSqlInfo = new HashMap<>();
// // 获取模型的coreSQL，coreSQL的真实表名需要追加数据库的schema信息，为了在Calcite中能够正常执行。
// for (Map.Entry<String, String> entry : modelInfo.entrySet()) {
// String schemaName = entry.getValue();
// SysSchemaFileBean schemaBeanInfo =
// SpringUtil.getBean(SchemaFileService.class).getSchemaBeanInfo(schemaName);
// if (schemaBeanInfo == null) {
// continue;
// }
// EResourceType resourceType = schemaBeanInfo.getType();
// SchemaBean schemaBean = null;
// if (EResourceType.INTERFACE_SCHEMA.equals(schemaBeanInfo.getType())) {
// InterfaceSchemaBean interfaceSchemaBean = schemaBeanInfo.unwrap(InterfaceSchemaBean.class);
// schemaBean = interfaceSchemaBean.unwrap(SchemaBean.class);
// schemaName = schemaBeanInfo.getName();
// } else {
// schemaBean = schemaBeanInfo.unwrap(SchemaBean.class);
// }
// DBType dbType = null;
// if(EResourceType.SCHEMA_V2.equals(schemaBeanInfo.getType()) ||
// EResourceType.INTERFACE_SCHEMA.equals(schemaBeanInfo.getType())) {
// dbType = DBType.MYSQL;
// } else {
// String dataSourceId = schemaBeanInfo.getDataSourceId();
// if (StrUtil.isBlank(dataSourceId)) {
// throw new NormalException("数据源ID不存在");
// }
// com.meicloud.mdv.model.DataSource dataSource =
// SpringUtil.getBean(DataSourceService.class).get(dataSourceId);
// if (dataSource == null) {
// throw new NormalException("数据源ID不存在");
// }
// // 直连数据库，需要获取数据库类型。
// dbType = dataSource.getDbType();
// }
// String sqlQuery = getSqlQuery(schemaName, schemaBean, resourceType, dbType);
// log.info("REPLACE TABLE [{}] WITH SQL [{}]", entry.getKey(), sqlQuery);
// sqlQuery = SuperSonicSQLUtils.correctSQL(sqlQuery);
// if (!EResourceType.SCHEMA_V2.equals(schemaBeanInfo.getType()) &&
// !EResourceType.INTERFACE_SCHEMA.equals(schemaBeanInfo.getType())) {
// if (Boolean.TRUE.equals(schemaBeanInfo.getIsSnapshot())) {
// // 迁移回来的快照数据，需要特殊兼容。
// sqlQuery = CalciteUtils.appendDataSourceSchema(ConstString.SNAPSHOT_DATASOURCE, sqlQuery);
// } else {
// sqlQuery = CalciteUtils.appendDataSourceSchema(schemaBeanInfo.getDataSourceId(),
// schemaBean.getCubeBean().getServerName(), sqlQuery);
// }
// }
// sqlQuery = wrapCoreSQL(sqlQuery, schemaBean.getCubeBean(), schemaName);
// modelSqlInfo.put(entry.getKey(), sqlQuery);
// }
// // 将SQL的表名占位符，替换成对应的Schema的coreSQL。
// for (Map.Entry<String, String> entry : modelSqlInfo.entrySet()) {
// // 生成的表名，可能存在带和不带`转义符两种情况
// sql = sql.replaceAll(String.format("`%s`|%s", entry.getKey(), entry.getKey()),
// String.format("(%s) as %s", entry.getValue(), entry.getKey()));
// }
// // 确保执行的Schema都在Calcite中。
// SuperSonicDataSourceUtils.ensureConnectionExistsDataSourceKeys(sql);
// log.info("PHYSICS SQL [{}]", sql.replaceAll("[\\r\\n]+", " "));
// stopWatch.stop();
// log.info("LOGIC SQL TRANSLATE PHYSICS SQL TIME:[{}]ms",
// stopWatch.lastTaskInfo().getTimeMillis());
// return sql;
// }
//
// /***
// * 修正成Calcite支持的SQL语法
// * 1.移除SQL语句，where条件占位符。
// * 2.包装关键词。如YEAR、MONTH包装成`YEAR`、`MONTH`。
// * 规则: 1、变量 username = '${varName}'
// * 2、<username> username = '${varName}'</username>
// */
// public static String correctSQL(String sql) {
// // 移除变量:XML变量:<varName>xxx</varName>
// if (existsXmlPlaceholderVariable(sql)) {
// sql = RegExUtils.replaceAll(sql, "<(\\w+)[^>]*>.*?</\\1>", "");
// }
// try {
// CCJSqlParser parser = CCJSqlParserUtil.newParser(sql);
// Statement stmt = parser.Statement();
// parser.getASTRoot().jjtAccept(new SQLNodeVisitor(), null);
// return stmt.toString();
// } catch (Exception e) {
// log.error("解析SQL异常", e);
// }
// return sql;
// }
//
// public static boolean existsXmlPlaceholderVariable(String sql) {
// return sql.contains("<") && sql.contains(">");
// }
// }
