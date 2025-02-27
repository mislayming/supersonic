package com.tencent.supersonic.headless.core.translator.parser.calcite.render;

import com.google.common.collect.Sets;
import com.tencent.supersonic.common.pojo.enums.EngineType;
import com.tencent.supersonic.headless.core.pojo.JoinRelation;
import com.tencent.supersonic.headless.core.pojo.OntologyQuery;
import com.tencent.supersonic.headless.core.translator.parser.calcite.S2CalciteSchema;
import com.tencent.supersonic.headless.core.translator.parser.calcite.TableView;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.AggFunctionNode;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.DataModelNode;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.FilterNode;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.IdentifyNode;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.MetricNode;
import com.tencent.supersonic.headless.core.translator.parser.calcite.node.SemanticNode;
import com.tencent.supersonic.headless.core.translator.parser.s2sql.*;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 处理多数据源join的渲染器
 * 主要职责是构建和处理多表join的SQL语句
 */
@Slf4j
public class JoinRender extends Renderer {


	@Override
	public void render(OntologyQuery metricCommand, List<DataModel> dataModels,
	                   SqlValidatorScope scope, S2CalciteSchema schema, boolean nonAgg) throws Exception {

		// 1. 处理where条件中的字段
		String queryWhere = metricCommand.getWhere();
		EngineType engineType = schema.getOntology().getDatabaseType();
		Set<String> whereFields = new HashSet<>();
		List<String> fieldWhere = new ArrayList<>();
		if (queryWhere != null && !queryWhere.isEmpty()) {
			SqlNode sqlNode = SemanticNode.parse(queryWhere, scope, engineType);
			FilterNode.getFilterField(sqlNode, whereFields);
			fieldWhere = whereFields.stream().collect(Collectors.toList());
		}

		// 2. 获取查询涉及的所有维度和度量
		Set<String> queryAllDimension = new HashSet<>();
		Set<String> measures = new HashSet<>();
		DataModelNode.getQueryDimensionMeasure(schema.getOntology(), metricCommand,
				queryAllDimension, measures);

		// 3. 初始化视图
		TableView innerView = new TableView();
		TableView filterView = new TableView();
		Map<String, SqlNode> innerSelect = new HashMap<>();
		Set<String> filterDimension = new HashSet<>();

		// 4. 构建join关系图
		JoinGraph joinGraph = buildJoinGraph(dataModels, schema);
		System.out.println("------ relation");
		System.out.println(joinGraph.toString());


		// 5. 构建join树
		SqlNode joinTree = buildJoinTree(
				joinGraph,
				dataModels,
				fieldWhere,
				measures,  // queryMetrics
				queryAllDimension,  // queryDimension
				queryWhere,
				scope,
				schema
		);

		// 6. 收集所有需要的字段
		Set<String> finalFields = new HashSet<>();  // 最终需要的字段
		Map<String, String> fieldToSourceMap = new HashMap<>();  // 字段到源表的映射

		// 6.1 处理维度字段
		for (String dimension : metricCommand.getDimensions()) {
			for (DataModel model : dataModels) {
				if (isDimensionBelongToModel(dimension, model)) {
					String tableAlias = Constants.JOIN_TABLE_PREFIX + model.getName();
					fieldToSourceMap.put(dimension, tableAlias);
					finalFields.add(dimension);
					break;
				}
			}
		}

		// 6.2 处理度量字段
		for (String metric : metricCommand.getMetrics()) {
			for (DataModel model : dataModels) {
				if (isMetricBelongToModel(metric, model)) {
					String tableAlias = Constants.JOIN_TABLE_PREFIX + model.getName();
					fieldToSourceMap.put(metric, tableAlias);
					finalFields.add(metric);
					break;
				}
			}
		}

		// 6.3 处理where条件中的字段
		for (String field : whereFields) {
			if (!fieldToSourceMap.containsKey(field)) {
				for (DataModel model : dataModels) {
					if (isFieldBelongToModel(field, model)) {
						String tableAlias = Constants.JOIN_TABLE_PREFIX + model.getName();
						fieldToSourceMap.put(field, tableAlias);
						finalFields.add(field);
						break;
					}
				}
			}
		}

		// 6.4 处理join条件中的字段
		for (Map.Entry<String, JoinRelation> entry : joinGraph.getJoinConditions().entrySet()) {
			JoinRelation joinRelation = entry.getValue();
			if (joinRelation != null && !CollectionUtils.isEmpty(joinRelation.getJoinCondition())) {
				// 处理预定义join关系中的字段
				for (Triple<String, String, String> triple : joinRelation.getJoinCondition()) {
					String leftTable = joinRelation.getLeft();
					String rightTable = joinRelation.getRight();

					// 添加左表字段
					for (DataModel model : dataModels) {
						if (model.getName().equals(leftTable) && isFieldBelongToModel(triple.getLeft(), model)) {
							String tableAlias = Constants.JOIN_TABLE_PREFIX + leftTable;
							fieldToSourceMap.put(triple.getLeft(), tableAlias);
							finalFields.add(triple.getLeft());
							break;
						}
					}

					// 添加右表字段
					for (DataModel model : dataModels) {
						if (model.getName().equals(rightTable) && isFieldBelongToModel(triple.getRight(), model)) {
							String tableAlias = Constants.JOIN_TABLE_PREFIX + rightTable;
							fieldToSourceMap.put(triple.getRight(), tableAlias);
							finalFields.add(triple.getRight());
							break;
						}
					}
				}
			}
		}

		// 处理没有预定义join关系的标识符字段
		for (DataModel model : dataModels) {
			for (Identify identify : model.getIdentifiers()) {
				String field = identify.getName();
				if (!finalFields.contains(field)) {
					String tableAlias = Constants.JOIN_TABLE_PREFIX + model.getName();
					fieldToSourceMap.put(field, tableAlias);
					finalFields.add(field);
				}
			}
		}

		// 6.5 处理zipper join的时间字段
		for (DataModel model : dataModels) {
			if (Materialization.TimePartType.ZIPPER.equals(model.getTimePartType())) {
				for (Dimension dimension : model.getDimensions()) {
					if (Constants.DIMENSION_TYPE_TIME.equalsIgnoreCase(dimension.getType())) {
						if (dimension.getName().startsWith(Constants.MATERIALIZATION_ZIPPER_START)
								|| dimension.getName().startsWith(Constants.MATERIALIZATION_ZIPPER_END)) {
							String tableAlias = Constants.JOIN_TABLE_PREFIX + model.getName();
							fieldToSourceMap.put(dimension.getName(), tableAlias);
							finalFields.add(dimension.getName());
						}
					}
				}
			}
		}

		// 7. 构建最终的查询视图
		// 7.1 构建内部视图 - 只选择需要的字段，并正确处理别名
		Set<String> selectFields = new LinkedHashSet<>(finalFields);  // 使用LinkedHashSet保持顺序

		// 确保join条件中使用的字段被包含在查询中
		for (DataModel model : dataModels) {
			for (Identify identify : model.getIdentifiers()) {
				String field = identify.getName();
				String tableAlias = Constants.JOIN_TABLE_PREFIX + model.getName();
				// 如果这个字段被用作join条件，确保它被包含在查询中，但不重复添加
				if (isFieldBelongToModel(field, model) && !selectFields.contains(field)) {
					selectFields.add(field);
					if (!fieldToSourceMap.containsKey(field)) {
						fieldToSourceMap.put(field, tableAlias);
					}
				}
			}
		}

		// 构建查询字段，使用Set避免重复
		Set<String> addedFields = new HashSet<>();
		for (String field : selectFields) {
			String sourceTable = fieldToSourceMap.get(field);
			if (sourceTable != null && !addedFields.contains(field)) {
				String qualifiedField = sourceTable + "." + field;
				SqlNode fieldNode = SemanticNode.parse(qualifiedField, scope, engineType);
				innerView.getMeasure().add(SemanticNode.buildAs(field, fieldNode));
				addedFields.add(field);
			}
		}
		innerView.setTable(joinTree);

		// 7.2 构建过滤视图
		filterView.setTable(SemanticNode.buildAs(Constants.JOIN_TABLE_OUT_PREFIX, innerView.build()));

		// 7.3 根据查询类型（聚合/非聚合）添加字段
		addedFields.clear();  // 重置已添加字段集合
		for (String field : finalFields) {
			if (!addedFields.contains(field)) {
				SqlNode fieldNode = SemanticNode.parse(field, scope, engineType);
				if (nonAgg) {
					filterView.getMeasure().add(fieldNode);
				} else {
					if (metricCommand.getDimensions().contains(field)) {
						filterView.getDimension().add(fieldNode);
					} else if (metricCommand.getMetrics().contains(field)) {
						filterView.getMeasure().add(fieldNode);
					}
				}
				addedFields.add(field);
			}
		}

		super.tableView = filterView;
	}

	/**
	 * 处理度量字段
	 *
	 * @param innerSelect   内部select字段映射
	 * @param filterView    过滤视图
	 * @param queryMetrics  查询的度量集合
	 * @param reqMetrics    请求的度量列表
	 * @param dataModel     数据模型
	 * @param sourceMeasure 源表度量集合
	 * @param scope         SQL验证范围
	 * @param schema        计算模式
	 * @param nonAgg        是否非聚合
	 */
	private void doMetric(Map<String, SqlNode> innerSelect, TableView filterView,
	                      Set<String> queryMetrics, List<String> reqMetrics, DataModel dataModel,
	                      Set<String> sourceMeasure, SqlValidatorScope scope, S2CalciteSchema schema,
	                      boolean nonAgg) throws Exception {
		String alias = Constants.JOIN_TABLE_PREFIX + dataModel.getName();
		EngineType engineType = schema.getOntology().getDatabaseType();
		for (String m : reqMetrics) {
			// 检查度量是否匹配当前数据模型
			if (getMatchMetric(schema, sourceMeasure, m, queryMetrics)) {
				// 构建度量节点
				MetricNode metricNode = buildMetricNode(m, dataModel, scope, schema, nonAgg, alias);

				// 处理非聚合节点
				if (!metricNode.getNonAggNode().isEmpty()) {
					for (String measure : metricNode.getNonAggNode().keySet()) {
						innerSelect.put(measure, SemanticNode.buildAs(measure,
								SemanticNode.parse(alias + "." + measure, scope, engineType)));
					}
				}
				// 处理聚合函数
				if (metricNode.getAggFunction() != null && !metricNode.getAggFunction().isEmpty()) {
					for (Map.Entry<String, String> entry : metricNode.getAggFunction().entrySet()) {
						if (metricNode.getNonAggNode().containsKey(entry.getKey())) {
							if (nonAgg) {
								filterView.getMeasure().add(SemanticNode.buildAs(entry.getKey(),
										SemanticNode.parse(entry.getKey(), scope, engineType)));
							} else {
								filterView.getMeasure()
										.add(SemanticNode.buildAs(entry.getKey(),
												AggFunctionNode.build(entry.getValue(),
														entry.getKey(), scope, engineType)));
							}
						}
					}
				}
			}
		}
	}

	/**
	 * 处理维度字段
	 *
	 * @param innerSelect     内部select字段映射
	 * @param filterDimension 过滤维度集合
	 * @param queryDimension  查询维度集合
	 * @param reqDimensions   请求的维度列表
	 * @param dataModel       数据模型
	 * @param dimension       维度集合
	 * @param scope           SQL验证范围
	 * @param schema          计算模式
	 */
	private void doDimension(Map<String, SqlNode> innerSelect, Set<String> filterDimension,
	                         Set<String> queryDimension, List<String> reqDimensions, DataModel dataModel,
	                         Set<String> dimension, SqlValidatorScope scope, S2CalciteSchema schema)
			throws Exception {
		String alias = Constants.JOIN_TABLE_PREFIX + dataModel.getName();
		EngineType engineType = schema.getOntology().getDatabaseType();
		for (String d : reqDimensions) {
			// 检查维度是否匹配当前数据模型
			if (getMatchDimension(schema, dimension, dataModel, d, queryDimension)) {
				// 处理标识符维度
				if (d.contains(Constants.DIMENSION_IDENTIFY)) {
					String[] identifyDimension = d.split(Constants.DIMENSION_IDENTIFY);
					innerSelect.put(d, SemanticNode.buildAs(d, SemanticNode
							.parse(alias + "." + identifyDimension[1], scope, engineType)));
				} else {
					innerSelect.put(d, SemanticNode.buildAs(d,
							SemanticNode.parse(alias + "." + d, scope, engineType)));
				}
				filterDimension.add(d);
			}
		}
	}

	/**
	 * 获取查询维度
	 * 从过滤维度中筛选出在查询维度或where条件字段中的维度
	 */
	private Set<String> getQueryDimension(Set<String> filterDimension,
	                                      Set<String> queryAllDimension, Set<String> whereFields) {
		return filterDimension.stream()
				.filter(d -> queryAllDimension.contains(d) || whereFields.contains(d))
				.collect(Collectors.toSet());
	}

	/**
	 * 检查度量是否匹配
	 * 判断度量是否存在于源表或是否为复合度量
	 */
	private boolean getMatchMetric(S2CalciteSchema schema, Set<String> sourceMeasure, String m,
	                               Set<String> queryMetrics) {
		Optional<Metric> metric = schema.getMetrics().stream()
				.filter(mm -> mm.getName().equalsIgnoreCase(m)).findFirst();
		boolean isAdd = false;
		// 检查复合度量
		if (metric.isPresent()) {
			Set<String> metricMeasures = metric.get().getMetricTypeParams().getMeasures().stream()
					.map(me -> me.getName()).collect(Collectors.toSet());
			if (sourceMeasure.containsAll(metricMeasures)) {
				isAdd = true;
			}
		}
		// 检查简单度量
		if (sourceMeasure.contains(m)) {
			isAdd = true;
		}
		if (isAdd && !queryMetrics.contains(m)) {
			queryMetrics.add(m);
		}
		return isAdd;
	}

	/**
	 * 检查维度是否匹配
	 * 判断维度是否存在于源表维度、标识符或schema定义的维度中
	 */
	private boolean getMatchDimension(S2CalciteSchema schema, Set<String> sourceDimension,
	                                  DataModel dataModel, String d, Set<String> queryDimension) {
		String oriDimension = d;
		boolean isAdd = false;
		// 处理标识符维度
		if (d.contains(Constants.DIMENSION_IDENTIFY)) {
			oriDimension = d.split(Constants.DIMENSION_IDENTIFY)[1];
		}
		// 检查源表维度
		if (sourceDimension.contains(oriDimension)) {
			isAdd = true;
		}
		// 检查标识符
		for (Identify identify : dataModel.getIdentifiers()) {
			if (identify.getName().equalsIgnoreCase(oriDimension)) {
				isAdd = true;
				break;
			}
		}
		// 检查schema定义的维度
		if (schema.getDimensions().containsKey(dataModel.getName())) {
			for (Dimension dim : schema.getDimensions().get(dataModel.getName())) {
				if (dim.getName().equalsIgnoreCase(oriDimension)) {
					isAdd = true;
				}
			}
		}
		if (isAdd && !queryDimension.contains(oriDimension)) {
			queryDimension.add(oriDimension);
		}
		return isAdd;
	}

	/**
	 * 获取表对象
	 */
	private SqlNode getTable(TableView tableView, SqlValidatorScope scope) throws Exception {
		return SemanticNode.getTable(tableView.getTable());
	}

	/**
	 * 构建Join节点
	 *
	 * @param left      左表节点
	 * @param leftTable 左表视图
	 * @param tableView 右表视图
	 * @param before    已处理的表别名映射
	 * @param dataModel 数据模型
	 * @param schema    计算模式
	 * @param scope     SQL验证范围
	 */
	private SqlNode buildJoin(SqlNode left, TableView leftTable, TableView tableView,
	                          Map<String, String> before, DataModel dataModel, S2CalciteSchema schema,
	                          SqlValidatorScope scope) throws Exception {
		EngineType engineType = schema.getOntology().getDatabaseType();

		// 获取基本join条件
		SqlNode condition = getCondition(leftTable, tableView, dataModel, schema, scope, engineType);

		// 获取join类型
		SqlLiteral sqlLiteral = SemanticNode.getJoinSqlLiteral("");

		// 检查预定义join关系
		JoinRelation matchJoinRelation = getMatchJoinRelation(before, tableView, schema);
		SqlNode joinRelationCondition = null;
		if (!CollectionUtils.isEmpty(matchJoinRelation.getJoinCondition())) {
			sqlLiteral = SemanticNode.getJoinSqlLiteral(matchJoinRelation.getJoinType());
			joinRelationCondition = getCondition(matchJoinRelation, scope, engineType);
			condition = joinRelationCondition;
		}

		// 处理zipper join
		if (Materialization.TimePartType.ZIPPER.equals(leftTable.getDataModel().getTimePartType())
				|| Materialization.TimePartType.ZIPPER
				.equals(tableView.getDataModel().getTimePartType())) {
			SqlNode zipperCondition = getZipperCondition(leftTable, tableView, dataModel, schema, scope);
			if (Objects.nonNull(joinRelationCondition)) {
				condition = new SqlBasicCall(SqlStdOperatorTable.AND,
						new ArrayList<>(Arrays.asList(zipperCondition, joinRelationCondition)),
						SqlParserPos.ZERO, null);
			} else {
				condition = zipperCondition;
			}
		}

		// 构建join节点
		return new SqlJoin(SqlParserPos.ZERO, left,
				SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
				sqlLiteral,
				SemanticNode.buildAs(tableView.getAlias(), getTable(tableView, scope)),
				SqlLiteral.createSymbol(JoinConditionType.ON, SqlParserPos.ZERO),
				condition);
	}

	/**
	 * 获取匹配的Join关系
	 * 从schema中查找匹配的预定义join关系
	 */
	private JoinRelation getMatchJoinRelation(Map<String, String> before, TableView tableView,
	                                          S2CalciteSchema schema) {
		JoinRelation matchJoinRelation = JoinRelation.builder().build();
		if (!CollectionUtils.isEmpty(schema.getJoinRelations())) {
			for (JoinRelation joinRelation : schema.getJoinRelations()) {
				// 检查正向join关系
				if (joinRelation.getRight().equalsIgnoreCase(tableView.getDataModel().getName())
						&& before.containsKey(joinRelation.getLeft())) {
					matchJoinRelation.setJoinCondition(joinRelation.getJoinCondition().stream()
							.map(r -> Triple.of(
									before.get(joinRelation.getLeft()) + "." + r.getLeft(),
									r.getMiddle(), tableView.getAlias() + "." + r.getRight()))
							.collect(Collectors.toList()));
					matchJoinRelation.setJoinType(joinRelation.getJoinType());
				}
				// 检查反向join关系
				else if (joinRelation.getLeft()
						.equalsIgnoreCase(tableView.getDataModel().getName())
						&& before.containsKey(joinRelation.getRight())) {
					matchJoinRelation.setJoinCondition(joinRelation.getJoinCondition().stream()
							.map(r -> Triple.of(
									before.get(joinRelation.getRight()) + "." + r.getRight(),
									r.getMiddle(), tableView.getAlias() + "." + r.getLeft()))
							.collect(Collectors.toList()));
					matchJoinRelation.setJoinType(joinRelation.getJoinType());
				}
			}
		}
		return matchJoinRelation;
	}

	/**
	 * 添加zipper字段
	 * 为zipper类型的表添加时间范围字段
	 */
	private void addZipperField(DataModel dataModel, List<String> fields) {
		if (Materialization.TimePartType.ZIPPER.equals(dataModel.getTimePartType())) {
			dataModel.getDimensions().stream()
					.filter(d -> Constants.DIMENSION_TYPE_TIME.equalsIgnoreCase(d.getType()))
					.forEach(t -> {
						if (t.getName().startsWith(Constants.MATERIALIZATION_ZIPPER_END)
								&& !fields.contains(t.getName())) {
							fields.add(t.getName());
						}
						if (t.getName().startsWith(Constants.MATERIALIZATION_ZIPPER_START)
								&& !fields.contains(t.getName())) {
							fields.add(t.getName());
						}
					});
		}
	}

	/**
	 * 获取zipper join条件
	 * 构建时间范围的join条件
	 */
	private SqlNode getZipperCondition(TableView left, TableView right, DataModel dataModel,
	                                   S2CalciteSchema schema, SqlValidatorScope scope) throws Exception {
		// 检查是否同时为zipper类型
		if (Materialization.TimePartType.ZIPPER.equals(left.getDataModel().getTimePartType())
				&& Materialization.TimePartType.ZIPPER
				.equals(right.getDataModel().getTimePartType())) {
			throw new Exception("not support two zipper table");
		}

		SqlNode condition = null;
		// 获取时间维度
		Optional<Dimension> leftTime = left.getDataModel().getDimensions().stream()
				.filter(d -> Constants.DIMENSION_TYPE_TIME.equalsIgnoreCase(d.getType()))
				.findFirst();
		Optional<Dimension> rightTime = right.getDataModel().getDimensions().stream()
				.filter(d -> Constants.DIMENSION_TYPE_TIME.equalsIgnoreCase(d.getType()))
				.findFirst();

		if (leftTime.isPresent() && rightTime.isPresent()) {
			String startTime = "";
			String endTime = "";
			String dateTime = "";

			// 获取zipper表的时间范围字段
			Optional<Dimension> startTimeOp = (Materialization.TimePartType.ZIPPER
					.equals(left.getDataModel().getTimePartType()) ? left : right).getDataModel()
					.getDimensions().stream()
					.filter(d -> Constants.DIMENSION_TYPE_TIME
							.equalsIgnoreCase(d.getType()))
					.filter(d -> d.getName()
							.startsWith(Constants.MATERIALIZATION_ZIPPER_START))
					.findFirst();
			Optional<Dimension> endTimeOp = (Materialization.TimePartType.ZIPPER
					.equals(left.getDataModel().getTimePartType()) ? left : right).getDataModel()
					.getDimensions().stream()
					.filter(d -> Constants.DIMENSION_TYPE_TIME
							.equalsIgnoreCase(d.getType()))
					.filter(d -> d.getName()
							.startsWith(Constants.MATERIALIZATION_ZIPPER_END))
					.findFirst();

			if (startTimeOp.isPresent() && endTimeOp.isPresent()) {
				// 确定zipper表和普通表
				TableView zipper = Materialization.TimePartType.ZIPPER
						.equals(left.getDataModel().getTimePartType()) ? left : right;
				TableView partMetric = Materialization.TimePartType.ZIPPER
						.equals(left.getDataModel().getTimePartType()) ? right : left;
				Optional<Dimension> partTime = Materialization.TimePartType.ZIPPER
						.equals(left.getDataModel().getTimePartType()) ? rightTime : leftTime;

				// 构建字段引用
				startTime = zipper.getAlias() + "." + startTimeOp.get().getName();
				endTime = zipper.getAlias() + "." + endTimeOp.get().getName();
				dateTime = partMetric.getAlias() + "." + partTime.get().getName();
			}

			EngineType engineType = schema.getOntology().getDatabaseType();

			// 构建条件: startTime <= dateTime AND dateTime < endTime
			ArrayList<SqlNode> operandList =
					new ArrayList<>(Arrays.asList(SemanticNode.parse(endTime, scope, engineType),
							SemanticNode.parse(dateTime, scope, engineType)));
			condition = new SqlBasicCall(SqlStdOperatorTable.AND,
					new ArrayList<SqlNode>(Arrays.asList(
							new SqlBasicCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
									new ArrayList<SqlNode>(Arrays.asList(
											SemanticNode.parse(startTime, scope, engineType),
											SemanticNode.parse(dateTime, scope, engineType))),
									SqlParserPos.ZERO, null),
							new SqlBasicCall(SqlStdOperatorTable.GREATER_THAN, operandList,
									SqlParserPos.ZERO, null))),
					SqlParserPos.ZERO, null);
		}
		return condition;
	}

	/**
	 * 获取基本的join条件
	 */
	private SqlNode getCondition(TableView left, TableView right, DataModel dataModel,
	                         S2CalciteSchema schema, SqlValidatorScope scope, EngineType engineType)
			throws Exception {

		SqlNode condition = null;
		Set<String> commonFields = new HashSet<>();

		// 1. 首先尝试从标识符中找join字段
		for (Identify leftId : left.getDataModel().getIdentifiers()) {
			for (Identify rightId : right.getDataModel().getIdentifiers()) {
				if (leftId.getName().equals(rightId.getName())) {
					commonFields.add(leftId.getName());
				}
			}
		}

		// 2. 如果没有找到标识符，尝试从所有字段中找共同字段
		if (commonFields.isEmpty()) {
			// 获取左表所有字段
			Set<String> leftFields = new HashSet<>();
			leftFields.addAll(left.getDataModel().getDimensions().stream()
					.map(Dimension::getName)
					.collect(Collectors.toSet()));

			leftFields.addAll(left.getDataModel().getMeasures().stream()
					.map(Measure::getName)
					.collect(Collectors.toSet()));

			// 获取右表所有字段
			Set<String> rightFields = new HashSet<>();
			rightFields.addAll(right.getDataModel().getDimensions().stream()
					.map(Dimension::getName)
					.collect(Collectors.toSet()));
			rightFields.addAll(right.getDataModel().getMeasures().stream()
					.map(Measure::getName)
					.collect(Collectors.toSet()));

			// 找出共同字段
			for (String field : leftFields) {
				if (rightFields.contains(field)) {
					commonFields.add(field);
				}
			}
		}

		// 3. 构建join条件
		for (String field : commonFields) {
			// 构建带表别名的字段引用
			String leftField = left.getAlias() + "." + field;
			String rightField = right.getAlias() + "." + field;

			List<SqlNode> ons = new ArrayList<>();
			ons.add(SemanticNode.parse(leftField, scope, engineType));
			ons.add(SemanticNode.parse(rightField, scope, engineType));

			SqlNode joinCondition = new SqlBasicCall(SqlStdOperatorTable.EQUALS, ons, SqlParserPos.ZERO, null);

			// 添加到现有条件中
			if (condition == null) {
				condition = joinCondition;
			} else {
				condition = new SqlBasicCall(SqlStdOperatorTable.AND,
						new ArrayList<>(Arrays.asList(condition, joinCondition)),
						SqlParserPos.ZERO, null);
			}
		}

		return condition;
	}

	/**
	 * 获取预定义join关系的条件
	 */
	private SqlNode getCondition(JoinRelation joinRelation, SqlValidatorScope scope,
	                         EngineType engineType) throws Exception {
		SqlNode condition = null;
		for (Triple<String, String, String> triple : joinRelation.getJoinCondition()) {
			// 使用正确的表别名
			String leftTable = joinRelation.getLeft();
			String rightTable = joinRelation.getRight();

			// 构建完整的字段引用
			String leftField = Constants.JOIN_TABLE_PREFIX + leftTable + "." + triple.getLeft();
			String rightField = Constants.JOIN_TABLE_PREFIX + rightTable + "." + triple.getRight();

			List<SqlNode> ons = new ArrayList<>();
			ons.add(SemanticNode.parse(leftField, scope, engineType));
			ons.add(SemanticNode.parse(rightField, scope, engineType));

			SqlNode joinCondition = new SqlBasicCall(
					SemanticNode.getBinaryOperator(triple.getMiddle()),
					ons,
					SqlParserPos.ZERO,
					null);

			if (condition == null) {
				condition = joinCondition;
			} else {
				condition = new SqlBasicCall(SqlStdOperatorTable.AND,
						new ArrayList<>(Arrays.asList(condition, joinCondition)),
						SqlParserPos.ZERO, null);
			}
		}
		return condition;
	}

	/**
	 * 构建join关系图
	 */
	private JoinGraph buildJoinGraph(List<DataModel> dataModels, S2CalciteSchema schema) {
		JoinGraph graph = new JoinGraph();

		// 从schema中的joinRelations构建图
		for (JoinRelation relation : schema.getOntology().getJoinRelations()) {
			// 验证join关系的有效性
			if (relation.getLeft() == null || relation.getRight() == null) {
				log.warn("Invalid join relation: left or right table is null");
				continue;
			}
			
			// 验证两个表是否都存在于dataModels中
			boolean leftExists = dataModels.stream()
				.anyMatch(m -> m.getName().equals(relation.getLeft()));
			boolean rightExists = dataModels.stream()
				.anyMatch(m -> m.getName().equals(relation.getRight()));
				
			if (!leftExists || !rightExists) {
				log.warn("Join relation references non-existent table: {} -> {}", 
					relation.getLeft(), relation.getRight());
				continue;
			}

			// 添加边
			graph.addEdge(relation.getLeft(), relation.getRight(), relation);
		}

		// 打印完整的图结构
		log.info("Built join graph:\n{}", graph);

		// 验证图的连通性
		Set<String> allTables = new HashSet<>();
		for (DataModel model : dataModels) {
			allTables.add(model.getName());
		}
		
		// 验证所有表都在图中
		for (String table : allTables) {
			if (!graph.getNeighbors(table).isEmpty()) {
				log.info("Table {} has {} neighbors", table, graph.getNeighbors(table).size());
			} else {
				log.warn("Table {} has no connections in the join graph", table);
			}
		}

		return graph;
	}

	/**
	 * 根据join图构建SQL join树
	 */
	private SqlNode buildJoinTree(JoinGraph graph, List<DataModel> dataModels,
	                          List<String> whereFields, Set<String> queryMetrics, Set<String> queryDimension,
	                          String whereCondition, SqlValidatorScope scope, S2CalciteSchema schema) throws Exception {
		
		// 1. 构建每个表的TableView
		Map<String, TableView> tableViews = new HashMap<>();
		for (DataModel model : dataModels) {
			// 收集这个表需要的所有字段
			Set<String> neededFields = new HashSet<>();

			// 1.1 添加JOIN条件中用到的字段
			for (JoinRelation relation : graph.getJoinConditions().values()) {
				if (relation.getLeft().equals(model.getName())) {
					neededFields.addAll(relation.getJoinCondition().stream()
						.map(Triple::getLeft)
						.collect(Collectors.toSet()));
				}
				if (relation.getRight().equals(model.getName())) {
					neededFields.addAll(relation.getJoinCondition().stream()
						.map(Triple::getRight)
						.collect(Collectors.toSet()));
				}
			}

			// 2. 添加查询结果中需要的字段
			neededFields.addAll(queryMetrics);
			neededFields.addAll(queryDimension);

			// 3. 添加where条件中用到的字段
			neededFields.addAll(whereFields);

			// 构建TableView时传入所有需要的字段
			TableView tableView = buildTableView(
					model,
					whereFields,
					queryMetrics,
					queryDimension,
					whereCondition,
					scope,
					schema,
					neededFields  // 传入所有需要的字段
			);

			tableViews.put(model.getName(), tableView);
		}

		// 2. 找到最优的join顺序 - 不再要求相邻表直接连接
		List<String> joinOrder = findOptimalJoinOrderRelaxed(dataModels, graph, queryMetrics, queryDimension);
		
		// 3. 按照优化后的顺序构建join树
		String firstTable = joinOrder.get(0);
		SqlNode currentNode = SemanticNode.buildAs(
				tableViews.get(firstTable).getAlias(),
				getTable(tableViews.get(firstTable), scope)
		);
		
		// 4. 逐个添加join
		for (int i = 1; i < joinOrder.size(); i++) {
			String currentTable = joinOrder.get(i);
			
			// 找到当前表与已加入表集合的最佳连接点
			String bestJoinTable = findBestJoinTable(currentTable, joinOrder.subList(0, i), graph);
			
			TableView rightView = tableViews.get(currentTable);
			TableView leftView = tableViews.get(bestJoinTable);
			
			// 获取join条件和类型
			JoinRelation joinRelation = graph.getJoinRelation(bestJoinTable, currentTable);
			SqlNode condition;
			SqlLiteral joinType;
			
			if (joinRelation != null && !CollectionUtils.isEmpty(joinRelation.getJoinCondition())) {
				// 使用预定义的join关系
				condition = getCondition(joinRelation, scope, schema.getOntology().getDatabaseType());
				joinType = SemanticNode.getJoinSqlLiteral(joinRelation.getJoinType());
			} else {
				// 如果图中没有预定义关系，使用标识符作为join条件
				condition = getCondition(leftView, rightView, rightView.getDataModel(), schema, scope,
						schema.getOntology().getDatabaseType());
				joinType = SemanticNode.getJoinSqlLiteral("");
			}
			
			// 构建join节点
			SqlNode joinNode = new SqlJoin(
				SqlParserPos.ZERO,
				currentNode,
				SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
				joinType,
				SemanticNode.buildAs(rightView.getAlias(), getTable(rightView, scope)),
				SqlLiteral.createSymbol(JoinConditionType.ON, SqlParserPos.ZERO),
				condition
			);
			
			currentNode = joinNode;
		}
		
		return currentNode;
	}

	// 找到最佳的连接表 - 与当前表有直接连接关系的已加入表
	private String findBestJoinTable(String currentTable, List<String> addedTables, JoinGraph graph) {
		for (String addedTable : addedTables) {
			if (graph.hasEdge(currentTable, addedTable)) {
				return addedTable;
			}
		}
		// 如果没有直接连接，返回最后一个添加的表
		return addedTables.get(addedTables.size() - 1);
	}

	// 放宽约束的join顺序算法
	private List<String> findOptimalJoinOrderRelaxed(List<DataModel> dataModels, JoinGraph graph,
			Set<String> queryMetrics, Set<String> queryDimension) {
		// 1. 计算每个表的权重
		Map<String, Integer> weights = new HashMap<>();
		for (DataModel model : dataModels) {
			int weight = 0;
			weight += model.getMeasures().stream()
					.filter(m -> queryMetrics.contains(m.getName()))
					.count() * 2;
			weight += model.getDimensions().stream()
					.filter(d -> queryDimension.contains(d.getName()))
					.count();
			Set<String> neighbors = graph.getNeighbors(model.getName());
			weight += (neighbors != null ? neighbors.size() : 0);
			weights.put(model.getName(), weight);
		}

		// 2. 初始化结果列表和已访问集合
		List<String> result = new ArrayList<>();
		Set<String> visited = new HashSet<>();
		
		// 3. 找出所有未访问的表中权重最高的
		String nextTable = null;
		int maxWeight = Integer.MIN_VALUE;
		
		for (DataModel model : dataModels) {
			String tableName = model.getName();
			if (!visited.contains(tableName)) {
				Integer weight = weights.get(tableName);
				if (weight != null && weight > maxWeight) {
					// 检查是否至少与一个已访问表有连接
					// 第一个表不需要检查连接
					maxWeight = weight;
					nextTable = tableName;
				}
			}
		}
		
		result.add(nextTable);
		visited.add(nextTable);

		// 4. 不再要求相邻表必须直接连接
		while (visited.size() < dataModels.size()) {
			// 找出所有未访问的表中权重最高的
			nextTable = null;  // 重置变量，不要重新声明
			maxWeight = Integer.MIN_VALUE;  // 重置变量，不要重新声明
			
			for (DataModel model : dataModels) {
				String tableName = model.getName();
				if (!visited.contains(tableName)) {
					Integer weight = weights.get(tableName);
					if (weight != null && weight > maxWeight) {
						// 检查是否至少与一个已访问表有连接
						boolean hasConnection = false;
						for (String visitedTable : visited) {
							if (graph.hasEdge(tableName, visitedTable)) {
								hasConnection = true;
								break;
							}
						}
						
						if (hasConnection) {
							maxWeight = weight;
							nextTable = tableName;
						}
					}
				}
			}
			
			// 如果没有找到连接的表，选择任意未访问表
			if (nextTable == null) {
				for (DataModel model : dataModels) {
					String tableName = model.getName();
					if (!visited.contains(tableName)) {
						nextTable = tableName;
						break;
					}
				}
			}
			
			result.add(nextTable);
			visited.add(nextTable);
		}
		
		return result;
	}

	/**
	 * 为数据模型构建TableView
	 */
	private TableView buildTableView(DataModel dataModel, List<String> whereFields,
	                                 Set<String> queryMetrics, Set<String> queryDimension, String whereCondition,
	                                 SqlValidatorScope scope, S2CalciteSchema schema,
	                                 Set<String> joinFields) throws Exception {

		// 1. 收集所有需要的字段
		Set<String> neededFields = new HashSet<>();

		// 1.1 添加where字段
		neededFields.addAll(whereFields.stream()
				.filter(field -> isFieldBelongToModel(field, dataModel))
				.collect(Collectors.toList()));

		// 1.2 添加查询度量
		neededFields.addAll(queryMetrics.stream()
				.filter(metric -> isMetricBelongToModel(metric, dataModel))
				.collect(Collectors.toSet()));

		// 1.3 添加查询维度
		neededFields.addAll(queryDimension.stream()
				.filter(dim -> isDimensionBelongToModel(dim, dataModel))
				.collect(Collectors.toSet()));

		// 1.4 添加用于join的字段
		neededFields.addAll(joinFields.stream()
				.filter(field -> isFieldBelongToModel(field, dataModel))
				.collect(Collectors.toSet()));

		// 1.5 添加主键和外键
		for (Identify identify : dataModel.getIdentifiers()) {
			neededFields.add(identify.getName());
		}

		// 2. 使用所有需要的字段构建TableView
		TableView tableView = SourceRender.renderOne(
				"",                // prefix
				new ArrayList<>(neededFields),  // 包含所有需要的字段
				Collections.emptySet(),   // metrics在neededFields中已包含
				Collections.emptySet(),   // dimensions在neededFields中已包含
				whereCondition,    // where条件
				dataModel,         // 数据模型
				scope,            // SQL验证范围
				schema,           // 计算模式
				true             // non-agg
		);

		// 3. 设置表的别名和数据模型
		String alias = Constants.JOIN_TABLE_PREFIX + dataModel.getName();
		tableView.setAlias(alias);
		tableView.setDataModel(dataModel);

		return tableView;
	}
	/**
	 * Join关系图
	 */
	@Data
	public static class JoinGraph {
		private Map<String, Set<String>> adjacencyList;
		private Map<String, JoinRelation> joinConditions;

		public JoinGraph() {
			this.adjacencyList = new HashMap<>();
			this.joinConditions = new HashMap<>();
		}

		public void addEdge(String table1, String table2, JoinRelation relation) {
			if (table1 == null || table2 == null || relation == null) {
				throw new IllegalArgumentException("Table names and relation cannot be null");
			}

			// 打印调试信息
			log.debug("Adding edge between {} and {}", table1, table2);
			if (relation.getJoinCondition() != null) {
				for (Triple<String, String, String> condition : relation.getJoinCondition()) {
					log.debug("Join condition: {}.{} {} {}.{}", 
						table1, condition.getLeft(), condition.getMiddle(), table2, condition.getRight());
				}
			}

			// 确保两个表都有对应的Set
			adjacencyList.computeIfAbsent(table1, k -> new HashSet<>());
			adjacencyList.computeIfAbsent(table2, k -> new HashSet<>());

			// 添加双向边
			adjacencyList.get(table1).add(table2);
			adjacencyList.get(table2).add(table1);

			// 存储join条件
			String key1 = table1 + "_" + table2;
			String key2 = table2 + "_" + table1;
			joinConditions.put(key1, relation);
			joinConditions.put(key2, relation);
		}

		public boolean hasEdge(String table1, String table2) {
			if (table1 == null || table2 == null) {
				return false;
			}
			
			Set<String> neighbors = adjacencyList.get(table1);
			boolean hasDirectEdge = neighbors != null && neighbors.contains(table2);
			
			// 打印调试信息
			log.debug("Checking edge between {} and {}: {}", table1, table2, hasDirectEdge);
			if (!hasDirectEdge) {
				log.debug("No direct edge found. {} neighbors: {}", table1, neighbors);
			}
			
			return hasDirectEdge;
		}

		public Set<String> getNeighbors(String table) {
			Set<String> neighbors = adjacencyList.get(table);
			// 打印调试信息
			log.debug("Getting neighbors for {}: {}", table, neighbors);
			return neighbors != null ? new HashSet<>(neighbors) : new HashSet<>();
		}

		public JoinRelation getJoinRelation(String table1, String table2) {
			String key = table1 + "_" + table2;
			JoinRelation relation = joinConditions.get(key);
			// 打印调试信息
			log.debug("Getting join relation between {} and {}: {}", table1, table2, 
				relation != null ? "found" : "not found");
			return relation;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder("Join Graph:\n");
			
			// 打印所有节点
			sb.append("Nodes: ").append(String.join(", ", adjacencyList.keySet())).append("\n\n");
			
			// 打印所有边
			sb.append("Edges:\n");
			Set<String> printedPairs = new HashSet<>();
			for (Map.Entry<String, Set<String>> entry : adjacencyList.entrySet()) {
				String table1 = entry.getKey();
				for (String table2 : entry.getValue()) {
					String pairKey = table1.compareTo(table2) < 0 
						? table1 + "_" + table2 
						: table2 + "_" + table1;
					
					if (!printedPairs.contains(pairKey)) {
						JoinRelation relation = joinConditions.get(pairKey);
						sb.append(String.format("  %s <-> %s\n", table1, table2));
						if (relation != null && relation.getJoinCondition() != null) {
							for (Triple<String, String, String> condition : relation.getJoinCondition()) {
								sb.append(String.format("    %s.%s %s %s.%s\n",
									table1, condition.getLeft(),
									condition.getMiddle(),
									table2, condition.getRight()));
							}
						}
						printedPairs.add(pairKey);
					}
				}
			}
			return sb.toString();
		}
	}

}

