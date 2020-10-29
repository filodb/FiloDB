package filodb.coordinator.queryplanner

import filodb.core.query.{PromQlQueryParams, QueryConfig, QueryContext}
import filodb.query.{BinaryJoin, LabelValues, LogicalPlan, SeriesKeysByFilters, SetOperator}
import filodb.query.exec._

/**
  * SinglePartitionPlanner is responsible for planning in situations where time series data is
  * distributed across multiple clusters.
  *
  * @param planners map of clusters names in the local partition to their Planner objects
  * @param plannerSelector a function that selects the planner name given the metric name
  *
  */
class SinglePartitionPlanner(planners: Map[String, QueryPlanner],
                             plannerSelector: String => String,
                             datasetMetricColumn: String,
                             queryConfig: QueryConfig)
  extends QueryPlanner {

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    logicalPlan match {
      case lp: BinaryJoin          => materializeBinaryJoin(lp, qContext)
      case lp: LabelValues         => materializeLabelValues(lp, qContext)
      case lp: SeriesKeysByFilters => materializeSeriesKeysFilters(lp, qContext)
      case _                       => materializeSimpleQuery(logicalPlan, qContext)

    }
  }

  /**
    * Returns planner for first metric in logical plan
    * If logical plan does not have metric, first planner present in planners is returned
    */
  private def getPlanner(logicalPlan: LogicalPlan): QueryPlanner = {
    val planner = LogicalPlanUtils.getMetricName(logicalPlan, datasetMetricColumn)
      .map(x => planners.get(plannerSelector(x)).get)
    if(planner.isEmpty)  planners.values.head else planner.head
  }

  /**
   * Returns lhs and rhs planners of BinaryJoin
   */
  private def getBinaryJoinPlanners(binaryJoin: BinaryJoin) : Seq[QueryPlanner] = {
    val lhsPlanners = binaryJoin.lhs match {
      case b: BinaryJoin => getBinaryJoinPlanners(b)
      case _             => Seq(getPlanner(binaryJoin.lhs))

    }

    val rhsPlanners = binaryJoin.rhs match {
      case b: BinaryJoin => getBinaryJoinPlanners(b)
      case _             => Seq(getPlanner(binaryJoin.rhs))

    }
    lhsPlanners ++ rhsPlanners
  }

  private def materializeSimpleQuery(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    getPlanner(logicalPlan).materialize(logicalPlan, qContext)
  }

  private def materializeBinaryJoin(logicalPlan: BinaryJoin, qContext: QueryContext): ExecPlan = {
    val allPlanners = getBinaryJoinPlanners(logicalPlan)

    if (allPlanners.forall(_.equals(allPlanners.head))) allPlanners.head.materialize(logicalPlan, qContext)
    else {

      val lhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
        copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.lhs)))
      val rhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
        copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.rhs)))

      val lhsExec = logicalPlan.lhs match {
        case b: BinaryJoin   => materializeBinaryJoin(b, lhsQueryContext)
        case               _ => getPlanner(logicalPlan.lhs).materialize(logicalPlan.lhs, lhsQueryContext)
      }

      val rhsExec = logicalPlan.rhs match {
        case b: BinaryJoin => materializeBinaryJoin(b, rhsQueryContext)
        case _             => getPlanner(logicalPlan.rhs).materialize(logicalPlan.rhs, rhsQueryContext)
      }

      val onKeysReal = ExtraOnByKeysUtil.getRealOnLabels(logicalPlan, queryConfig.addExtraOnByKeysTimeRanges)

      if (logicalPlan.operator.isInstanceOf[SetOperator])
        SetOperatorExec(qContext, InProcessPlanDispatcher, Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
          LogicalPlanUtils.renameLabels(onKeysReal, datasetMetricColumn),
          LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn), datasetMetricColumn)
      else
        BinaryJoinExec(qContext, InProcessPlanDispatcher, Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
          logicalPlan.cardinality, LogicalPlanUtils.renameLabels(onKeysReal, datasetMetricColumn),
          LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn),
          LogicalPlanUtils.renameLabels(logicalPlan.include, datasetMetricColumn), datasetMetricColumn)
    }
  }

  private def materializeLabelValues(logicalPlan: LogicalPlan, qContext: QueryContext) = {
    val execPlans = planners.values.toList.distinct.map(_.materialize(logicalPlan, qContext))
    if (execPlans.size == 1) execPlans.head
    else LabelValuesDistConcatExec(qContext, InProcessPlanDispatcher, execPlans)
  }

  private def materializeSeriesKeysFilters(logicalPlan: LogicalPlan, qContext: QueryContext) = {
    val execPlans = planners.values.toList.distinct.map(_.materialize(logicalPlan, qContext))
    if (execPlans.size == 1) execPlans.head
    else PartKeysDistConcatExec(qContext, InProcessPlanDispatcher, execPlans)
  }
}

