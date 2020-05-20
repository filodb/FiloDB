package filodb.coordinator.queryplanner

import filodb.core.query.QueryContext
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
class SinglePartitionPlanner(planners: Map[String, QueryPlanner], plannerSelector: String => String,
                             datasetMetricColumn: String)
  extends QueryPlanner {

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    logicalPlan match {
      case lp: BinaryJoin          => materializeBinaryJoin(lp, qContext)
      case lp: LabelValues         => materializeLabelValues(lp, qContext)
      case lp: SeriesKeysByFilters => materializeSeriesKeysFilters(lp, qContext)
      case _                       => materializeSimpleQuery(logicalPlan, qContext)

    }
  }

  private def materializeSimpleQuery(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    planners.get(plannerSelector(LogicalPlanUtils.getMetricName(logicalPlan).head)).get.
      materialize(logicalPlan, qContext)
  }

  private def materializeBinaryJoin(logicalPlan: BinaryJoin, qContext: QueryContext): ExecPlan = {

    val lhsMetrics = LogicalPlanUtils.getMetricName(logicalPlan.lhs)
    val rhsMetrics = LogicalPlanUtils.getMetricName(logicalPlan.rhs)
    val lhsPlanner = planners.get(plannerSelector(lhsMetrics.head)).get
    val rhsPlanner = planners.get(plannerSelector(rhsMetrics.head)).get
    val lhsExec = logicalPlan.lhs match {
      case b: BinaryJoin => materializeBinaryJoin(b, qContext)
      case _             => lhsPlanner.materialize(logicalPlan.lhs, qContext)
    }

    val rhsExec = logicalPlan.rhs match {
      case b: BinaryJoin => materializeBinaryJoin(b, qContext)
      case _             => rhsPlanner.materialize(logicalPlan.rhs, qContext)
    }

    if (logicalPlan.operator.isInstanceOf[SetOperator])
      SetOperatorExec(qContext, InProcessPlanDispatcher, Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
        LogicalPlanUtils.renameLabels(logicalPlan.on, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn), datasetMetricColumn)
    else
      BinaryJoinExec(qContext, InProcessPlanDispatcher, Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
        logicalPlan.cardinality, LogicalPlanUtils.renameLabels(logicalPlan.on, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.include, datasetMetricColumn), datasetMetricColumn)
  }

  private def materializeLabelValues(logicalPlan: LogicalPlan, qContext: QueryContext) = {
    val execPlans = planners.values.toList.distinct.map(_.materialize(logicalPlan, qContext))
    LabelValuesDistConcatExec(qContext, InProcessPlanDispatcher, execPlans)
  }

  private def materializeSeriesKeysFilters(logicalPlan: LogicalPlan, qContext: QueryContext) = {
    val execPlans = planners.values.toList.distinct.map(_.materialize(logicalPlan, qContext))
    PartKeysDistConcatExec(qContext, InProcessPlanDispatcher, execPlans)
  }
}

