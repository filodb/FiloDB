package filodb.coordinator.queryplanner

import filodb.core.query.QueryContext
import filodb.query.{BinaryJoin, LabelValues, LogicalPlan, SeriesKeysByFilters}
import filodb.query.exec.{ExecPlan, LabelValuesDistConcatExec, PartKeysDistConcatExec}

trait PlannerProvider {

  /**
    * Returns remote cluster planner for given metricName.
    * If it does not return anything localPlanner should be used
    *
    * @param metricName Name of metric
    */
  def getRemotePlanner(metricName: String): Option[QueryPlanner]

  /**
    * Returns all remote planners for current partition
    */
  def getAllRemotePlanners: Seq[QueryPlanner]

  /**
    * Returns true if metricName belongs to remote planner/cluster
    *
    * @param metricName Name of metric
    */
  def hasRemotePlanner(metricName: String): Boolean = getRemotePlanner(metricName).isDefined
}

/**
  * MultiClusterPlanner is responsible for planning in situations where time series data is
  * distributed across multiple clusters.
  *
  */
class MultiClusterPlanner(plannerProvider: PlannerProvider, localPlanner: HighAvailabilityPlanner)
  extends QueryPlanner {

  override def getBasePlanner: SingleClusterPlanner = localPlanner.getBasePlanner

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    logicalPlan match {
      case lp: BinaryJoin          => materializeBinaryJoin(lp, qContext)
      case lp: LabelValues         => materializeLabelValues(lp, qContext)
      case lp: SeriesKeysByFilters => materializeSeriesKeysFilters(lp, qContext)
      case _                       => materializeSimpleQuery(logicalPlan, qContext)

    }
  }

  private def materializeSimpleQuery(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    plannerProvider.getRemotePlanner(LogicalPlanUtils.getMetricName(logicalPlan).head).getOrElse(localPlanner).
      materialize(logicalPlan, qContext)
  }

  private def materializeBinaryJoin(logicalPlan: BinaryJoin, qContext: QueryContext): ExecPlan = {

    val lhsMetrics = LogicalPlanUtils.getMetricName(logicalPlan.lhs)
    val rhsMetrics = LogicalPlanUtils.getMetricName(logicalPlan.rhs)
    val lhsPlanner = plannerProvider.getRemotePlanner(lhsMetrics.head).getOrElse(localPlanner)
    val rhsPlanner = plannerProvider.getRemotePlanner(rhsMetrics.head).getOrElse(localPlanner)
    val lhsExec = logicalPlan.lhs match {
      case b: BinaryJoin => materializeBinaryJoin(b, qContext)
      case _             => lhsPlanner.materialize(logicalPlan.lhs, qContext)
    }

    val rhsExec = logicalPlan.rhs match {
      case b: BinaryJoin => materializeBinaryJoin(b, qContext)
      case _             => rhsPlanner.materialize(logicalPlan.rhs, qContext)
    }

    var planner = lhsPlanner
    var dispatcher = lhsExec.dispatcher


    // If LHS is remote planner and RHS is local, use RHS to do BinaryJoin
    if (plannerProvider.hasRemotePlanner(lhsMetrics.head) && !plannerProvider.hasRemotePlanner(rhsMetrics.head)) {
       planner = rhsPlanner
       dispatcher = rhsExec.dispatcher
    }

    planner.getBasePlanner.createBinaryJoinExec(qContext, logicalPlan, Seq(lhsExec), Seq(rhsExec), dispatcher)

  }

  private def materializeLabelValues(logicalPlan: LogicalPlan, qContext: QueryContext) = {
    val execPlans = plannerProvider.getAllRemotePlanners.toList.distinct.map(_.materialize(logicalPlan, qContext))
    val localExec = localPlanner.materialize(logicalPlan, qContext)
    LabelValuesDistConcatExec(qContext, localExec.dispatcher, execPlans :+ localExec)
  }

  private def materializeSeriesKeysFilters(logicalPlan: LogicalPlan, qContext: QueryContext) = {
    val execPlans = plannerProvider.getAllRemotePlanners.toList.distinct.map(_.materialize(logicalPlan, qContext))
    val localExec = localPlanner.materialize(logicalPlan, qContext)
    PartKeysDistConcatExec(qContext, localExec.dispatcher, execPlans :+ localExec)
  }
}

