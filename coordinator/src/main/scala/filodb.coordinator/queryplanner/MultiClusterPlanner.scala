// scalastyle:off
package filodb.coordinator.queryplanner

import filodb.core.query.QueryContext
import filodb.query.exec.{ExecPlan, LabelValuesDistConcatExec, PartKeysDistConcatExec}
import filodb.query.{BinaryJoin, LabelValues, LogicalPlan, SeriesKeysByFilters}

/**
  * MultiClusterPlanner is responsible for planning in situations where time series data is
  * distributed across multiple clusters.
  *
  */

case class RoutingKey (workspace: String, namespace: String)

trait PlannerProvider {

  // It will give remote cluster planner for given metricName.
  // If it does not return anything localPlanner should be used
  def getRemotePlanner(metricName: String): Option[QueryPlanner]
  
  // Get remote planners for current partition
  def getRemotePlannersForCurrentPartition: Seq[QueryPlanner]
}

class MultiClusterPlanner(plannerProvider: PlannerProvider, localPlanner: HighAvailabilityPlanner) extends QueryPlanner {

  override def getBasePlanner: SingleClusterPlanner = localPlanner.getBasePlanner

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    logicalPlan match {
      case lp: BinaryJoin          => processBinaryJoin(lp, qContext)
      case lp: LabelValues         => processLabelValues(lp, qContext)
      case lp: SeriesKeysByFilters => processSeriesKeysFilters(lp, qContext)
      case _                       => processSimpleQuery(logicalPlan, qContext)

    }
  }

  def processSimpleQuery(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    plannerProvider.getRemotePlanner(LogicalPlanUtils.getMetricName(logicalPlan)).getOrElse(localPlanner).materialize(logicalPlan, qContext)
  }

  def processBinaryJoin(logicalPlan: BinaryJoin, qContext: QueryContext): ExecPlan = {

    val lhsMetrics = LogicalPlanUtils.getMetricName(logicalPlan.lhs)
    val rhsMetrics = LogicalPlanUtils.getMetricName(logicalPlan.rhs)
    val lhsPlanner = plannerProvider.getRemotePlanner(lhsMetrics).getOrElse(localPlanner)
    val rhsPlanner = plannerProvider.getRemotePlanner(rhsMetrics).getOrElse(localPlanner)
    val lhsExec = logicalPlan.lhs match {
      case b: BinaryJoin => processBinaryJoin(b, qContext)
      case _             => lhsPlanner.materialize(logicalPlan.lhs, qContext)
    }

    val rhsExec = logicalPlan.rhs match {
      case b: BinaryJoin => processBinaryJoin(b, qContext)
      case _             => rhsPlanner.materialize(logicalPlan.rhs, qContext)
    }


    // TODO Dispatch to LHS planner by default. Randomly select lhs or rhs in future
    var planner = lhsPlanner
    var dispatcher = lhsExec.dispatcher


    // If LHS is remote planner use RHS to do BinaryJoin
    if (plannerProvider.getRemotePlanner(lhsMetrics).isDefined) {
       planner = rhsPlanner
       dispatcher = rhsExec.dispatcher
    }

    planner.getBasePlanner.materializeBinaryJoin(qContext,logicalPlan, lhsExec, rhsExec, dispatcher)

  }

  def processLabelValues(logicalPlan: LogicalPlan, qContext: QueryContext) = {
    val execPlans = plannerProvider.getRemotePlannersForCurrentPartition.toList.distinct.map(_.getBasePlanner.materialize(values, qContext))
    val localExec = localPlanner.materialize(logicalPlan, qContext)
    LabelValuesDistConcatExec(qContext,localExec.dispatcher, execPlans :+ localExec)
  }

  def processSeriesKeysFilters(logicalPlan: LogicalPlan, qContext: QueryContext) = {
    val execPlans = plannerProvider.getRemotePlannersForCurrentPartition.toList.distinct.map(_.getBasePlanner.materialize(values, qContext))
    val localExec = localPlanner.materialize(logicalPlan, qContext)
    PartKeysDistConcatExec(qContext,localExec.dispatcher, execPlans :+ localExec)
  }
}

