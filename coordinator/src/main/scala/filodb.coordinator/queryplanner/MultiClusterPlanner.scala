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
  def getPlanner(metricName: String): QueryPlanner
  // Returns true if Planner is for local/current cluster
  def isLocal(planner: QueryPlanner): Boolean
  // Give all remote & local planners for given time range
  def getPlanners (routingKey: RoutingKey, queryTimeRange: TimeRange): Seq[QueryPlanner]
  // Get remote planners for current partition. It will give all Recording rules cluster planners and
  def getRemotePlannersForPartition: Seq[QueryPlanner]
}

class MultiClusterPlanner(plannerProvider: PlannerProvider, localPlanner: HighAvailabilityPlanner) extends QueryPlanner {

  override def getSingleClusterPlanner: SingleClusterPlanner = localPlanner.getSingleClusterPlanner

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    logicalPlan match {
      case lp: BinaryJoin          => processBinaryJoin(lp, qContext)
      case lp: LabelValues         => processLabelValues(lp, qContext)
      case lp: SeriesKeysByFilters => processSeriesKeysFilters(lp, qContext)
      case _                       => processSimpleQuery(logicalPlan, qContext)

    }
  }

  def processSimpleQuery(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    plannerProvider.getPlanner(LogicalPlanUtils.getLabelValueFromLogicalPlan(logicalPlan, PromMetricLabel).get.head).materialize(logicalPlan, qContext)
  }

  def processBinaryJoin(logicalPlan: BinaryJoin, qContext: QueryContext): ExecPlan = {

    val lhsMetrics = LogicalPlanUtils.getMetricName(logicalPlan.lhs)
    val rhsMetrics = LogicalPlanUtils.getMetricName(logicalPlan.rhs)
    val lhsPlanner = plannerProvider.getPlanner(lhsMetrics)
    val rhsPlanner = plannerProvider.getPlanner(rhsMetrics)
    val lhsExec = logicalPlan.lhs match {
      case b: BinaryJoin => processBinaryJoin(b, qContext)
      case _             => lhsPlanner.materialize(logicalPlan.lhs, qContext)
    }

    val rhsExec = logicalPlan.rhs match {
      case b: BinaryJoin => processBinaryJoin(b, qContext)
      case _             => lhsPlanner.materialize(logicalPlan.rhs, qContext)
    }

    // TODO Dispatch to LHS planner by default. Randomly select lhs or rhs in future
    var planner = lhsPlanner
    var dispatcher = lhsExec.dispatcher

    if (plannerProvider.isLocal(rhsPlanner)) {
       planner = rhsPlanner
       dispatcher = rhsExec.dispatcher
    }

    planner.getSingleClusterPlanner.materializeBinaryJoin(qContext,logicalPlan, lhsExec, rhsExec, dispatcher)

  }

  def processLabelValues(values: LabelValues, qContext: QueryContext) = {
    val execPlans = plannerProvider.getRemotePlannersForPartition.toList.distinct.map(_.materialize(values, qContext))
    val localExec = localPlanner.materialize(values, qContext)
    LabelValuesDistConcatExec(qContext,localExec.dispatcher, execPlans :+ localExec)
  }

  def processSeriesKeysFilters(values: SeriesKeysByFilters, qContext: QueryContext) = {
    val execPlans = plannerProvider.getRemotePlannersForPartition.toList.distinct.map(_.materialize(values, qContext))
    val localExec = localPlanner.materialize(values, qContext)
    PartKeysDistConcatExec(qContext,localExec.dispatcher, execPlans :+ localExec)
  }
}

