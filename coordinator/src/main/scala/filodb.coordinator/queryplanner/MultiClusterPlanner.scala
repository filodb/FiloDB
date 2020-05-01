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

trait MetricLocality {
  def isLocal(metricName: String): Boolean
}

class LocalPlanner(metricPlannerMap: Map[String, QueryPlanner], localPlanner: SingleClusterPlanner,
                   metricLocality: MetricLocality) extends QueryPlanner {

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    logicalPlan match {
      case lp: BinaryJoin          => processBinaryJoin(lp, qContext)
      case lp: LabelValues         => processLabelValues(lp, qContext)
      case lp: SeriesKeysByFilters => processSeriesKeysFilters(lp, qContext)
      case _                       => throw new IllegalArgumentException
    }
  }

  def processBinaryJoin(logicalPlan: BinaryJoin, qContext: QueryContext): ExecPlan = {

    val lhsMetrics = LogicalPlanUtils.getMetricName(logicalPlan.lhs)
    val rhsMetrics = LogicalPlanUtils.getMetricName(logicalPlan.rhs)
    val lhsLocal = if (rhsMetrics.forall(metricLocality.isLocal(_))) true else false

    val lhsExec = if(!logicalPlan.lhs.isInstanceOf[BinaryJoin])
                  processBinaryJoin(logicalPlan.lhs.asInstanceOf[BinaryJoin], qContext) else
                  metricPlannerMap.get(lhsMetrics.head).get.
                    materialize(logicalPlan.lhs, qContext)

    val rhsExec = if(!logicalPlan.rhs.isInstanceOf[BinaryJoin])
                  processBinaryJoin(logicalPlan.rhs.asInstanceOf[BinaryJoin], qContext) else
                  metricPlannerMap.get(rhsMetrics.head).get.
                    materialize(logicalPlan.rhs, qContext)

    localPlanner.materializeBinaryJoinWithChildExec(qContext,logicalPlan,lhsExec, rhsExec, lhsLocal)

  }

  def processLabelValues(values: LabelValues, qContext: QueryContext) = {
    // TODO override equals of QueryPlanner maybe compare with dsRef
    val execPlans = metricPlannerMap.values.toList.distinct.filterNot(_ == localPlanner).map(_.materialize(values, qContext))
    val localExec = localPlanner.materialize(values, qContext)
    LabelValuesDistConcatExec(qContext,localPlanner.pickDispatcher(Seq(localExec)), execPlans :+ localExec)
  }

  def processSeriesKeysFilters(values: SeriesKeysByFilters, qContext: QueryContext) = {
    val execPlans = metricPlannerMap.values.toList.distinct.filterNot(_ == localPlanner).map(_.materialize(values, qContext))
    val localExec = localPlanner.materialize(values, qContext)
    PartKeysDistConcatExec(qContext,localPlanner.pickDispatcher(Seq(localExec)), execPlans :+ localExec)
  }
}

