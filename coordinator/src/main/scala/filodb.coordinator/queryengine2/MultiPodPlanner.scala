package filodb.coordinator.queryengine2

import filodb.query.{LogicalPlan, QueryContext}
import filodb.query.exec.ExecPlan

/**
  * MultiPodPlanner is responsible for breaking down queries into separate subqueries,
  * one for each metric, routing each of those subqueries to the pod that houses the metric,
  * fetches results and applies transformation on top
  */
class MultiPodPlanner extends QueryPlanner {

  def materialize(logicalPlan: LogicalPlan,
                  qContext: QueryContext): ExecPlan = {
    ???
  }

}
