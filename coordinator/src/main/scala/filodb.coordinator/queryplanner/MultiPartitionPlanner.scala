package filodb.coordinator.queryplanner

import filodb.core.query.QueryContext
import filodb.query.LogicalPlan
import filodb.query.exec.ExecPlan

class MultiPartitionPlanner(metricPlannerMap: Map[String, QueryPlanner],
                            localPlanner: QueryPlanner) extends  QueryPlanner {

  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    // TODO create multiple planners based on query locality and stitch results
    localPlanner.materialize(logicalPlan, qContext)
  }
}
