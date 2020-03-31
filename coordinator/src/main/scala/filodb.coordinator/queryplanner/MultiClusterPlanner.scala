package filodb.coordinator.queryplanner

import filodb.core.query.QueryContext
import filodb.query.LogicalPlan
import filodb.query.exec.ExecPlan

/**
  * MultiClusterPlanner is responsible for planning in situations where time series data is
  * distributed across multiple clusters.
  *
  * This is TBD.
  */
class MultiClusterPlanner extends QueryPlanner {

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    ???
  }

}
