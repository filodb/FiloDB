package filodb.coordinator.queryengine2

import filodb.query.{LogicalPlan, QueryContext}
import filodb.query.exec.ExecPlan

/**
  * DownsampleStitchPlanner knows retention of raw data. For any query that arrives
  * beyond the retention period of raw data, it splits the query into two time ranges -
  * latest time range from raw data, and old time range from downsampled data
  * @param rawClusterPlanner
  * @param downsampleClusterPlanner
  */
class DownsampleStitchPlanner(rawClusterPlanner: SingleClusterPlanner,
                              downsampleClusterPlanner: SingleClusterPlanner) extends QueryPlanner {

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    ???
  }

}
