package filodb.coordinator.queryengine2

import filodb.query.{LogicalPlan, QueryOptions}
import filodb.query.exec.ExecPlan

class DownsampleStitchPlanner(rawClusterPlanner: SingleClusterPlanner,
                              downsampleClusterPlanner: SingleClusterPlanner) extends QueryPlanner {

  def materialize(queryId: String,
                  submitTime: Long,
                  rootLogicalPlan: LogicalPlan,
                  options: QueryOptions): ExecPlan = {
    ???
  }

}
