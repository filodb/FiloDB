package filodb.coordinator.queryengine2

import filodb.query.{LogicalPlan, QueryOptions}
import filodb.query.exec.ExecPlan

trait QueryPlanner {

  def materialize(queryId: String,
                  submitTime: Long,
                  logicalPlan: LogicalPlan,
                  options: QueryOptions): ExecPlan
}
