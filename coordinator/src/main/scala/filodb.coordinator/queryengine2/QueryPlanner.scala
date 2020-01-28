package filodb.coordinator.queryengine2

import filodb.query.{LogicalPlan, QueryContext}
import filodb.query.exec.ExecPlan

/**
  * Abstraction for Query Planning. QueryPlanners can be composed using decorator pattern to add capabilities.
  */
trait QueryPlanner {

  /**
    * Converts a logical plan to execution plan.
    *
    * @param logicalPlan Logical plan after converting PromQL -> AST -> LogicalPlan
    * @param qContext holder for additional query parameters
    * @return materialized Execution Plan which can be dispatched
    */
  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan
}
