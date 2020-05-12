package filodb.coordinator.queryplanner

import filodb.core.DatasetRef
import filodb.core.query.{PromQlQueryParams, QueryContext}
import filodb.query.LogicalPlan
import filodb.query.exec.{ExecPlan, InProcessPlanDispatcher, PromQlExec}

class RemotePartitionPlanner(dsRef: DatasetRef) extends QueryPlanner {

  override def getSingleClusterPlanner: SingleClusterPlanner = ???
  /**
    * Converts a logical plan to execution plan.
    *
    * @param logicalPlan Logical plan after converting PromQL -> AST -> LogicalPlan
    * @param qContext    Should have PromQlQueryParams which will have routing details
    * @return materialized Execution Plan which can be dispatched
    */
  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val promQlParams = PromQlQueryParams(queryParams.config, queryParams.promQl,
      queryParams.startSecs, queryParams.stepSecs, queryParams.endSecs,
      queryParams.spread, processFailure = true)

   PromQlExec(qContext, InProcessPlanDispatcher, dsRef, promQlParams)
  }
}
