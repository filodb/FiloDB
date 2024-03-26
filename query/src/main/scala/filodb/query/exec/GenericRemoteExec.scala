package filodb.query.exec

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.DatasetRef
import filodb.core.query.QueryContext
import filodb.core.query.QuerySession
import filodb.core.store.ChunkSource
import filodb.query._


case class GenericRemoteExec(
   dispatcher: PlanDispatcher,
   execPlan: ExecPlan
) extends ExecPlan {
  /**
   * Query Processing parameters
   */
  override def queryContext: QueryContext = execPlan.queryContext

  /**
   * Child execution plans representing sub-queries
   */
  override def children: Seq[ExecPlan] = ???

  override def dataset: DatasetRef = execPlan.dataset

  /**
   * This is a simple shell that does not use execute or executeStreaming logic of the parent ExecPlan
   */
  override def doExecute(source: ChunkSource, querySession: QuerySession)(implicit sched: Scheduler): ExecResult = ???

  override def execute(source: ChunkSource, querySession: QuerySession)
                      (implicit sched: Scheduler): Task[QueryResponse] = {
    val planWithParams = ExecPlanWithClientParams(
      execPlan,
      ClientParams(execPlan.queryContext.plannerParams.queryTimeoutMillis)
    )
    dispatcher.dispatch(planWithParams, UnsupportedChunkSource())
  }

  /**
   * Implement when shard level plan execution is done
   */
  override def executeStreaming(source: ChunkSource, querySession: QuerySession)
                               (implicit sched: Scheduler): Observable[StreamQueryResponse] = ???

  /**
   * Args to use for the ExecPlan for printTree purposes only.
   * DO NOT change to a val. Increases heap usage.
   */
  override protected def args: String = ""

  override def submitTime: Long = execPlan.queryContext.submitTime
}