package filodb.query.exec

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.store.ChunkSource
import filodb.query.{QueryResponse, StrQueryResponse}

object PlanDispatcher {
  val streamingResultsEnabled = false
}

/**
  * This trait externalizes distributed query execution strategy
  * from the ExecPlan.
  */
trait PlanDispatcher extends java.io.Serializable {
  def clusterName: String
  def dispatch(plan: ExecPlanWithClientParams, source: ChunkSource)
              (implicit sched: Scheduler): Task[QueryResponse]

  def dispatchStreaming(plan: ExecPlanWithClientParams, source: ChunkSource)
                        (implicit sched: Scheduler): Observable[StrQueryResponse]
  def isLocalCall: Boolean
}
