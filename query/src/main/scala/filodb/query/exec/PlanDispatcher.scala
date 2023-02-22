package filodb.query.exec

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.GlobalConfig
import filodb.core.store.ChunkSource
import filodb.query.{QueryResponse, StreamQueryResponse}

object PlanDispatcher {
  val streamingResultsEnabled = GlobalConfig.systemConfig.getBoolean("filodb.query.streaming-query-results-enabled")
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
                       (implicit sched: Scheduler): Observable[StreamQueryResponse]
  def isLocalCall: Boolean
}
