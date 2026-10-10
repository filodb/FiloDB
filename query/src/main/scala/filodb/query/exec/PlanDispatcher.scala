package filodb.query.exec

import java.util.concurrent.TimeUnit

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.GlobalConfig
import filodb.core.metrics.FilodbMetrics
import filodb.core.store.ChunkSource
import filodb.query.{QueryResponse, StreamQueryResponse}

object PlanDispatcher {
  val streamingResultsEnabled = GlobalConfig.systemConfig.getBoolean("filodb.query.streaming-query-results-enabled")

  /**
    * Latency of dispatching an ExecPlan, measured at the dispatcher itself — the universal choke point that every
    * dispatch (top-level and child) flows through, regardless of whether it was reached via
    * QueryPlanner.dispatchExecPlan or a direct `execPlan.dispatcher.dispatch(...)` call. Tagged with:
    *   - `dispatch`: "local" (in-process) | "remote" (actor/grpc/flight), derived from [[PlanDispatcher.isLocalCall]]
    *   - `dataset`
    * Exported to Prometheus as `query_dispatch_latency_seconds`.
    */
  lazy val dispatchLatency = FilodbMetrics.timeHistogram("query-dispatch-latency", TimeUnit.NANOSECONDS)
}

/**
  * This trait externalizes distributed query execution strategy
  * from the ExecPlan.
  */
trait PlanDispatcher extends java.io.Serializable {
  def clusterName: String
  def isLocalCall: Boolean

  /** Metric label for [[PlanDispatcher.dispatchLatency]]: "local" for in-process dispatch, "remote" otherwise. */
  def dispatchType: String = if (isLocalCall) "local" else "remote"

  /**
    * Dispatch the plan, recording dispatch latency labeled by [[dispatchType]]. `final` so the measurement cannot
    * be bypassed; implementations provide the actual dispatch behavior in [[doDispatch]].
    */
  final def dispatch(plan: ExecPlanWithClientParams, source: ChunkSource)
                    (implicit sched: Scheduler): Task[QueryResponse] = {
    val startNanos = System.nanoTime()
    doDispatch(plan, source).guarantee(Task.eval {
      PlanDispatcher.dispatchLatency.record(System.nanoTime() - startNanos,
        Map("dispatch" -> dispatchType, "dataset" -> plan.execPlan.dataset.dataset))
    })
  }

  final def dispatchStreaming(plan: ExecPlanWithClientParams, source: ChunkSource)
                             (implicit sched: Scheduler): Observable[StreamQueryResponse] = {
    val startNanos = System.nanoTime()
    doDispatchStreaming(plan, source).guarantee(Task.eval {
      PlanDispatcher.dispatchLatency.record(System.nanoTime() - startNanos,
        Map("dispatch" -> dispatchType, "dataset" -> plan.execPlan.dataset.dataset))
    })
  }

  /** Actual dispatch implementation; wrapped by [[dispatch]] for latency measurement. */
  def doDispatch(plan: ExecPlanWithClientParams, source: ChunkSource)
                (implicit sched: Scheduler): Task[QueryResponse]

  def doDispatchStreaming(plan: ExecPlanWithClientParams, source: ChunkSource)
                         (implicit sched: Scheduler): Observable[StreamQueryResponse]
}
