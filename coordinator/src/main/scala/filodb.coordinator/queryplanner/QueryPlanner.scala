package filodb.coordinator.queryplanner

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.metrics.FilodbMetrics
import filodb.core.query.{QueryContext, QuerySession}
import filodb.query.{LogicalPlan, QueryResponse, StreamQueryResponse}
import filodb.query.exec.{ClientParams, ExecPlan, ExecPlanWithClientParams, UnsupportedChunkSource}

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

  /**
    * Trigger orchestration of the ExecPlan. It sends the ExecPlan to the destination where it will be executed.
    *
    * This overload preserves the original signature so existing callers (the HTTP query API and the CLI) are
    * unaffected; their dispatch is attributed to the HTTP entry path for the `query-dispatch-latency` metric.
    */
  def dispatchExecPlan(execPlan: ExecPlan,
                       querySession: QuerySession,
                       parentSpan: kamon.trace.Span)
                      (implicit sched: Scheduler, timeout: FiniteDuration): Task[QueryResponse] =
    dispatchExecPlan(execPlan, querySession, parentSpan, QueryPlanner.SourceHttp)

  /**
    * Trigger orchestration of the ExecPlan.
    *
    * @param querySource the entry transport that originated this dispatch ("http" | "grpc"). It is emitted as the
    *                    `source` tag on the shared `query-dispatch-latency` metric so the HTTP and gRPC entry paths
    *                    can be compared apples-to-apples. NOTE: it is passed as a method argument and is NOT
    *                    serialized into the QueryContext, so it introduces no cross-node (Kryo/proto) compatibility
    *                    concerns with the data nodes.
    */
  def dispatchExecPlan(execPlan: ExecPlan,
                       querySession: QuerySession,
                       parentSpan: kamon.trace.Span,
                       querySource: String)
                      (implicit sched: Scheduler, timeout: FiniteDuration): Task[QueryResponse] = {
    val startNanos = System.nanoTime()
    // Please note that the following needs to be wrapped inside `runWithSpan` so that the context will be propagated
    // across threads. Note that task/observable will not run on the thread where span is present since
    // kamon uses thread-locals.
    // Dont finish span since this code didnt create it
    Kamon.runWithSpan(parentSpan, false) {
      // UnsupportedChunkSource because leaf plans shouldn't execute in-process from a planner method call.
      execPlan.dispatcher.dispatch(ExecPlanWithClientParams(execPlan,
        ClientParams(execPlan.queryContext.plannerParams.queryTimeoutMillis),
        querySession), UnsupportedChunkSource())
    }.guarantee(Task.eval {
      QueryPlanner.dispatchPlanLatency.record(System.nanoTime() - startNanos,
        Map("source" -> querySource, "dataset" -> execPlan.dataset.dataset))
    })
  }

  /**
    * Streaming variant. See [[dispatchExecPlan]]; this overload keeps the original signature for existing callers.
    */
  def dispatchStreamingExecPlan(execPlan: ExecPlan,
                       querySession: QuerySession,
                       parentSpan: kamon.trace.Span)
                      (implicit sched: Scheduler, timeout: FiniteDuration): Observable[StreamQueryResponse] =
    dispatchStreamingExecPlan(execPlan, querySession, parentSpan, QueryPlanner.SourceHttp)

  def dispatchStreamingExecPlan(execPlan: ExecPlan,
                       querySession: QuerySession,
                       parentSpan: kamon.trace.Span,
                       querySource: String)
                      (implicit sched: Scheduler, timeout: FiniteDuration): Observable[StreamQueryResponse] = {
    val startNanos = System.nanoTime()
    // Please note that the following needs to be wrapped inside `runWithSpan` so that the context will be propagated
    // across threads. Note that task/observable will not run on the thread where span is present since
    // kamon uses thread-locals.
    // Dont finish span since this code didnt create it
    Kamon.runWithSpan(parentSpan, false) {
      // UnsupportedChunkSource because leaf plans shouldn't execute in-process from a planner method call.
      execPlan.dispatcher.dispatchStreaming(ExecPlanWithClientParams(execPlan,
        ClientParams(execPlan.queryContext.plannerParams.queryTimeoutMillis),
        querySession), UnsupportedChunkSource())
    }.guarantee(Task.eval {
      QueryPlanner.dispatchPlanLatency.record(System.nanoTime() - startNanos,
        Map("source" -> querySource, "dataset" -> execPlan.dataset.dataset))
    })
  }

}

object QueryPlanner {
  /** Entry-transport tag values for the shared `query-dispatch-latency` metric. */
  val SourceHttp = "http"
  val SourceGrpc = "grpc"

  /**
    * Execute-dispatch latency emitted from BOTH the HTTP and gRPC entry paths at their shared convergence
    * (`dispatchExecPlan` / `dispatchStreamingExecPlan`). Tagged with `source` (http|grpc) and `dataset` so the two
    * transports can be compared directly. Exported to Prometheus as `query_dispatch_latency_seconds`.
    */
  lazy val dispatchPlanLatency =
    FilodbMetrics.timeHistogram("query-dispatch-latency", TimeUnit.NANOSECONDS)
}
