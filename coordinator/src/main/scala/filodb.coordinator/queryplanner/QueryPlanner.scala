package filodb.coordinator.queryplanner

import scala.concurrent.duration.FiniteDuration

import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler

import filodb.core.query.QueryContext
import filodb.query.{LogicalPlan, QueryResponse}
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

  /**
    * Trigger orchestration of the ExecPlan. It sends the ExecPlan to the destination where it will be executed.
    */
  def dispatchExecPlan(execPlan: ExecPlan,
                       parentSpan: kamon.trace.Span)
                      (implicit sched: Scheduler, timeout: FiniteDuration): Task[QueryResponse] = {
    // Please note that the following needs to be wrapped inside `runWithSpan` so that the context will be propagated
    // across threads. Note that task/observable will not run on the thread where span is present since
    // kamon uses thread-locals.
    Kamon.runWithSpan(parentSpan, false) {
      execPlan.dispatcher.dispatch(execPlan)
    }
  }

  def getSingleClusterPlanner: SingleClusterPlanner

}
