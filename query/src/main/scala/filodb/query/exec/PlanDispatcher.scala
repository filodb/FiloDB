package filodb.query.exec

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import monix.eval.Task
import monix.execution.Scheduler

import filodb.core.QueryTimeoutException
import filodb.query.QueryResponse

/**
  * This trait externalizes distributed query execution strategy
  * from the ExecPlan.
  */
trait PlanDispatcher extends java.io.Serializable {
  def clusterName: String
  def dispatch(plan: ExecPlan)
              (implicit sched: Scheduler): Task[QueryResponse]
}

/**
  * This implementation provides a way to distribute query execution
  * using Akka Actors.
  */
case class ActorPlanDispatcher(target: ActorRef, clusterName: String) extends PlanDispatcher {

  def dispatch(plan: ExecPlan)(implicit sched: Scheduler): Task[QueryResponse] = {
    val queryTimeElapsed = System.currentTimeMillis() - plan.queryContext.submitTime
    val remainingTime = plan.queryContext.plannerParams.queryTimeoutMillis - queryTimeElapsed
    // Don't send if time left is very small
    if (remainingTime < 1) {
      Task.raiseError(QueryTimeoutException(queryTimeElapsed, this.getClass.getName))
    } else {
      val t = Timeout(FiniteDuration(remainingTime, TimeUnit.MILLISECONDS))
      val fut = (target ? plan)(t).map {
        case resp: QueryResponse => resp
        case e =>  throw new IllegalStateException(s"Received bad response $e")
      }
      // TODO We can send partial results on timeout. Try later. Need to address QueryTimeoutException too.
//        .recover { // if partial results allowed, then return empty result
//        case e: AskTimeoutException if (plan.queryContext.plannerParams.allowPartialResults) =>
//            Query.qLogger.warn(s"Swallowed AskTimeoutException since partial result was enabled: ${e.getMessage}")
//            QueryResult(plan.queryContext.queryId, ResultSchema.empty, Nil, true,
//              Some("Result may be partial since query on some shards timed out"))
//      }
      Task.fromFuture(fut)
    }
  }
}
