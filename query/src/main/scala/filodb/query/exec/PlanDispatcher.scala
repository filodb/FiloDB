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
  def dispatch(plan: ExecPlan)
              (implicit sched: Scheduler): Task[QueryResponse]
}

/**
  * This implementation provides a way to distribute query execution
  * using Akka Actors.
  */
case class ActorPlanDispatcher(target: ActorRef) extends PlanDispatcher {

  def dispatch(plan: ExecPlan)(implicit sched: Scheduler): Task[QueryResponse] = {
    val queryTimeElapsed = System.currentTimeMillis() - plan.queryContext.submitTime
    val remainingTime = plan.queryContext.queryTimeoutMillis - queryTimeElapsed
    // Don't send if time left is very small
    if (remainingTime < 1) {
      Task.raiseError(QueryTimeoutException(queryTimeElapsed, this.getClass.getName))
    } else {
      val t = Timeout(FiniteDuration(remainingTime, TimeUnit.MILLISECONDS))
      val fut = (target ? plan)(t).map {
        case resp: QueryResponse => resp
        case e =>  throw new IllegalStateException(s"Received bad response $e")
      }
      Task.fromFuture(fut)
    }
  }
}
