package filodb.query.exec

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import monix.eval.Task

import filodb.query.QueryResponse

/**
  * This trait externalizes distributed query execution strategy
  * from the ExecPlan.
  */
trait PlanDispatcher extends java.io.Serializable {
  def dispatch(plan: BaseExecPlan)
              (implicit sched: ExecutionContext,
               timeout: FiniteDuration): Task[QueryResponse]
}

/**
  * This implementation provides a way to distribute query execution
  * using Akka Actors.
  */
case class ActorPlanDispatcher(target: ActorRef) extends PlanDispatcher {

  def dispatch(plan: BaseExecPlan)
              (implicit sched: ExecutionContext,
               timeout: FiniteDuration): Task[QueryResponse] = {
    implicit val _ = Timeout(timeout)
    val fut = (target ? plan).map {
      case resp: QueryResponse => resp
      case e =>  throw new IllegalStateException(s"Received bad response $e")
    }
    Task.fromFuture(fut)
  }
}
