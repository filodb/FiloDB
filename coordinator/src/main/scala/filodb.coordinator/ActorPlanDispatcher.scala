package filodb.coordinator

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, AskTimeoutException}
import akka.util.Timeout
import monix.catnap.ConcurrentQueue
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.coordinator.ActorSystemHolder.system
import filodb.core.QueryTimeoutException
import filodb.core.query.{QueryStats, ResultSchema}
import filodb.core.store.ChunkSource
import filodb.query.{QueryResponse, QueryResult, StrQueryResponse, StrQueryResultFooter}
import filodb.query.Query.qLogger
import filodb.query.exec.{ExecPlanWithClientParams, PlanDispatcher}


/**
 * This implementation provides a way to distribute query execution
 * using Akka Actors.
 */
case class ActorPlanDispatcher(target: ActorRef, clusterName: String) extends PlanDispatcher {

  def dispatch(plan: ExecPlanWithClientParams, source: ChunkSource)(implicit sched: Scheduler): Task[QueryResponse] = {
    // "source" is unused (the param exists to support InProcessDispatcher).
    val queryTimeElapsed = System.currentTimeMillis() - plan.execPlan.queryContext.submitTime
    val remainingTime = plan.clientParams.deadline - queryTimeElapsed
    lazy val emptyPartialResult: QueryResult = QueryResult(plan.execPlan.queryContext.queryId, ResultSchema.empty, Nil,
      QueryStats(), true, Some("Result may be partial since query on some shards timed out"))

    // Don't send if time left is very small
    if (remainingTime < 1) {
      Task.raiseError(QueryTimeoutException(queryTimeElapsed, this.getClass.getName))
    } else {
      val t = Timeout(FiniteDuration(remainingTime, TimeUnit.MILLISECONDS))

      // Query Planner sets target as null when shard is down
      if (target == ActorRef.noSender) {
        Task.eval({
          qLogger.warn(s"Creating partial result as shard is not available")
          emptyPartialResult
        })
      } else {
        val fut = (target ? plan.execPlan) (t).map {
          case resp: QueryResponse => resp
          case e => throw new IllegalStateException(s"Received bad response $e")
        }
          // TODO We can send partial results on timeout. Try later. Need to address QueryTimeoutException too.
          .recover { // if partial results allowed, then return empty result
            case e: AskTimeoutException if (plan.execPlan.queryContext.plannerParams.allowPartialResults)
            =>
              qLogger.warn(s"Swallowed AskTimeoutException for query id: ${plan.execPlan.queryContext.queryId} " +
                s"since partial result was enabled: ${e.getMessage}")
              emptyPartialResult
          }

        Task.fromFuture(fut)
      }
    }
  }

  def dispatchStreaming(plan: ExecPlanWithClientParams, source: ChunkSource)
                       (implicit sched: Scheduler): Observable[StrQueryResponse] = {
    // "source" is unused (the param exists to support InProcessDispatcher).
    val queryTimeElapsed = System.currentTimeMillis() - plan.execPlan.queryContext.submitTime
    val remainingTime = plan.clientParams.deadline - queryTimeElapsed
    lazy val emptyPartialResult = StrQueryResultFooter(plan.execPlan.queryContext.queryId,
      QueryStats(), true, Some("Result may be partial since query on some shards timed out"))

    // Don't send if time left is very small
    if (remainingTime < 1) {
      Observable.raiseError(QueryTimeoutException(queryTimeElapsed, this.getClass.getName))
    } else {
      val t = FiniteDuration(remainingTime, TimeUnit.MILLISECONDS)

      // Query Planner sets target as null when shard is down
      if (target == ActorRef.noSender) {
        Observable.now({
          qLogger.warn(s"Creating partial result as shard is not available")
          emptyPartialResult
        })
      } else {
        Observable.fromTask(ConcurrentQueue.unbounded[Task, StrQueryResponse]()).flatMap { queue =>
          class ResultActor extends Actor {
            def receive: Receive = {
              case q: StrQueryResponse => queue.offer(q)
            }
          }
          // temporary actor to get streaming results
          val resultActor = system.actorOf(Props.create(classOf[ResultActor]))
          target.tell(plan, resultActor) // send the query to the target
          // deal with timeout by sending partial result flag
          lazy val timeoutPartialResult = StrQueryResultFooter(plan.execPlan.queryContext.queryId,
            QueryStats(), true, Some("Result may be partial since some shards did not respond in time"))
          system.scheduler.scheduleOnce(t, resultActor, timeoutPartialResult)(system.dispatcher, Actor.noSender)
          Observable.repeatEvalF(queue.poll)
            .takeWhileInclusive(_.isInstanceOf[StrQueryResultFooter])
            .guarantee(Task.eval(system.stop(resultActor)))
        }
      }
    }

  }

  override def isLocalCall: Boolean = false
}
