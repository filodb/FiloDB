package filodb.coordinator

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import akka.actor.{ActorRef, Props}
import akka.pattern.{ask, AskTimeoutException}
import akka.util.Timeout
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{MulticastStrategy, Observable}
import monix.reactive.subjects.ConcurrentSubject

import filodb.coordinator.ActorSystemHolder.system
import filodb.core.QueryTimeoutException
import filodb.core.query.{QueryStats, ResultSchema}
import filodb.core.store.ChunkSource
import filodb.query.{QueryResponse, QueryResult, StreamQueryError, StreamQueryResponse, StreamQueryResultFooter}
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
                       (implicit sched: Scheduler): Observable[StreamQueryResponse] = {
    // "source" is unused (the param exists to support InProcessDispatcher).
    val queryTimeElapsed = System.currentTimeMillis() - plan.execPlan.queryContext.submitTime
    val remainingTime = plan.clientParams.deadline - queryTimeElapsed
    lazy val emptyPartialResult = StreamQueryResultFooter(plan.execPlan.queryContext.queryId,
      QueryStats(), true, Some("Result may be partial since query on some shards timed out"))

    // Don't send if time left is very small
    if (remainingTime < 1) {
      Observable.raiseError(QueryTimeoutException(queryTimeElapsed, this.getClass.getName))
    } else {
      // TODO timeout query if response stream not completed in time

      // Query Planner sets target as null when shard is down
      if (target == ActorRef.noSender) {
        Observable.now({
          qLogger.warn(s"Creating partial result as shard is not available")
          emptyPartialResult
        })
      } else {
        val subject = ConcurrentSubject[StreamQueryResponse](MulticastStrategy.Publish)
        class ResultActor extends BaseActor {
          def receive: Receive = {
            case q: StreamQueryResponse =>
              try {
                subject.onNext(q)
                qLogger.debug(s"Got ${q.getClass} as response from ${sender()}")
              } catch { case e: Throwable =>
                qLogger.error(s"Exception when processing $q", e)
              }
            case msg =>
              qLogger.error(s"Unexpected message $msg in ResultActor")
                subject.onNext(StreamQueryError(plan.execPlan.queryContext.queryId, QueryStats(),
                  new IllegalStateException(s"Unexpected result type $msg")))
          }
        }
        val resultActor = system.actorOf(Props(new ResultActor))
        subject
           .doOnSubscribe(Task.eval {
             target.tell(plan.execPlan, resultActor)
             qLogger.debug(s"Sent to $target the plan ${plan.execPlan}")
           })
           .takeWhileInclusive(!_.isLast)
           .guarantee(Task.eval {
             qLogger.debug(s"Stopping $resultActor")
             system.stop(resultActor)
           })
      }
    }
  }

  override def isLocalCall: Boolean = false
}
