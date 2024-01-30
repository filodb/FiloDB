package filodb.coordinator

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import akka.actor.ActorRef
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import filodb.coordinator.client.QueryCommands.ProtoExecPlan
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import filodb.core.QueryTimeoutException
import filodb.core.query.{QueryStats, QueryWarnings, ResultSchema}
import filodb.core.store.ChunkSource
import filodb.query.{QueryResponse, QueryResult, StreamQueryResponse, StreamQueryResultFooter}
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
    val remainingTime = plan.clientParams.deadlineMs - queryTimeElapsed
    lazy val emptyPartialResult: QueryResult = QueryResult(plan.execPlan.queryContext.queryId, ResultSchema.empty, Nil,
      QueryStats(), QueryWarnings(), true, Some("Result may be partial since query on some shards timed out"))

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
        // HACK
        val doProto = plan.execPlan.queryContext.plannerParams.warnLimits.groupByCardinality == 7777
        val message = if (doProto) {
          val protoPlan = ProtoConverters.execPlanToProto(plan.execPlan)
          ProtoExecPlan(plan.execPlan.dataset, protoPlan.toByteArray, plan.execPlan.submitTime)
        } else {
          plan.execPlan
        }
        val fut = (target ? message) (t).map {
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
    val remainingTime = plan.clientParams.deadlineMs - queryTimeElapsed
    lazy val emptyPartialResult = StreamQueryResultFooter(plan.execPlan.queryContext.queryId, plan.execPlan.planId,
      QueryStats(), QueryWarnings(), true, Some("Result may be partial since query on some shards timed out"))

    // Don't send if time left is very small
    if (remainingTime < 1) {
      Observable.raiseError(QueryTimeoutException(queryTimeElapsed, this.getClass.getName))
    } else {
      // Query Planner sets target as null when shard is down
      if (target == ActorRef.noSender) {
        Observable.now({
          qLogger.warn(s"Creating partial result as shard is not available")
          emptyPartialResult
        })
      } else {
        ResultActor.subject
          .doOnSubscribe(Task.eval {
            target.tell(plan.execPlan, ResultActor.resultActor)
            qLogger.debug(s"DISPATCHING ${plan.execPlan.planId}")
          })
         .filter(_.planId == plan.execPlan.planId)
         .takeWhileInclusive(!_.isLast)
        // TODO timeout query if response stream not completed in time
      }
    }
  }

  override def isLocalCall: Boolean = false
}
