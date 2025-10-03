package filodb.coordinator

import java.util.concurrent.TimeUnit

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorRef
import akka.pattern.{ask, AskTimeoutException}
import akka.util.Timeout
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.coordinator.client.QueryCommands.ProtoExecPlan
import filodb.core.QueryTimeoutException
import filodb.core.query.{QueryStats, QueryWarnings, ResultSchema}
import filodb.core.store.ChunkSource
import filodb.query._
import filodb.query.Query.qLogger
import filodb.query.exec.{ExecPlanWithClientParams, PlanDispatcher}

/**
 * This implementation provides a way to distribute query execution
 * using Akka Actors.
 */
case class ActorPlanDispatcher(target: ActorRef, clusterName: String) extends PlanDispatcher {

  def getCaseClassOrProtoExecPlan(execPlan: filodb.query.exec.ExecPlan): QueryCommand = {
    val doProto = execPlan.queryContext.plannerParams.useProtoExecPlans
    val ep =
      if (doProto) {
        import filodb.coordinator.ProtoConverters._
        val protoPlan = execPlan.toExecPlanContainerProto
        val protoQueryContext = execPlan.queryContext.toProto
        ProtoExecPlan(execPlan.dataset, protoPlan.toByteArray, protoQueryContext.toByteArray, execPlan.submitTime)
      } else {
        execPlan
      }
    ep
  }

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
          qLogger.warn(s"Target Actor is ActorRef.noSender ! Creating partial result as shard is not available")
          emptyPartialResult
        })
      } else {
        val message = getCaseClassOrProtoExecPlan(plan.execPlan)
        val fut = (target ? message) (t).map {
          case resp: QueryResponse => resp
          case e => throw new IllegalStateException(s"Received bad response $e")
        }
          // TODO We can send partial results on timeout. Try later. Need to address QueryTimeoutException too.
          .recoverWith {
            case e: AskTimeoutException =>
              qLogger.error(s"AskTimeoutException for query id: " +
                s"${plan.execPlan.queryContext.queryId} to target ${target.path}: ${e.getMessage}")
              Query.timeOutCounter
                .withTag("dispatcher", "actor-plan")
                .withTag("dataset", plan.execPlan.dataset.dataset)
                .withTag("cluster", clusterName)
                .withTag("target", target.path.toString)
                .increment()
              if (plan.execPlan.queryContext.plannerParams.allowPartialResults) {
                qLogger.warn(s"Swallowed AskTimeoutException since partial results are enabled.")
                Future.successful(emptyPartialResult)
              } else {
                Future.failed(e) // re-throw exception
              }
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
            target.tell(getCaseClassOrProtoExecPlan(plan.execPlan), ResultActor.resultActor)
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
