package filodb.coordinator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.metric.MeasurementUnit
import kamon.tag.TagSet
import monix.execution.{Scheduler, UncaughtExceptionReporter}

import filodb.core.DatasetRef
import filodb.core.memstore.FiloSchedulers
import filodb.query.exec.ExecPlan

case class QueryCompletedScheduleNext(completedQueryId: String, sched: Scheduler)

/**
 * Why do we need a special Query Executor ?
 * This Query Executor came into being when we intended to spray all time series of a given metric
 * into every shard, this scattering queries to lots of shards. When the number of shards is high, there
 * are many child queries and what we see is a total shuffle of child queries from different queryIds. This shuffle
 * caused overall latency increase per query.
 *
 * This scheduler does multiple things:
 * 1. If a non-leaf plan is submitted, it schedules it immediately in a dedicated scheduler so we dont block the
 *    dispatch of child plans to other nodes
 * 2. For leaf plans, we maintain a dedicated pool of single threaded schedulers, and assign one queryId to scheduler
 *    at any given time.
 * 3. If free scheduler is not available, it waits in a queue which groups all plans by queryId. When a scheduler
 *    frees up, all plans for the queryId at the top of the queue are scheduled in that scheduler.
 *
 * Since the child queries of same queryId execute together and in quick succession, overall latency is under control.
 *
 * THIS CLASS IS NOT THREADSAFE. This class contains multiple mutable data structures that should be guarded from
 * concurrent mutation. Call methods of this class in the context of Query Actor ONLY.
 *
 * Acknowledgement: This class builds on some initial ground work done by Evan Chan (velvia)
 *
 * TODO: Add more docs describing necessity and strategy.
 */
class QueryExecutor(ref: DatasetRef,
                    parallelism: Int,
                    queryActor: ActorRef) extends StrictLogging {

  private val tags = TagSet.from(Map("dataset" -> ref.toString))
  private val schedulerAssignmentDelay = Kamon.histogram("query-scheduler-assignment-delay",
    MeasurementUnit.time.milliseconds).withTags(tags)
  private val numWaitingQueryIds = Kamon.gauge("num-waiting-queryIds").withTags(tags)

  /**
   * Represents a query that is scheduled and waiting for execution
   * @param plan the execPlan object
   * @param replyTo the actor to send query result reply to
   * @param execPlanFunc the function that will execute the query plan. The function takes the plan, the replyTo
   *                     address and the scheduler and returns a future that completes when query execution finishes.
   */
  private case class QueryToExecute(plan: ExecPlan,
                            replyTo: ActorRef,
                            execPlanFunc: (ExecPlan, ActorRef, Scheduler) => Future[Unit])

  /**
   * Map of executing queryIds to the details. It tracks number of plans currently executing for
   * each queryId. When the count drops to zero, the scheduler is free.
   * The assigned scheduler is also tracked.
   */
  private val executingQueryIdToPlanCount = new mutable.HashMap[String, Int]()

  /**
   * Free scheduler pool. Number of schedulers depends on parallelism passed in.
   */
  private val leafScheduler = Scheduler.computation(parallelism = parallelism,
    name = s"${FiloSchedulers.QuerySchedName}-$ref-Leaf",
    reporter = newExceptionHandler())

  /**
   * Ordered hash map of queryIds waiting to execute
   */
  private val waitingQueryIdToPlans = new mutable.LinkedHashMap[String, ArrayBuffer[QueryToExecute]]()

  /**
   * Non-Leaf Plan scheduler - plans are not queued but executed immediately.
   */
  private val nonLeafSched = Scheduler.computation(parallelism = parallelism,
                                           name = s"${FiloSchedulers.QuerySchedName}-$ref-NonLeaf",
                                           reporter = newExceptionHandler())

  /**
   * Invoked by query actor when there is a new query submitted to query actor
   */
  def execute(plan: ExecPlan,
              isLeafPlan: Boolean,
              replyTo: ActorRef,
              execPlanFunc: (ExecPlan, ActorRef, Scheduler) => Future[Unit]): Unit = {
    lazy val executingOn = executingQueryIdToPlanCount.get(plan.queryContext.queryId)
    lazy val qte = QueryToExecute(plan, replyTo, execPlanFunc)
    if (!isLeafPlan) { // schedule immediately since not a leaf plan
      execPlanFunc(plan, replyTo, nonLeafSched)
    } else if (executingOn.isDefined) { // queryId already executing, so schedule right away
      scheduleNow(qte, leafScheduler)
    } else if (executingQueryIdToPlanCount.size < parallelism) { // bandwidth available now, so schedule right away
      scheduleNow(qte, leafScheduler)
    } else { // no free scheduler available, needs to be queued
      val waitingPlans = waitingQueryIdToPlans.getOrElseUpdate(plan.queryContext.queryId, ArrayBuffer.empty)
      waitingPlans += qte
      numWaitingQueryIds.update(waitingQueryIdToPlans.size)
    }
  }

  /**
   * Modifies data structures tracking execution start, and invokes exec function of query.
   * On complete, it sends QueryCompletedScheduleNext to QueryActor.
   */
  private def scheduleNow(q: QueryToExecute, sched: Scheduler) = {
    val currentCount = executingQueryIdToPlanCount.getOrElseUpdate(q.plan.queryContext.queryId, 0)
    executingQueryIdToPlanCount(q.plan.queryContext.queryId) = currentCount + 1
    schedulerAssignmentDelay.record(System.currentTimeMillis() - q.plan.submitTime)
    val f = q.execPlanFunc(q.plan, q.replyTo, sched)
    f.onComplete(_ => queryActor ! QueryCompletedScheduleNext(q.plan.queryContext.queryId, sched))(sched)
  }

  /**
   * Invoked by query actor when it gets notified with QueryCompletedScheduleNext
   * that a plan execution was completed. Data structures tracking execution are mutated.
   * Then next waiting query is scheduled.
   *
   * Note: The data structures in this not-thread-safe class are mutated in the context of query actor only.
   */
  def queryCompletedScheduleNext(qc: QueryCompletedScheduleNext): Unit = {
    val currentCount = executingQueryIdToPlanCount(qc.completedQueryId)
    if (currentCount == 1) { // the last plan with this queryId completed
      executingQueryIdToPlanCount.remove(qc.completedQueryId)
      if (waitingQueryIdToPlans.nonEmpty) { // there are waiting leaf queries
        val (queryId, plans) = waitingQueryIdToPlans.head
        plans.foreach(q => scheduleNow(q, leafScheduler))
        waitingQueryIdToPlans.remove(queryId)
        numWaitingQueryIds.update(waitingQueryIdToPlans.size)
      }
    } else {
      executingQueryIdToPlanCount(qc.completedQueryId) = currentCount - 1
    }
  }

  private def newExceptionHandler(): UncaughtExceptionReporter = new UncaughtExceptionReporter {
    def reportFailure(ex: scala.Throwable): Unit = {
      logger.error("Uncaught Exception in Query Scheduler", ex)
    }
  }
}
