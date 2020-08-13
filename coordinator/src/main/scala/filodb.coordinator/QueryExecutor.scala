package filodb.coordinator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import monix.execution.{Scheduler, UncaughtExceptionReporter}

import filodb.core.DatasetRef
import filodb.core.memstore.FiloSchedulers
import filodb.query.exec.ExecPlan

case class QueryCompletedScheduleNext(completedQueryId: String, sched: Scheduler)

/**
 * NOT THREADSAFE. Should execute in the context of Query Actor ONLY.
 *
 * TODO: Add more docs describing necessity and strategy.
 */
class QueryExecutor(ref: DatasetRef,
                    parallelism: Int,
                    queryActor: ActorRef) extends StrictLogging {

  private case class QueryToExecute(plan: ExecPlan,
                            replyTo: ActorRef,
                            execPlanFunc: (ExecPlan, ActorRef, Scheduler) => Future[Unit])

  /**
   * Map of executing queryIds to the scheduler they are assigned
   */
  private val executingQueryIdToScheduler = new mutable.HashMap[String, Scheduler]()

  /**
   * Tracks number of plans currently executing for each queryId. When the count drops to zero,
   * the scheduler is free.
   */
  private val executingQueryIdToPlanCount = new mutable.HashMap[String, Int]().withDefaultValue(0)

  /**
   * Free scheduler pool. Number of schedulers depends on parallelism passed in.
   */
  private val freeSchedulers = new mutable.Queue[Scheduler]()
  (0 to parallelism).foreach { i =>
    freeSchedulers += Scheduler.singleThread(name = s"${FiloSchedulers.QuerySchedName}-$ref-$i",
      reporter = newExceptionHandler())
  }

  /**
   * Ordered hash map of queryIds waiting to execute
   */
  private val waitingQueryIdToPlans = new mutable.LinkedHashMap[String, ArrayBuffer[QueryToExecute]]()

  /**
   * Non-Leaf scheduler - plans are not queued but executed immediately.
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
    val executingOn = executingQueryIdToScheduler.get(plan.queryContext.queryId)
    val qte = QueryToExecute(plan, replyTo, execPlanFunc)
    if (!isLeafPlan) { // schedule immediately since not a leaf plan
      execPlanFunc(plan, replyTo, nonLeafSched)
    } else if (executingOn.isDefined) { // queryId already executing, so schedule right away
      scheduleNow(qte, executingOn.get)
    } else if (freeSchedulers.nonEmpty) { // free scheduler available now, so schedule right away
      val sched = freeSchedulers.dequeue()
      scheduleNow(qte, sched)
    } else { // no free scheduler available, needs to be queued
      val waitingPlans = waitingQueryIdToPlans.getOrElseUpdate(plan.queryContext.queryId, ArrayBuffer.empty)
      waitingPlans += qte
    }
  }

  /**
   * Modifies data structures tracking execution start, and invokes exec function of query.
   * On complete, it sends QueryCompletedScheduleNext to QueryActor.
   */
  private def scheduleNow(q: QueryToExecute, sched: Scheduler) = {
    val qCount = executingQueryIdToPlanCount(q.plan.queryContext.queryId)
    executingQueryIdToPlanCount(q.plan.queryContext.queryId) = (qCount + 1)
    executingQueryIdToScheduler(q.plan.queryContext.queryId) = sched
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
    val qCount = executingQueryIdToPlanCount(qc.completedQueryId)
    executingQueryIdToPlanCount(qc.completedQueryId) = (qCount - 1)
    if (qCount == 1) { // the last plan with this queryId completed
      executingQueryIdToScheduler.remove(qc.completedQueryId)
      executingQueryIdToPlanCount.remove(qc.completedQueryId)
      scheduleNextWaitingQueryId(qc.sched)
    }
  }

  /**
   * If there is a waiting query, schedule it for execution
   */
  private def scheduleNextWaitingQueryId(sched: Scheduler): Unit = {
    if (waitingQueryIdToPlans.nonEmpty) {
      val (queryId, plans) = waitingQueryIdToPlans.head
      plans.foreach(q => scheduleNow(q, sched))
      waitingQueryIdToPlans.remove(queryId)
    } else {
      freeSchedulers += sched
    }
  }

  private def newExceptionHandler(): UncaughtExceptionReporter = new UncaughtExceptionReporter {
    def reportFailure(ex: scala.Throwable): Unit = {
      logger.error("Uncaught Exception in Query Scheduler", ex)
    }
  }
}
