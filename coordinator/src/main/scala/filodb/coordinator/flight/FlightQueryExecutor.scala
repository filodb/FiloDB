package filodb.coordinator.flight

import java.util.concurrent.TimeUnit

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.util.Using

import akka.pattern.AskTimeoutException
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.catnap.CircuitBreaker
import monix.eval.Task
import monix.execution.exceptions.ExecutionRejectedException
import net.ceedubs.ficus.Ficus._
import org.apache.arrow.flight.FlightProducer
import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorLoader, VectorUnloader}

import filodb.coordinator.QueryScheduler
import filodb.core.QueryTimeoutException
import filodb.core.memstore.{FiloSchedulers, TimeSeriesStore}
import filodb.core.metrics.FilodbMetrics
import filodb.core.query.{ArrowSerializedRangeVector, FlightAllocator, QueryConfig, QueryContext, QueryLimitException,
                          QuerySession, QueryStats, RangeVector, SerializedRangeVector}
import filodb.core.store.CorruptVectorException
import filodb.query.{BadQueryException, QueryError, QueryResponse, QueryResult}
import filodb.query.exec.{ExecPlan, InProcessPlanDispatcher}

/**
 * Trait that executes physical plans received over Flight RPC
 * Implements query execution with circuit breaker, metrics, and result streaming
 * to Flight client
 *
 */
trait FlightQueryExecutor extends StrictLogging {

  System.setProperty("arrow.memory.debug.allocator", "true") // allows debugging of memory leaks - look into logs

  def memStore: TimeSeriesStore
  def allocator: BufferAllocator
  def sysConfig: com.typesafe.config.Config

  // metric names below are kept same as QueryActor for continuity and easier migration to Flight
  private val epRequests = FilodbMetrics.counter("queryactor-execplan-requests", Map.empty)
  private val queryErrors = FilodbMetrics.counter("queryactor-query-errors", Map.empty)
  private val execPlanLatency = FilodbMetrics.timeHistogram("queryactor-execplan-latency",
    TimeUnit.NANOSECONDS, Map.empty)
  private val numRejectedPlans = FilodbMetrics.counter("circuit-breaker-num-rejected-plans", Map.empty)

  private val perReqAllocatorLimit = sysConfig.getBytes("filodb.flight.server.per-request-allocator-limit")

  private val circuitBreakerEnabled = sysConfig.getBoolean("filodb.query.circuit-breaker.enabled")
  private val circuitBreakerNumFailures = sysConfig.getInt("filodb.query.circuit-breaker.open-when-num-failures")
  private val circuitBreakerResetTimeout = sysConfig.as[FiniteDuration]("filodb.query.circuit-breaker.reset-timeout")
  private val circuitBreakerExpBackOffFactor = sysConfig.getDouble("filodb.query.circuit-breaker.exp-backoff-factor")
  private val circuitBreakerMaxTimeout = sysConfig.as[FiniteDuration]("filodb.query.circuit-breaker.max-reset-timeout")
  private val circuitBreaker = CircuitBreaker[Task].unsafe(
    circuitBreakerNumFailures, circuitBreakerResetTimeout, circuitBreakerExpBackOffFactor, circuitBreakerMaxTimeout,
    onRejected = Task.eval(numRejectedPlans.increment()),
    onClosed = Task.eval(logger.info("Query CircuitBreaker closed")),
    onHalfOpen = Task.eval(logger.info("Query CircuitBreaker is now half-open")),
    onOpen = Task.eval(logger.info("Query CircuitBreaker is now open"))
  )

  private val queryConfig = QueryConfig(sysConfig.getConfig("filodb.query"))
  private val queryScheduler = QueryScheduler.queryScheduler

  // scalastyle:off method.length
  def executePhysicalPlanEntry(flightContext: FlightProducer.CallContext,
                               execPlan: ExecPlan,
                               listener: ServerStreamListener): Unit = {
    execPhysicalPlanInner(execPlan)

    def execPhysicalPlanInner(q: ExecPlan): Unit = {
      logger.debug(s"Received request to run query $q")
      epRequests.increment(1, Map("dataset" -> q.dataset.toString))
      if (checkTimeoutBeforeQueryExec(listener, q.queryContext)) {
        return
      }
      val queryExecuteSpan = Kamon.spanBuilder(s"query-actor-exec-plan-execute-${q.getClass.getSimpleName}")
        .asChildOf(Kamon.currentSpan())
        .start()
      val startTime = System.nanoTime()
      // Dont finish span since we finish it asynchronously when response is received
      Kamon.runWithSpan(queryExecuteSpan, finishSpan = false) {
        queryExecuteSpan.tag("query", q.getClass.getSimpleName)
        queryExecuteSpan.tag("query-id", q.queryContext.queryId)
        val reqAllocator = allocator.newChildAllocator(s"query-flight-producer-req-${q.planId}",
          0, perReqAllocatorLimit)
        val flightAllocator = new FlightAllocator(reqAllocator)
        val querySession = QuerySession(q.queryContext,
          queryConfig,
          flightAllocator = Some(flightAllocator),
          preventRangeVectorSerialization = true, // because we will be serializing as ArrowSRV here
          catchMultipleLockSetErrors = true)
        queryExecuteSpan.mark("query-actor-received-execute-start")

        val execTask = q.execute(memStore, querySession)(queryScheduler)
          .executeOn(queryScheduler)
          .asyncBoundary
          .onErrorHandle { t =>
            QueryError(q.queryContext.queryId, querySession.queryStats, t)
          }.map { res =>
            logger.debug(s"Query execution pipeline constructed for queryPlanId=${q.planId}")
            // Avoiding the assert when the InProcessPlanDispatcher is used. As it runs
            // the query on the current thread instead of the scheduler
            if (!q.dispatcher.isInstanceOf[InProcessPlanDispatcher]) {
              // This check was copied from QueryActor - but why would a
              // plan come here, if InProcessPlanDispatcher is used? Leaving it for now since it is harmless
              FiloSchedulers.assertThreadName(FiloSchedulers.FlightIoSchedName)
            }
            streamResults(res, flightAllocator, listener, q)
            res match {
              case e: QueryError =>
                queryErrors.increment(1, Map("dataset" -> q.dataset.toString))
                queryExecuteSpan.fail(e.t.getMessage)
                // rethrow so circuit beaker can block queries when there is a surge of such exceptions
                if (e.t.isInstanceOf[QueryTimeoutException] || e.t.isInstanceOf[AskTimeoutException]) throw e.t
                else logQueryErrors(e.t, q)
              case _ =>
            }
          }.guarantee(Task.eval {
            SerializedRangeVector.queryCpuTime.increment(querySession.queryStats.totalCpuNanos)
            queryExecuteSpan.finish()
            querySession.close()
            val timeTaken = System.nanoTime() - startTime
            execPlanLatency.record(timeTaken,
              Map("plan" -> q.getClass.getSimpleName, "dataset" -> q.dataset.toString))
          })

        val execTaskWithCircuitBreaker = if (circuitBreakerEnabled) circuitBreaker.protect(execTask)
                                         else execTask
        execTaskWithCircuitBreaker.onErrorRecover { case t =>
            logQueryErrors(t, q)
            querySession.close()
            listener.error(t)
            // all other errors are already handled; listener should already have the error
          }.runToFuture(QueryScheduler.flightIoScheduler)
      }
    }

    /*
     * Checks if the query has already timed out before starting execution.
     * If it has timed out, sends the timeout error in the response footer and
     * completes the listener.
     * @return true if the query has timed out, false otherwise
     */
    def checkTimeoutBeforeQueryExec(listener: ServerStreamListener,
                                    queryContext: QueryContext) = {
      val ex = queryContext.checkQueryTimeout(this.getClass.getName, false)
      ex match {
        case Some(qte) => listener.error(qte)
          true
        case None =>      false
      }
    }

    def logQueryErrors(t: Throwable, execPlan: ExecPlan): Unit = {
      // error logging
      t match {
        case _: BadQueryException => // dont log user errors
        case _: AskTimeoutException => // dont log ask timeouts. useless - let it simply flow up
        case _: QueryTimeoutException | _: ExecutionRejectedException => // log just message, no need for stacktrace
          logger.error(s"Query Error ${t.getClass.getSimpleName} queryId=${execPlan.queryContext.queryId} " +
            s"${execPlan.queryContext.origQueryParams} ${t.getMessage}")
        case _: QueryLimitException =>
          logger.warn(s"Query Limit Breached ${execPlan.queryContext.origQueryParams} ${t.getMessage}")
        case e: Throwable =>
          logger.error(s"Query Error queryId=${execPlan.queryContext.queryId} " +
            s"${execPlan.queryContext.origQueryParams}", e)
      }
      // debug logging
      t match {
        case cve: CorruptVectorException => memStore.analyzeAndLogCorruptPtr(execPlan.dataset, cve)
        case t: Throwable =>
      }
    }

    def streamResults(queryResult: QueryResponse,
                      flightAllocator: FlightAllocator,
                      listener: ServerStreamListener,
                      execPlan: ExecPlan
                     ): Unit = {
      FiloSchedulers.assertThreadName(FiloSchedulers.FlightIoSchedName)
      logger.debug(s"Streaming results for queryPlanId=${execPlan.planId}")
      // Order of messages: ResultSchema, zero or more RVs with metadata, QueryStats, Throwable (if error)
      queryResult match {
        case qe: QueryError =>
          sendRespFooterAndComplete(listener, flightAllocator, execPlan, qe.queryStats, Some(qe.t))
        case res: QueryResult =>
          val respHeader = RespHeader(res.resultSchema)
          // ownership of metadata buf that is the result of serializeToArrowBuf is now with flight listener
          // and hence not closed here
          checkAllocatorLimits(flightAllocator, execPlan.queryContext)
          logger.debug(s"Sending header for queryPlanId=${execPlan.planId}")
          listener.putMetadata(FlightKryoSerDeser.serializeToArrowBuf(respHeader, flightAllocator))
          logger.debug(s"Sending RVs for queryPlanId=${execPlan.planId}")
          val vsr = flightAllocator.withRequestAllocator(a => ArrowSerializedRangeVector.emptyVectorSchemaRoot(a)) {
            throw new IllegalStateException("FlightAllocator is already closed, cannot create VectorSchemaRoot")
          }
          Using.resource(vsr) { vec =>
            listener.start(vec)
            val rb = SerializedRangeVector.newBuilder()
            res.result.foreach { rv =>
              vec.clear()
              val rvMetadata = RvMetadata(rv.key, rv.outputRange)
              rv match {
                case asrv: ArrowSerializedRangeVector =>
                  checkAllocatorLimits(flightAllocator, execPlan.queryContext)
                  val unloader = new VectorUnloader(asrv.vectorSchemaRoot)
                  val loader = new VectorLoader(vec)
                  Using.resource(unloader.getRecordBatch) { rb =>
                    loader.load(rb)
                  }
                  listener.putNext(FlightKryoSerDeser.serializeToArrowBuf(rvMetadata, flightAllocator))
                  asrv.close()
                case rv: RangeVector =>
                  checkAllocatorLimits(flightAllocator, execPlan.queryContext)
                  val fut = Future.apply {
                    // This call triggers the intensive iterators and calculations and should be done on query scheduler
                    FiloSchedulers.assertThreadName(FiloSchedulers.QuerySchedName)
                    logger.debug(s"Serializing RV into Arrow for queryPlanId=${execPlan.planId} ")
                    ArrowSerializedRangeVector.populateVectorSchemaRoot(rv, res.resultSchema.toRecordSchema,
                      vec, s"${execPlan.queryContext.queryId}:${queryResult.id}", rb, res.queryStats)
                    // ownership of metadata buf that is the result of serializeToArrowBuf is now with flight listener
                    // and hence not closed here
                  }(queryScheduler)
                  // We are on an io scheduler (already asserted), so okay to block here
                  // TODO See if we can do better later
                  Await.result(fut, execPlan.queryContext.queryTimeRemaining.millis)
                  listener.putNext(FlightKryoSerDeser.serializeToArrowBuf(rvMetadata, flightAllocator))
              }
            }
            sendRespFooterAndComplete(listener, flightAllocator, execPlan, res.queryStats, None)
            // TODO update query statistics on result bytes and metrics as well
          }
      }
    }

    def sendRespFooterAndComplete(listener: ServerStreamListener,
                                  flightAllocator: FlightAllocator,
                                  execPlan: ExecPlan,
                                  s: QueryStats, t: Option[Throwable]): Unit = {
      checkAllocatorLimits(flightAllocator, execPlan.queryContext)
      logger.debug(s"Sending response footer for queryPlanId=${execPlan.planId} and completing " +
        s"stream for queryStats=$s, throwable=$t")
      val respFooter = RespFooter(s, t)
      // ownership of metadata buf that is the result of serializeToArrowBuf is now with flight listener
      // and hence not closed here
      listener.putMetadata(FlightKryoSerDeser.serializeToArrowBuf(respFooter, flightAllocator))
      listener.completed()
    }

    def checkAllocatorLimits(flightAllocator: FlightAllocator, queryContext: QueryContext): Unit = {
      val used = flightAllocator.allocatedBytes
      val limit = flightAllocator.allocationLimit * 0.9 // 90% of limit
      if (used > limit) {
        val msg = s"Reached $used bytes of result size (final or intermediate) for data serialized out of a host " +
          s"or shard breaching maximum result size limit ($limit bytes)."
        throw QueryLimitException(
          s"$msg Try to apply more filters, reduce the time range, and/or increase the step size.",
          queryContext.queryId
        )
      }
    }
  }
}
