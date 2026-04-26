package filodb.coordinator.flight

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration
import scala.util.Using

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.trace.Span
import monix.catnap.CircuitBreaker
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.exceptions.ExecutionRejectedException
import monix.reactive.Observable
import net.ceedubs.ficus.Ficus._
import org.apache.arrow.flight.{CallStatus, FlightProducer, FlightRuntimeException}
import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorLoader, VectorUnloader}

import filodb.coordinator.QueryScheduler
import filodb.coordinator.flight.ArrowSerializedRangeVectorOps.VsrPopulationState
import filodb.core.QueryTimeoutException
import filodb.core.binaryrecord2.BinaryRecordRowReader
import filodb.core.binaryrecord2.RecordContainer.BRIterator
import filodb.core.memstore.FiloSchedulers
import filodb.core.metrics.FilodbMetrics
import filodb.core.query._
import filodb.query.{BadQueryException, QueryError, QueryResponse, QueryResult}
import filodb.query.exec.ExecPlan

/**
 * Trait that executes physical plans received over Flight RPC
 * Implements query execution with circuit breaker, metrics, and result streaming
 * to Flight client
 *
 */
trait FlightQueryResultStreaming extends StrictLogging {

  // FIXME enable debugging for now until we stabilize and productionize. Then remove.
  //  It has performance overhead.
  System.setProperty("arrow.memory.debug.allocator", "true") // allows debugging of memory leaks - look into logs

  def serverAllocator: BufferAllocator
  def sysConfig: com.typesafe.config.Config

  // metric names below are kept same as QueryActor for continuity and easier migration to Flight
  private val epRequests = FilodbMetrics.counter("queryactor-execplan-requests", Map.empty)
  private val queryErrors = FilodbMetrics.counter("queryactor-query-errors", Map.empty)
  private val execPlanLatency = FilodbMetrics.timeHistogram("queryactor-execplan-latency",
    TimeUnit.NANOSECONDS, Map.empty)
  private val numRejectedPlans = FilodbMetrics.counter("circuit-breaker-num-rejected-plans", Map.empty)

  private val perReqAllocatorLimit = sysConfig.getBytes("filodb.flight.server.per-request-allocator-limit")

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
  protected val queryScheduler: Scheduler = QueryScheduler.queryScheduler

  // scalastyle:off method.length
  def executePhysicalPlanAndRespond(flightContext: FlightProducer.CallContext,
                                    execPlan: ExecPlan,
                                    listener: ServerStreamListener): Unit = {
    execPhysicalPlanInner(execPlan)

    def execPhysicalPlanInner(q: ExecPlan): Unit = {
      logger.debug(s"Received request to run query $q")
      epRequests.increment(1, Map("dataset" -> q.dataset.toString))
      if (checkTimeoutBeforeQueryExec(listener, q.queryContext)) {
        return
      }
      val querySpan = Kamon.spanBuilder(s"query-actor-exec-plan-execute-${q.getClass.getSimpleName}")
        .asChildOf(Kamon.currentSpan())
        .start()
      val startTime = System.nanoTime()
      // Dont finish span since we finish it asynchronously when response is received
      Kamon.runWithSpan(querySpan, finishSpan = false) {
        querySpan.tag("query", q.getClass.getSimpleName)
        querySpan.tag("query-id", q.queryContext.queryId)
        val reqAllocator = serverAllocator.newChildAllocator(s"query-flight-producer-req-${q.planId}",
          0, perReqAllocatorLimit)
        val flightAllocator = new FlightAllocator(reqAllocator)
        val querySession = QuerySession(q.queryContext,
          queryConfig,
          flightAllocator = Some(flightAllocator),
          preventRangeVectorSerialization = true, // because we will be serializing as ArrowSRV here
          catchMultipleLockSetErrors = true)
        querySpan.mark("query-actor-received-execute-start")

        val execTask = executePlan(q, querySession).flatMap { res =>
          logger.debug(s"Query execution pipeline constructed by Flight producer for queryPlanId=${q.planId}")
          FiloSchedulers.assertThreadName(FiloSchedulers.FlightIoSchedName)
          streamResults(res, flightAllocator, listener, q, querySpan)
        }.onErrorRecover {
            // if for any reason streamResults thew exception, do it here
            case f: FlightRuntimeException =>
              if (f.status() == CallStatus.TIMED_OUT) {
                throw QueryTimeoutException(System.currentTimeMillis() - q.queryContext.submitTime,
                                              this.getClass.getName, Some(f))
              } // convert to QueryTimeoutException for circuit breaker handling
              sendRespFooterAndComplete(listener, flightAllocator, q, querySpan,
                                        querySession.queryStats, Some(f))
            case qte: QueryTimeoutException =>
              throw qte // rethrow so circuit breaker can handle
            case e =>
              sendRespFooterAndComplete(listener, flightAllocator, q, querySpan,
                                        querySession.queryStats, Some(e))
        }
        circuitBreaker.protect(execTask).onErrorRecover { case t =>
          // typically a logged QueryTimeout will be thrown here; all other errors are already handled above
          sendRespFooterAndComplete(listener, flightAllocator, q, querySpan, querySession.queryStats, Some(t))
        }.guarantee(Task.eval {
            SerializedRangeVector.queryCpuTime.increment(querySession.queryStats.totalCpuNanos)
            val timeTaken = System.nanoTime() - startTime
            execPlanLatency.record(timeTaken,
              Map("plan" -> q.getClass.getSimpleName, "dataset" -> q.dataset.toString))
            querySpan.finish()
          querySession.close()
        })
        .runToFuture(QueryScheduler.flightIoScheduler)
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
      queryErrors.increment(1, Map("dataset" -> execPlan.dataset.toString))
      // error logging
      t match {
        case _: BadQueryException => // dont log user errors
        case _: QueryTimeoutException | _: ExecutionRejectedException => // log just message, no need for stacktrace
          logger.error(s"Query Error ${t.getClass.getSimpleName} queryId=${execPlan.queryContext.queryId} " +
            s"${execPlan.queryContext.origQueryParams} ${t.getMessage}")
        case _: QueryLimitException =>
          logger.warn(s"Query Limit Breached ${execPlan.queryContext.origQueryParams} ${t.getMessage}")
        case e: Throwable =>
          logger.error(s"Query Error queryId=${execPlan.queryContext.queryId} " +
            s"${execPlan.queryContext.origQueryParams}", e)
      }
//      // debug logging
//      t match {
//        case cve: CorruptVectorException => memStore.analyzeAndLogCorruptPtr(execPlan.dataset, cve)
//        case t: Throwable =>
//      }
    }

    def streamResults(queryResult: QueryResponse,
                      flightAllocator: FlightAllocator,
                      listener: ServerStreamListener,
                      execPlan: ExecPlan,
                      querySpan: Span
                     ): Task[Unit] = {

      // This method has portions that need to run on Flight IO scheduler and portions that need to run
      // on Query scheduler. The portions that run on Query scheduler are marked clearly.

      // Order of messages to be sent as part of Flight response:
      // 1. Header: ResultSchema
      // 2. zero or more VSRs containing collection of RVs
      // 3. Footer: QueryStats, RVRange, Throwable (if error)
      queryResult match {
        case qe: QueryError =>
          Task.eval {
            logger.debug(s"Streaming error for queryPlanId=${execPlan.planId}")
            FiloSchedulers.assertThreadName(FiloSchedulers.FlightIoSchedName)
            // rethrow so circuit breaker can handle
            if (qe.t.isInstanceOf[QueryTimeoutException]) throw qe.t
            sendRespFooterAndComplete(listener, flightAllocator, execPlan, querySpan, qe.queryStats, Some(qe.t))
          }
        case res: QueryResult =>
          Task.eval {
            logger.debug(s"Streaming result for queryPlanId=${execPlan.planId}")
            FiloSchedulers.assertThreadName(FiloSchedulers.FlightIoSchedName)
            val respHeader = RespHeader(res.resultSchema)
            // ownership of metadata buf that is the result of serializeToArrowBuf is now with flight listener
            // and hence not closed here
            flightAllocator.checkAllocatorLimits(execPlan.queryContext)
            logger.debug(s"Sending header for queryPlanId=${execPlan.planId}")
            listener.putMetadata(FlightKryoSerDeser.serializeToArrowBuf(respHeader, flightAllocator))
          }.flatMap { _ =>
            Task.eval {
              logger.debug(s"Sending RVs for queryPlanId=${execPlan.planId}")
              FiloSchedulers.assertThreadName(FiloSchedulers.FlightIoSchedName)
              // here we initialize the state object with emoty flightVsr which
              // will be populated when we need to send VSR over the wire as response
              flightAllocator.withRequestAllocator { a =>
                VsrPopulationState(flightVsr = ArrowSerializedRangeVectorOps.emptyVectorSchemaRoot(a))
              } {
                throw new IllegalStateException("FlightAllocator is already closed, cannot create VectorSchemaRoot")
              }
            }.bracket { state =>
              FiloSchedulers.assertThreadName(FiloSchedulers.FlightIoSchedName)
              listener.start(state.flightVsr)
              val rb = SerializedRangeVector.newBuilder()
              if (res.result.forall(rv => rv.isInstanceOf[ArrowSerializedRangeVector] ||
                                            (rv.isInstanceOf[SerializableRangeVector] &&
                                            rv.asInstanceOf[SerializableRangeVector].hasFormulatedRows))) {
                logger.debug(s"Applying direct VSR streaming optimization for queryPlanId=${execPlan.planId} " +
                  s"since all RVs in result are ArrowSerializedRangeVector2")
                // need to deduplicate to avoid sending the same VSR twice (multiple RVs can point to same VSR)
                // use distinct (preserves order, deduplicates by identity) instead of toSet (reorders)
                val resultVsrs = res.result.iterator.flatMap {
                  case asrv: ArrowSerializedRangeVector => asrv.vsrs  // only cast ArrowSerializedRangeVector
                  case _ => Iterator.empty  // skip SRVs like ScalarFixedDouble (they are already in VSRs)
                }.toSeq.distinct
                Observable.fromIterable(resultVsrs).map { vsr =>
                  val unloader = new VectorUnloader(vsr)
                  val loader = new VectorLoader(state.flightVsr)
                  Using.resource(unloader.getRecordBatch) { rb =>
                    loader.load(rb)
                  }
                  logger.debug(s"Putting arrow vsr directly into flight response queryPlanId=${execPlan.planId} " +
                    s"rowCount=${state.flightVsr.getRowCount} " +
                    s"vectorSize0=${state.flightVsr.getVector(0).getValueCount} " +
                    s"vectorSize1=${state.flightVsr.getVector(1).getValueCount}")

                  listener.putNext()
                }.completedL
              } else {
                val recSchema = res.resultSchema.toRecordSchema
                val brIterator = new BRIterator(new BinaryRecordRowReader(recSchema))
                Observable.fromIterable(res.result).mapEval { rv =>
                  if (rv.isInstanceOf[ArrowSerializedRangeVector]) {
                    // Mixed is unexpected but we should still be able to handle it - just log at
                    // debug level since it is not an error. We will continue to serialize them into ASRVs again
                    // to avoid errors. Dont send the VSRs out since it will break the state continuity across VSRs
                    logger.debug(s"Understand why we see SRV and Arrow Results: queryContext=${execPlan.queryContext}")
                  }
                  state.flightVsr.getFieldVectors.forEach(_.setValueCount(0)) // reset vectors before each RV is loaded
                  state.flightVsr.setRowCount(0)
                  Task.eval {
                    // This lambda triggers intensive iterators and calculations and should be done on query sched
                    FiloSchedulers.assertThreadName(FiloSchedulers.QuerySchedName)
                    flightAllocator.checkAllocatorLimits(execPlan.queryContext)
                    logger.debug(s"Serializing RV into Arrow for queryPlanId=${execPlan.planId} ")
                    flightAllocator.withRequestAllocator { allocator =>
                      ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(rv, recSchema,
                        s"${execPlan.queryContext.queryId}:${queryResult.id}", rb, res.queryStats,
                        allocator, state, brIterator)
                    } {
                      throw new IllegalStateException("FlightAllocator is already closed, cannot populate VSRs")
                    }
                  }.executeOn(queryScheduler).asyncBoundary.map { _ =>
                    FiloSchedulers.assertThreadName(FiloSchedulers.FlightIoSchedName)
                    // unload each finishedVsr into vec and putNext to listener
                    state.finishedVsrs.foreach { vsr =>
                      val unloader = new VectorUnloader(vsr)
                      val loader = new VectorLoader(state.flightVsr)
                      Using.resource(unloader.getRecordBatch) { rb =>
                        loader.load(rb)
                      }
                      logger.debug(s"Putting next vsr into flight response for " +
                        s"queryPlanId=${execPlan.planId} rowCount=${state.flightVsr.getRowCount} " +
                        s"vectorSize0=${state.flightVsr.getVector(0).getValueCount} " +
                        s"vectorSize1=${state.flightVsr.getVector(1).getValueCount}")
                      listener.putNext()
                      state.freeVsrs.offer(vsr) // add to free pool after sending over flight
                    }
                    state.finishedVsrs.clear() // clear finished list after sending all over flight
                  }
                }.completedL.map { _ =>
                  FiloSchedulers.assertThreadName(FiloSchedulers.FlightIoSchedName)
                  if (state.currentVsr != null) {
                    val lastVsr = state.finishAndGetCurrentVsr()
                    val unloader = new VectorUnloader(lastVsr)
                    val loader = new VectorLoader(state.flightVsr)
                    Using.resource(unloader.getRecordBatch) { rb =>
                      loader.load(rb)
                    }
                    logger.debug(s"Putting last vsr into flight response for " +
                      s"queryPlanId=${execPlan.planId} rowCount=${state.flightVsr.getRowCount} " +
                      s"vectorSize0=${state.flightVsr.getVector(0).getValueCount} " +
                      s"vectorSize1=${state.flightVsr.getVector(1).getValueCount}")
                    listener.putNext()
                    state.freeVsrs.offer(lastVsr) // add to free pool after sending over flight
                  }
                }
              }
            } { state =>
              // this lambda is the finally clause for bracket method
              FiloSchedulers.assertThreadName(FiloSchedulers.FlightIoSchedName)
              Task.eval {
                state.flightVsr.close()
                while (!state.freeVsrs.isEmpty) {
                  state.freeVsrs.poll().close()
                }
              }
            }.map { _ =>
              FiloSchedulers.assertThreadName(FiloSchedulers.FlightIoSchedName)
              sendRespFooterAndComplete(listener, flightAllocator, execPlan,
                                        querySpan, res.queryStats, None)
            }
          }
      }
    }

    def sendRespFooterAndComplete(listener: ServerStreamListener,
                                  flightAllocator: FlightAllocator,
                                  execPlan: ExecPlan,
                                  queryExecuteSpan: Span,
                                  s: QueryStats,
                                  t: Option[Throwable]): Unit = {
      t.foreach { e =>
        logQueryErrors(e, execPlan)
        queryExecuteSpan.fail(e.getMessage)
      }
      // dont check allocator limit here since we want to send footer even if soft limit is breached - we have
      // 10% room for precisely this
      // checkAllocatorLimits(flightAllocator, execPlan.queryContext)
      logger.debug(s"Sending response footer for queryPlanId=${execPlan.planId} and completing " +
        s"stream for queryStats=$s, throwable=$t")
      val respFooter = RespFooter(s, t)
      // ownership of metadata buf that is the result of serializeToArrowBuf is now with flight listener
      // and hence not closed here
      listener.putMetadata(FlightKryoSerDeser.serializeToArrowBuf(respFooter, flightAllocator))
      listener.completed()
    }
  }

  /**
   * Method that subclasses should implement to decide how to execute the physical plan.
   * Query response will be streamed back to client.
   * @param q
   * @param querySession
   * @return query response
   */
  def executePlan(q: ExecPlan, querySession: QuerySession): Task[QueryResponse]
}
