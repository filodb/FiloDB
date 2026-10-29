package filodb.coordinator.flight

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration._
import scala.util.Using

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.trace.Span
import monix.catnap.CircuitBreaker
import monix.eval.Task
import monix.execution.{Callback, Scheduler}
import monix.execution.exceptions.ExecutionRejectedException
import monix.reactive.Observable
import net.ceedubs.ficus.Ficus._
import org.apache.arrow.flight.{CallStatus, FlightProducer, FlightRuntimeException}
import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.memory.{BufferAllocator, OutOfMemoryException}
import org.apache.arrow.vector.{VectorLoader, VectorUnloader}

import filodb.coordinator.QueryScheduler
import filodb.coordinator.flight.ArrowSerializedRangeVectorOps.VsrPopulationState
import filodb.core.QueryTimeoutException
import filodb.core.memstore.FiloSchedulers
import filodb.core.metrics.FilodbMetrics
import filodb.core.query._
import filodb.memory.data.Shutdown
import filodb.query.{BadQueryException, QueryError, QueryResponse, QueryResult}
import filodb.query.exec.ExecPlan

object FlightQueryResultStreaming {
  val ACCEPT_RESPONSE_VERSION1 = "1.0"
}

/**
 * Non-blocking bridge from gRPC flow control to a Monix Task.
 * Must be constructed before `listener.start()` since gRPC does not allow changing the
 * onReady handler once the call has started.
 *
 * `awaitReady` completes immediately if the listener is ready to accept another batch, otherwise it
 * suspends (without parking a thread) until gRPC signals readiness. It fails if the client cancels
 * the stream, or if the query deadline passes while waiting.
 */
private[flight] final class FlightBackpressure(listener: ServerStreamListener, queryContext: QueryContext) {
  // scalastyle:off null
  private val waiter = new AtomicReference[Callback[Throwable, Unit]](null)

  private def cancelledEx = CallStatus.CANCELLED.withDescription("client cancelled stream").toRuntimeException

  private def wakeWaiter(): Unit = {
    val cb = waiter.getAndSet(null)
    if (cb != null) {
      if (listener.isCancelled) cb.onError(cancelledEx) else cb.onSuccess(())
    }
  }
  // scalastyle:on null

  listener.setOnReadyHandler(() => wakeWaiter())
  listener.setOnCancelHandler(() => wakeWaiter())

  val awaitReady: Task[Unit] = Task.suspend {
    if (listener.isCancelled) Task.raiseError(cancelledEx)
    else if (listener.isReady) Task.unit
    else {
      Task.cancelable[Unit] { cb =>
        waiter.set(cb)
        // re-check to avoid a lost wakeup between the isReady check and the registration
        if (listener.isReady || listener.isCancelled) wakeWaiter()
        Task.eval { waiter.compareAndSet(cb, null); () } // scalastyle:ignore null
      }.timeoutTo(
        math.max(queryContext.queryTimeRemaining, 0L).millis,
        Task.defer(Task.raiseError(QueryTimeoutException(
          System.currentTimeMillis() - queryContext.submitTime, classOf[FlightBackpressure].getName)))
      ).asyncBoundary // resume on the default (query) scheduler rather than the gRPC callback thread
    }
  }
}

/**
 * Trait that executes physical plans received over Flight RPC
 * Implements query execution with circuit breaker, metrics, and result streaming
 * to Flight client
 *
 */
trait FlightQueryResultStreaming extends StrictLogging {

  // Enable if debugging. Then certainly remove - It has performance overhead.
  // System.setProperty("arrow.memory.debug.allocator", "true") // allows debugging of memory leaks - look into logs

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

  /**
   * Facade method that executes the physical plan and streams results back to Flight client.
   */
  protected def executePhysicalPlanAndRespond(flightContext: FlightProducer.CallContext,
                                              q: ExecPlan,
                                              listener: ServerStreamListener): Unit = {
    logger.debug(s"Received request to run query $q")
    epRequests.increment(1, Map("dataset" -> q.dataset.toString))
    if (checkTimeoutBeforeQueryExec(listener, q.queryContext)) {
      return
    }
    // must be installed before listener.start()
    val backpressure = new FlightBackpressure(listener, q.queryContext)
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
        FiloSchedulers.assertThreadName(FiloSchedulers.QuerySchedName)
        streamResults(res, flightAllocator, listener, q, querySpan, backpressure)
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
        case oom: OutOfMemoryException =>
          throw oom // rethrow so circuit breaker can handle
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
        // Entire server-side pipeline runs on the query scheduler, just like QueryActor. Nothing here blocks:
        // listener.putNext/putMetadata only enqueue onto gRPC's write queue (Netty does the socket I/O) and
        // slow clients are handled by the non-blocking FlightBackpressure. executeAsync hops off the
        // gRPC executor thread immediately.
        .executeAsync
        .runToFuture(queryScheduler)
    }
  }

  /*
   * Checks if the query has already timed out before starting execution.
   * If it has timed out, sends the timeout error in the response footer and
   * completes the listener.
   * @return true if the query has timed out, false otherwise
   */
  private def checkTimeoutBeforeQueryExec(listener: ServerStreamListener,
                                          queryContext: QueryContext) = {
    val ex = queryContext.checkQueryTimeout(this.getClass.getName, false)
    ex match {
      case Some(qte) => listener.error(qte)
        true
      case None => false
    }
  }

  private def logQueryErrors(t: Throwable, execPlan: ExecPlan): Unit = {
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

  private def streamResults(queryResult: QueryResponse,
                            flightAllocator: FlightAllocator,
                            listener: ServerStreamListener,
                            execPlan: ExecPlan,
                            querySpan: Span,
                            backpressure: FlightBackpressure
                           ): Task[Unit] = {

    // This method runs entirely on the Query scheduler. Serialization of RVs into VSRs and
    // sending them to the listener are interleaved without any thread hops.

    // Order of messages to be sent as part of Flight response:
    // 1. Header: ResultSchema
    // 2. zero or more VSRs containing collection of RVs
    // 3. Footer: QueryStats, RVRange, Throwable (if error)
    queryResult match {
      case qe: QueryError =>
        Task.eval {
          logger.debug(s"Streaming error for queryPlanId=${execPlan.planId}")
          FiloSchedulers.assertThreadName(FiloSchedulers.QuerySchedName)
          // rethrow so circuit breaker can handle
          if (qe.t.isInstanceOf[QueryTimeoutException] || qe.t.isInstanceOf[OutOfMemoryException]) throw qe.t
          sendRespFooterAndComplete(listener, flightAllocator, execPlan, querySpan, qe.queryStats, Some(qe.t))
        }
      case res: QueryResult =>
        Task.eval {
          logger.debug(s"Streaming result for queryPlanId=${execPlan.planId}")
          FiloSchedulers.assertThreadName(FiloSchedulers.QuerySchedName)
          // ownership of metadata buf is now with flight listener and hence not closed here
          flightAllocator.checkAllocatorLimits(execPlan.queryContext)
          logger.debug(s"Sending header for queryPlanId=${execPlan.planId}")
          listener.putMetadata(FlightProtoSerDeser.serializeHeaderToArrowBuf(res.resultSchema, flightAllocator))
        }.flatMap { _ =>
          Task.eval {
            logger.debug(s"Sending RVs for queryPlanId=${execPlan.planId}")
            FiloSchedulers.assertThreadName(FiloSchedulers.QuerySchedName)
            // here we initialize the state object with emoty flightVsr which
            // will be populated when we need to send VSR over the wire as response
            flightAllocator.withRequestAllocator { a =>
              new VsrPopulationState(flightVsr = ArrowSerializedRangeVectorOps.emptyVectorSchemaRoot(a))
            } {
              throw new IllegalStateException("FlightAllocator is already closed, cannot create VectorSchemaRoot")
            }
          }.bracket { state =>
            // Sends all VSRs in state.finishedVsrs over Flight and returns each to the free pool.
            // Each VSR is removed from finishedVsrs BEFORE sending so that, if checkResultBytes (or
            // any step inside) throws, the bracket release finds only cleanly tracked VSRs:
            //   • finishedVsrs — remaining unsent VSRs, closed by the bracket
            //   • freeVsrs     — already sent VSRs, closed by the bracket
            // The VSR that triggered the exception is closed inline in the catch before re-throwing.
            // Each iteration first awaits listener readiness (non-blocking) so batches are not buffered for
            // slow clients. The wait happens BEFORE the VSR is removed from finishedVsrs, so a cancel/timeout
            // while waiting leaves it tracked for the bracket release to close.
            def drainFinishedVsrs(resultSize: Long, label: String): Task[Long] =
              Task.tailRecM(resultSize) { bytes =>
                if (state.finishedVsrs.isEmpty) Task.now(Right(bytes))
                else backpressure.awaitReady.map { _ =>
                  FiloSchedulers.assertThreadName(FiloSchedulers.QuerySchedName)
                  val vsr = state.finishedVsrs.remove(0) // take ownership; no longer in finishedVsrs
                  try {
                    var newBytes = bytes
                    val unloader = new VectorUnloader(vsr)
                    val loader = new VectorLoader(state.flightVsr)
                    Using.resource(unloader.getRecordBatch) { rb =>
                      val vsrBytes = rb.computeBodyLength()
                      newBytes += vsrBytes
                      res.queryStats.getResultBytesCounter(Nil).addAndGet(vsrBytes)
                      execPlan.checkResultBytes(newBytes, queryConfig, res.warnings)
                      loader.load(rb)
                    }
                    logger.debug(s"Putting $label vsr into flight response for " +
                      s"queryPlanId=${execPlan.planId} rowCount=${state.flightVsr.getRowCount} " +
                      s"vectorSize0=${state.flightVsr.getVector(0).getValueCount} " +
                      s"vectorSize1=${state.flightVsr.getVector(1).getValueCount}")
                    listener.putNext()
                    val offered = state.freeVsrs.offer(vsr) // return to free pool for reuse
                    if (!offered) Shutdown.haltAndCatchFire(
                      new IllegalStateException("Failed to add VSR to free pool"))
                    Left(newBytes)
                  } catch {
                    case t: Throwable =>
                      // vsr is not in finishedVsrs or freeVsrs; close here so bracket can't leak it.
                      // Any remaining finishedVsrs items are closed by the bracket release.
                      vsr.close()
                      throw t
                  }
                }
              }

            FiloSchedulers.assertThreadName(FiloSchedulers.QuerySchedName)
            listener.start(state.flightVsr)
            var numResultSamples = 0
            var resultSize = 0L
            if (res.result.forall(rv => rv.isInstanceOf[ArrowSerializedRangeVector] ||
              (rv.isInstanceOf[SerializableRangeVector] &&
                rv.asInstanceOf[SerializableRangeVector].hasFormulatedRows))) {
              logger.debug(s"Applying direct VSR streaming optimization for queryPlanId=${execPlan.planId} " +
                s"since all RVs in result are ArrowSerializedRangeVector2")
              // need to deduplicate to avoid sending the same VSR twice (multiple RVs can point to same VSR)
              // use distinct (preserves order, deduplicates by identity) instead of toSet (reorders)
              val resultVsrs = res.result.iterator.flatMap {
                case asrv: ArrowSerializedRangeVector => asrv.vsrs // only cast ArrowSerializedRangeVector
                case _ => Iterator.empty // skip SRVs like ScalarFixedDouble (they are already in VSRs)
              }.toSeq.distinct
              val samplesScannedConfig = execPlan.queryContext.plannerParams.samplesScannedConfig
              numResultSamples = res.result.foldLeft(0) {
                case (acc, srv: SerializableRangeVector) =>
                  if (samplesScannedConfig.srvSamplesEnabled)
                    QueryUtils.trackSamplesScanned(srv, execPlan.getClass, res.queryStats,
                      res.resultSchema, samplesScannedConfig)
                  acc + srv.numRowsSerialized
                case _ =>
                  throw new IllegalStateException("should not reach here since RVs are all serializable")
              }
              execPlan.checkSamplesLimit(numResultSamples, res.warnings)
              // load each result vsr into the flightVsr and send it over the wire to listener
              // ownership of the VSR is now with flight listener and hence not closed here
              Observable.fromIterable(resultVsrs).mapEval { vsr =>
                // wait (non-blocking) for the client to be ready BEFORE overwriting flightVsr
                backpressure.awaitReady.map { _ =>
                  FiloSchedulers.assertThreadName(FiloSchedulers.QuerySchedName)
                  val unloader = new VectorUnloader(vsr)
                  val loader = new VectorLoader(state.flightVsr)
                  Using.resource(unloader.getRecordBatch) { rb =>
                    val vsrBytes = rb.computeBodyLength()
                    resultSize += vsrBytes
                    res.queryStats.getResultBytesCounter(Nil).addAndGet(vsrBytes)
                    execPlan.checkResultBytes(resultSize, queryConfig, res.warnings)
                    loader.load(rb)
                  }
                  logger.debug(s"Putting arrow vsr directly into flight response queryPlanId=${execPlan.planId} " +
                    s"rowCount=${state.flightVsr.getRowCount} " +
                    s"vectorSize0=${state.flightVsr.getVector(0).getValueCount} " +
                    s"vectorSize1=${state.flightVsr.getVector(1).getValueCount}")

                  listener.putNext()
                }
              }.completedL
            } else {
              val recSchema = res.resultSchema.toRecordSchema
              Observable.fromIterable(res.result).mapEval { rv =>
                if (rv.isInstanceOf[ArrowSerializedRangeVector]) {
                  // Mixed is unexpected but we should still be able to handle it - just log at
                  // debug level since it is not an error. We will continue to serialize them into ASRVs again
                  // to avoid errors. Dont send the VSRs out since it will break the state continuity across VSRs
                  logger.debug(s"Understand why we see SRV and Arrow Results: queryContext=${execPlan.queryContext}")
                }
                state.flightVsr.setRowCount(0) // reset vectors before each RV is loaded, this resets fields counts too
                Task.eval {
                  // This lambda triggers intensive iterators and calculations; runs on query sched
                  FiloSchedulers.assertThreadName(FiloSchedulers.QuerySchedName)
                  flightAllocator.checkAllocatorLimits(execPlan.queryContext)
                  logger.debug(s"Serializing RV into Arrow for queryPlanId=${execPlan.planId} ")
                  val samplesScannedConfig = execPlan.queryContext.plannerParams.samplesScannedConfig
                  if (samplesScannedConfig.srvSamplesEnabled)
                    QueryUtils.trackSamplesScanned(rv, execPlan.getClass, res.queryStats,
                      res.resultSchema, samplesScannedConfig)
                  val samplesForRv = rv match {
                    case srv: SerializableRangeVector => srv.numRowsSerialized
                    case _ => rv.estimateNumRows().toInt
                  }
                  numResultSamples += samplesForRv
                  execPlan.checkSamplesLimit(numResultSamples, res.warnings)
                  flightAllocator.withRequestAllocator { allocator =>
                    ArrowSerializedRangeVectorOps.populateRvContentsIntoVsrs(rv, recSchema,
                      s"${execPlan.queryContext.queryId}:${queryResult.id}", res.queryStats,
                      allocator, state)
                  } {
                    throw new IllegalStateException("FlightAllocator is already closed, cannot populate VSRs")
                  }
                }.flatMap { _ =>
                  drainFinishedVsrs(resultSize, "next").map(resultSize = _)
                }
              }.completedL.flatMap { _ =>
                FiloSchedulers.assertThreadName(FiloSchedulers.QuerySchedName)
                // Move the partially-filled currentVsr into finishedVsrs first so the bracket
                // release can close it if drainFinishedVsrs throws below.
                // scalastyle:off null
                if (state.currentVsr != null) state.finishedVsrs += state.finishAndGetCurrentVsr()
                // scalastyle:on null
                drainFinishedVsrs(resultSize, "last").map(resultSize = _)
              }
            }
          } { state =>
            // This lambda is the finally clause for the bracket: runs on success, error, AND
            // cancellation, so it is the single guaranteed cleanup site for all VSR memory.
            //
            // VSR accounting at this point:
            //   • state.flightVsr    — always allocated; close unconditionally
            //   • state.freeVsrs     — VSRs that were sent and returned to the pool; close all
            //   • state.finishedVsrs — VSRs not yet sent (non-empty only on exception mid-drain)
            //   • state.currentVsr   — partially-filled VSR not yet moved to finishedVsrs
            //                         (non-null only if populateRvContentsIntoVsrs threw before the
            //                          pre-send finalisation ran — safety net)
            FiloSchedulers.assertThreadName(FiloSchedulers.QuerySchedName)
            Task.eval {
              state.flightVsr.close()
              // close any VSRs removed from finishedVsrs mid-drain but not yet in freeVsrs
              // (drainFinishedVsrs catch already closed the one that threw, but any items after
              // it in the ArrayBuffer are still here)
              state.finishedVsrs.foreach(_.close())
              state.finishedVsrs.clear()
              // close VSRs that were successfully sent and returned to the pool
              while (!state.freeVsrs.isEmpty) {
                state.freeVsrs.poll().close()
              }
              // safety net: should always be null here, but close if present to avoid a leak
              // scalastyle:off null
              if (state.currentVsr != null) {
                state.currentVsr.close()
                state.currentVsr = null
              }
              // scalastyle:on null
            }
          }.map { _ =>
            FiloSchedulers.assertThreadName(FiloSchedulers.QuerySchedName)
            sendRespFooterAndComplete(listener, flightAllocator, execPlan,
              querySpan, res.queryStats, None,
              res.mayBePartial, res.partialResultReason, res.warnings)
          }
        }
    }
  }

  // scalastyle:off parameter.number
  private def sendRespFooterAndComplete(listener: ServerStreamListener,
                                        flightAllocator: FlightAllocator,
                                        execPlan: ExecPlan,
                                        queryExecuteSpan: Span,
                                        s: QueryStats,
                                        t: Option[Throwable],
                                        mayBePartial: Boolean = false,
                                        partialResultReason: Option[String] = None,
                                        warnings: QueryWarnings = QueryWarnings()): Unit = {
    t.foreach { e =>
      logQueryErrors(e, execPlan)
      queryExecuteSpan.fail(e.getMessage)
    }
    // dont check allocator limit here since we want to send footer even if soft limit is breached - we have
    // 10% room for precisely this
    // checkAllocatorLimits(flightAllocator, execPlan.queryContext)
    logger.debug(s"Sending response footer for queryPlanId=${execPlan.planId} and completing " +
      s"stream for queryStats=$s, throwable=$t, mayBePartial=$mayBePartial, warnings=$warnings")
    t match {
      case Some(oom: OutOfMemoryException) =>
        // We already know the allocator is exhausted - that's exactly why we're here. Attempting
        // to allocate a footer buffer would almost certainly hit the same OutOfMemoryException
        // again, wasting a retry against an allocator we know is out of room. Skip straight to
        // listener.error(), which needs no further Arrow allocation, so the client gets an
        // immediate, clean failure instead of a doomed allocation attempt.
        logger.warn(s"Skipping response footer allocation for queryPlanId=${execPlan.planId} " +
          s"since the Arrow allocator is already known to be exhausted; completing stream with error", oom)
        listener.error(oom)
      case _ =>
        try {
          // ownership of metadata buf is now with flight listener and hence not closed here
          listener.putMetadata(FlightProtoSerDeser.serializeFooterToArrowBuf(s, t, flightAllocator,
            mayBePartial, partialResultReason, warnings))
          listener.completed()
        } catch {
          case oom: OutOfMemoryException =>
            // The footer buffer allocation itself failed - typically because the shared
            // server/root allocator (not necessarily this request's own per-request allocator)
            // is exhausted by other concurrent requests. Retrying the same allocation here would
            // likely fail again and leave the client blocked in a blocking stream.next() call
            // until its own RPC deadline. Fall back to listener.error(), which does not require
            // any further Arrow buffer allocation, so the client always gets an immediate,
            // clean failure instead of a hang. Query memory is still fully reclaimed by the
            // guaranteed querySession.close() that runs regardless of how this method exits.
            logger.warn(s"Failed to allocate response footer buffer for queryPlanId=${execPlan.planId} " +
              s"due to Arrow allocator exhaustion; completing stream with error instead", oom)
            listener.error(t.getOrElse(oom))
        }
    }
  }

  /**
   * Method that subclasses should implement to decide how to execute the physical plan.
   * Query response will be streamed back to client.
   *
   * @param q
   * @param querySession
   * @return query response
   */
  def executePlan(q: ExecPlan, querySession: QuerySession): Task[QueryResponse]
}
