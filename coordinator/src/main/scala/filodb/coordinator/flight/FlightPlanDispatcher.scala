package filodb.coordinator.flight

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.util.Using

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.apache.arrow.flight._
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}

import filodb.coordinator.QueryScheduler
import filodb.core.QueryTimeoutException
import filodb.core.memstore.FiloSchedulers
import filodb.core.query.{QuerySession, QueryStats, ResultSchema}
import filodb.core.store.ChunkSource
import filodb.query.{QueryError, QueryResponse, QueryResult, StreamQueryResponse}
import filodb.query.ProtoConverters._
import filodb.query.exec.{ExecPlan, ExecPlanWithClientParams, PlanDispatcher}

case class FlightPlanDispatcher(location: Location,
                                clusterName: String)
  extends PlanDispatcher {

  import filodb.query.Query.qLogger

  override def dispatch(plan: ExecPlanWithClientParams, source: ChunkSource)
                       (implicit sched: Scheduler): Task[QueryResponse] = {
    // Check remaining time similar to GrpcPlanDispatcher
    val queryTimeElapsed = System.currentTimeMillis() - plan.execPlan.queryContext.submitTime
    val remainingTime = plan.clientParams.deadlineMs - queryTimeElapsed

    // Don't send if time left is very small
    if (remainingTime < 1) {
      Task.raiseError(QueryTimeoutException(queryTimeElapsed, this.getClass.getName))
    } else {
      dispatchFlightPlan(plan, remainingTime)
    }
  }

  private def dispatchFlightPlan(
    plan: ExecPlanWithClientParams,
    remainingTimeMs: Long): Task[QueryResponse] = {
    qLogger.debug(s"FlightPlanDispatcher dispatching queryPlanId=${plan.execPlan.planId} " +
      s"${plan.execPlan.getClass.getSimpleName} to $location")
    val client = FlightClientManager.getClient(location)

    val ticket = plan.execPlan match {
      case remoteExec: PromQLFlightRemoteExec =>  new Ticket(remoteExec.grpcRequest.toByteArray)
      case otherExec =>                           new Ticket(FlightKryoSerDeser.serializeToBytes(otherExec))
    }
    executeFlightRequest(plan.execPlan, client, ticket, remainingTimeMs, plan.querySession)
  }

  // scalastyle:off method.length
  private def executeFlightRequest(
    plan: ExecPlan,
    client: FlightClient,
    ticket: Ticket,
    timeoutMs: Long,
    querySession: QuerySession
  ): Task[QueryResponse] = {
    Task.evalAsync {
      require(querySession.flightAllocator.isDefined, "FlightAllocator must be" +
                 " provided in QuerySession when enabling Flight dispatcher")
      val flightAllocator = querySession.flightAllocator.get
      FiloSchedulers.assertThreadName(FiloSchedulers.FlightIoSchedName)
      Using.resource(client.getStream(ticket, CallOptions.timeout(timeoutMs, TimeUnit.MILLISECONDS))) { stream =>
        var resultSchema: Option[ResultSchema] = None
        var footerStats: Option[QueryStats] = None
        var footerThrowable: Option[Throwable] = None
        val vsrs = mutable.ListBuffer[VectorSchemaRoot]()
        var canceled = false
        // Order of messages: ResultSchema, zero or more RVs with metadata, QueryStats, Throwable (if error)
        while (!canceled && stream.next()) { // next is a blocking call - this is why we run on ioScheduler
          if (stream.getLatestMetadata == null) {
            flightAllocator.withRequestAllocator { requestAllocator =>
              flightAllocator.checkAllocatorLimits(plan.queryContext)
              val reqVsr = VectorSchemaRoot.create(ArrowSerializedRangeVectorOps.arrowSrvSchema, requestAllocator)
              flightAllocator.registerCloseable(reqVsr)
              // stream.getRoot is owned by the stream and should not be closed by us
              val root = stream.getRoot
              // move vector data into per-requestAllocator so that it is released when RVs are consumed
              val unloader = new VectorUnloader(root)
              val loader = new VectorLoader(reqVsr)
              Using.resource(unloader.getRecordBatch) { rb =>
                loader.load(rb)
              }
              qLogger.debug(s"Got next vsr from flight response for vsrHash=${reqVsr.hashCode()}" +
                s"queryPlanId=${plan.planId} rowCount=${reqVsr.getRowCount} " +
                s"vectorSize0=${reqVsr.getVector(0).getValueCount} " +
                s"vectorSize1=${reqVsr.getVector(1).getValueCount}")

              require(resultSchema.isDefined, "ResultSchema must be received before RangeVectors")
              vsrs += reqVsr
              qLogger.debug(s"FlightPlanDispatcher received VSR planId=${plan.planId}")
            } {
              // scalastyle:off null
              stream.cancel("Cancelling due to closed FlightAllocator", null)
              // scalastyle:on null
              canceled = true
            }
          } else {
            val meta = FlightProtoSerDeser.deserializeMetadata(stream.getLatestMetadata)
            if (meta.hasHeader) {
              resultSchema = Some(meta.getHeader.getResultSchema.fromProto)
              qLogger.debug(s"FlightPlanDispatcher received header for queryPlanId=${plan.planId} " +
                s"with schema: ${resultSchema.get}")
            } else if (meta.hasFooter) {
              val footer = meta.getFooter
              footerStats = Some(footer.getQueryStats.fromProto)
              footerThrowable = if (footer.hasThrowable) Some(footer.getThrowable.fromProto) else None
              qLogger.debug(s"FlightPlanDispatcher received footer for queryPlanId=${plan.planId} with stats: " +
                s"${footerStats.get}, throwable: $footerThrowable")
            } else {
              qLogger.warn(s"FlightPlanDispatcher received metadata with unknown type for queryPlanId=${plan.planId}")
            }
          }
          // Error on stream is thrown as exception and will be handled at onErrorHandle below
        }
        if (canceled) {
          QueryError(plan.queryContext.queryId, QueryStats(), new RuntimeException("FlightAllocator closed"))
        } else if (footerThrowable.isDefined) {
          QueryError(plan.queryContext.queryId, footerStats.getOrElse(QueryStats()), footerThrowable.get)
        } else {
          val srvs = ArrowSerializedRangeVectorOps.convertVsrsIntoArrowSrvs(vsrs.toSeq, resultSchema.get)
          QueryResult(plan.queryContext.queryId, resultSchema.get, srvs,
            footerStats.getOrElse(QueryStats()))
        }
      }
    }.onErrorHandle { ex =>
      qLogger.error(s"FlightPlanDispatcher - Flight request for queryId=${plan.queryContext.queryId}" +
        s" to $location failed", ex)
      // Attempt to force reconnection on certain errors
      ex match {
        case _: java.net.ConnectException | _: java.io.IOException =>
          qLogger.info(s"FlightPlanDispatcher - Connection error to $location, forcing reconnection")
          FlightClientManager.global.getClient(location, forceRebuild = true)
        case f: FlightRuntimeException => if (f.status() == CallStatus.TIMED_OUT) {
              throw QueryTimeoutException(System.currentTimeMillis() - plan.queryContext.submitTime,
                                          this.getClass.getName, Some(f))
            } // convert to QueryTimeoutException for circuit breaker handling
        case _ =>
      }
      QueryError(plan.queryContext.queryId, QueryStats(), ex)
    }.executeOn(QueryScheduler.flightIoScheduler).asyncBoundary
  }

  def dispatchStreaming(plan: ExecPlanWithClientParams, source: ChunkSource)
                       (implicit sched: Scheduler): Observable[StreamQueryResponse] = {
    // TODO: Implement streaming dispatch when needed
    // This would follow similar pattern but return Observable instead of Task
    qLogger.warn("Streaming dispatch not yet implemented for FlightPlanDispatcher")
    ???
  }

  def isLocalCall: Boolean = false

}

