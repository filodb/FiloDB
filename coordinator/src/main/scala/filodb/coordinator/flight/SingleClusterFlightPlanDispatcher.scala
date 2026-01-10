package filodb.coordinator.flight

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.util.Using

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.apache.arrow.flight.{CallOptions, FlightClient, Location, Ticket}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}

import filodb.coordinator.QueryScheduler
import filodb.core.QueryTimeoutException
import filodb.core.memstore.FiloSchedulers
import filodb.core.query.{ArrowSerializedRangeVector, QueryContext, QueryLimitException, QuerySession, QueryStats}
import filodb.core.store.ChunkSource
import filodb.query.{QueryError, QueryResponse, QueryResult, StreamQueryResponse}
import filodb.query.exec.{ExecPlan, ExecPlanWithClientParams, PlanDispatcher}

case class SingleClusterFlightPlanDispatcher(location: Location, clusterName: String)
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
    val ticket = new Ticket(FlightKryoSerDeser.serializeToBytes(plan.execPlan))
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
      val srvs = mutable.ListBuffer[ArrowSerializedRangeVector]()
      FiloSchedulers.assertThreadName(FiloSchedulers.FlightIoSchedName)
      Using.resource(client.getStream(ticket, CallOptions.timeout(timeoutMs, TimeUnit.MILLISECONDS))) { stream =>
        var respHeader: Option[RespHeader] = None
        var respFooter: Option[RespFooter] = None
        var canceled = false
        // Order of messages: ResultSchema, zero or more RVs with metadata, QueryStats, Throwable (if error)
        while (canceled || stream.next()) { // next is a blocking call - this is why we run on ioScheduler
          FlightKryoSerDeser.deserialize(stream.getLatestMetadata) match {
            case header: RespHeader =>
              respHeader = Some(header)
              qLogger.debug(s"FlightPlanDispatcher received RespHeader for queryPlanId=${plan.planId} " +
                s"with schema: ${header.resultSchema}")
            case rvMetadata: RvMetadata =>
              flightAllocator.withRequestAllocator { requestAllocator =>
                checkAllocatorLimits(requestAllocator, plan.queryContext)
                val reqVsr = VectorSchemaRoot.create(ArrowSerializedRangeVector.arrowSrvSchema, requestAllocator)
                flightAllocator.registerCloseable(reqVsr)
                // stream.getRoot is owned by the stream and should not be closed by us
                val root = stream.getRoot
                // move vector data into per-requestAllocator so that it is released when RVs are consumed
                val unloader = new VectorUnloader(root)
                val loader = new VectorLoader(reqVsr)
                Using.resource(unloader.getRecordBatch) { rb =>
                  loader.load(rb)
                }
                qLogger.debug(s"FlightPlanDispatcher received RV planId=${plan.planId} with Key: ${rvMetadata.rvk}")
                require(respHeader.isDefined, "ResultSchema must be received before RangeVectors")
                val asrv = new ArrowSerializedRangeVector(rvMetadata.rvk, reqVsr.getRowCount,
                  respHeader.get.resultSchema.toRecordSchema,
                  reqVsr, rvMetadata.rvRange)
                srvs += asrv
                // this ensures that this srv will be closed at the end of the query session
                flightAllocator.registerCloseable(asrv)
              } {
                // scalastyle:off null
                stream.cancel("Cancelling due to closed FlightAllocator", null)
                // scalastyle:on null
                canceled = true
              }
            case footer: RespFooter =>
              respFooter = Some(footer)
              qLogger.debug(s"FlightPlanDispatcher received RespFooter for queryPlanId=${plan.planId} with stats: " +
                s"${footer.queryStats}, throwable: ${footer.throwable}")
          }
          // Error on stream is thrown as exception and will be handled at onErrorHandle below
        }
        if (canceled) {
          QueryError(plan.queryContext.queryId, QueryStats(), new RuntimeException("FlightAllocator closed"))
        } else if (respFooter.isDefined && respFooter.get.throwable.isDefined) {
          QueryError(plan.queryContext.queryId, respFooter.get.queryStats, respFooter.get.throwable.get)
        } else {
          QueryResult(plan.queryContext.queryId, respHeader.get.resultSchema, srvs,
            respFooter.map(_.queryStats).getOrElse(QueryStats()))
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

  def checkAllocatorLimits(reqAllocator: BufferAllocator, queryContext: QueryContext): Unit = {
    val used = reqAllocator.getAllocatedMemory
    val limit = reqAllocator.getLimit * 0.9 // 90% of limit
    if (used > limit) {
      val msg = s"Reached $used bytes of result size (final or intermediate) " +
        s"for data serialized out of a host or shard breaching maximum result size limit" +
        s"($limit bytes)."
      throw QueryLimitException(
        s"$msg Try to apply more filters, reduce the time range, and/or increase the step size.",
        queryContext.queryId
      )
    }
  }
}

