package filodb.coordinator.flight

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.util.Using

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.apache.arrow.flight.{CallOptions, FlightClient, Location, Ticket}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}

import filodb.core.QueryTimeoutException
import filodb.core.memstore.FiloSchedulers
import filodb.core.memstore.FiloSchedulers.FlightIoSchedName
import filodb.core.query.{ArrowSerializedRangeVector, QuerySession, QueryStats}
import filodb.core.store.ChunkSource
import filodb.query.{QueryError, QueryResponse, QueryResult, StreamQueryResponse}
import filodb.query.exec.{ExecPlanWithClientParams, PlanDispatcher}

object SingleClusterFlightPlanDispatcher {
  private val flightIoScheduler = Scheduler.io(FlightIoSchedName)
}

case class SingleClusterFlightPlanDispatcher(location: Location, clusterName: String)
  extends PlanDispatcher {

  import SingleClusterFlightPlanDispatcher._
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
    if (plan.querySession.queryAllocator.isEmpty)
      throw new IllegalArgumentException("QueryAllocator must be provided in ExecPlanWithClientParams for Flight")
    qLogger.debug(s"FlightPlanDispatcher executing request ${plan.execPlan.getClass.getSimpleName} to $location")
    val client = FlightClientManager.getClient(location)
    val ticket = new Ticket(FlightKryoSerDeser.serializeToBytes(plan.execPlan))
    executeFlightRequest(plan.execPlan.planId, client, ticket, remainingTimeMs, plan.querySession)
  }

  // scalastyle:off method.length
  private def executeFlightRequest(
    planId: String,
    client: FlightClient,
    ticket: Ticket,
    timeoutMs: Long,
    querySession: QuerySession
  ): Task[QueryResponse] = {
    Task.evalAsync {
      require(querySession.queryAllocator.isDefined, "QueryAllocator must be provided in QuerySession for Flight")
      val requestAllocator = querySession.queryAllocator.get
      val srvs = mutable.ListBuffer[ArrowSerializedRangeVector]()
      FiloSchedulers.assertThreadName(FiloSchedulers.FlightIoSchedName)
      Using.resource(client.getStream(ticket, CallOptions.timeout(timeoutMs, TimeUnit.MILLISECONDS))) { stream =>
        var respHeader: Option[RespHeader] = None
        var respFooter: Option[RespFooter] = None
        // Order of messages: ResultSchema, zero or more RVs with metadata, QueryStats, Throwable (if error)
        while (stream.next()) { // this is a blocking call - this is why we run on ioScheduler below
          FlightKryoSerDeser.deserialize(stream.getLatestMetadata) match {
            case header: RespHeader =>
              respHeader = Some(header)
              qLogger.debug(s"FlightPlanDispatcher received RespHeader with schema: ${header.resultSchema}")
            case rvMetadata: RvMetadata =>
              val reqVsr = VectorSchemaRoot.create(ArrowSerializedRangeVector.arrowSrvSchema, requestAllocator)
              querySession.registerArrowCloseable(reqVsr)
              // stream.getRoot is owned by the stream and should not be closed by us
              val root = stream.getRoot
              // move vector data into per-requestAllocator so that it is released when RVs are consumed
              val unloader = new VectorUnloader(root)
              val loader = new VectorLoader(reqVsr)
              Using.resource(unloader.getRecordBatch) { rb =>
                loader.load(rb)
              }
              qLogger.debug(s"FlightPlanDispatcher received RV for RangeVectorKey: ${rvMetadata.rvk}")
              require(respHeader.isDefined, "ResultSchema must be received before RangeVectors")
              val asrv = new ArrowSerializedRangeVector(rvMetadata.rvk, reqVsr.getRowCount,
                respHeader.get.resultSchema.toRecordSchema,
                reqVsr, rvMetadata.rvRange)
              srvs += asrv
              // this ensures that this srv will be closed at the end of the query session
              querySession.registerArrowCloseable(asrv)
            case footer: RespFooter =>
              respFooter = Some(footer)
              qLogger.debug(s"FlightPlanDispatcher received RespFooter with stats: ${footer.queryStats}, " +
                s"throwable: ${footer.throwable}")
          }
          // Error on stream is thrown as exception and will be handled at onErrorHandle below
        }
        if (respFooter.isDefined && respFooter.get.throwable.isDefined) {
          QueryError(planId, respFooter.get.queryStats, respFooter.get.throwable.get)
        } else {
          QueryResult(planId, respHeader.get.resultSchema, srvs, respFooter.map(_.queryStats).getOrElse(QueryStats()))
        }
      }
    }.onErrorHandle { ex =>
      qLogger.error(s"FlightPlanDispatcher - Flight request to $location failed", ex)

      // Attempt to force reconnection on certain errors
      ex match {
        case _: java.net.ConnectException | _: java.io.IOException =>
          qLogger.info(s"FlightPlanDispatcher - Connection error to $location, forcing reconnection")
          FlightClientManager.global.getClient(location, forceRebuild = true)
        case _ =>
      }
      QueryError(planId, QueryStats(), ex)
    }.executeOn(flightIoScheduler).asyncBoundary
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

