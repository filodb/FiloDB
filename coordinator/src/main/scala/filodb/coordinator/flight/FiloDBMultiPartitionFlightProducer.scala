package filodb.coordinator.flight

import java.util
import java.util.{Collections, Optional}
import java.util.concurrent.ExecutorService

import com.typesafe.config.Config
import io.grpc.BindableService
import monix.eval.Task
import org.apache.arrow.flight._
import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.flight.auth.ServerAuthHandler
import org.apache.arrow.memory.BufferAllocator

import filodb.coordinator.queryplanner.QueryPlanner
import filodb.core.query.{QueryContext, QuerySession}
import filodb.grpc.GrpcMultiPartitionQueryService
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.ProtoConverters.{PlannerParamsFromProtoConverter, QueryParamsFromProtoConversion}
import filodb.query.QueryResponse
import filodb.query.exec.{ClientParams, ExecPlan, ExecPlanWithClientParams, UnsupportedChunkSource}

/**
 * FiloDB Flight Producer for multi-partition queries - serves Flight RPCs for FiloDB multi-partition queries
 * It extends FlightQueryExecutor to execute PromQL queries and stream results back to client.
 * @param queryPlannerSelector selects a query planner
 * @param serverAllocator allocator for Flight buffers
 * @param location location advertised to clients for where to connect for flight RPCs. Not used during invocation now.
 * @param sysConfig system config
 */
class FiloDBMultiPartitionFlightProducer(
                                    val queryPlannerSelector: String => QueryPlanner,
                                    val serverAllocator: BufferAllocator,
                                    val location: Location,
                                    val sysConfig: Config) extends NoOpFlightProducer with FlightQueryResultStreaming {

  override def listActions(context: FlightProducer.CallContext,
                           listener: FlightProducer.StreamListener[ActionType]): Unit = {
    // empty for now since this is only for reads, no updates or actions supported
    listener.onCompleted()
  }

  override def listFlights(context: FlightProducer.CallContext, criteria: Criteria,
                           listener: FlightProducer.StreamListener[FlightInfo]): Unit = {
    // empty for now - we dont support listing flights since we only support Command FlightDescriptors
    listener.onCompleted()
  }

  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
    if (!descriptor.isCommand) {
      throw new UnsupportedOperationException("Only Command FlightDescriptors are supported")
    } else {
      val flightEndpoint = new FlightEndpoint(new Ticket(descriptor.getCommand), location)
      new FlightInfo(ArrowSerializedRangeVectorOps.arrowSrvSchema,
        descriptor, Collections.singletonList(flightEndpoint), -1, -1)
    }
  }

  def executePlan(execPlan: ExecPlan, querySession: QuerySession): Task[QueryResponse] = {
    // UnsupportedChunkSource because leaf plans shouldn't execute in-process from a planner method call.
    execPlan.dispatcher.dispatch(ExecPlanWithClientParams(execPlan,
      ClientParams(execPlan.queryContext.plannerParams.queryTimeoutMillis),
      querySession), UnsupportedChunkSource())(queryScheduler)
  }

  /**
   * Handle doGet requests - execute query plan and stream results
   */
  // scalastyle:off method.length
  override def getStream(context: FlightProducer.CallContext,
                         ticket: Ticket,
                         listener: ServerStreamListener): Unit = {

    try {
      val request = GrpcMultiPartitionQueryService.Request.parseFrom(ticket.getBytes)
      val queryParams = request.getQueryParams
      val qContext = QueryContext(origQueryParams = request.getQueryParams.fromProto,
        plannerParams = request.getPlannerParams.fromProto)
      val queryPlanner = queryPlannerSelector(request.getPlannerSelector)
      // Catch parsing errors, query materialization and errors in dispatch
      val logicalPlan = Parser.queryRangeToLogicalPlan(
        queryParams.getPromQL,
        TimeStepParams(queryParams.getStart, queryParams.getStep, queryParams.getEnd))
      val execPlan = queryPlanner.materialize(logicalPlan, qContext)
      executePhysicalPlanAndRespond(context, execPlan, listener)
    } catch {
      case ex: Throwable =>
        logger.error("Error executing plan", ex)
        listener.error(ex)
    }
  }
}

object FiloDBMultiPartitionFlightProducer {
  def makeBindableService(queryPlannerSelector: String => QueryPlanner,
                          serverAllocator: BufferAllocator,
                          location: Location,
                          allConfig: Config,
                          executor: ExecutorService): BindableService = {
    val noAuthHandler = new ServerAuthHandler {
      override def isValid(token: Array[Byte]): Optional[String] = Optional.of("")

      override def authenticate(outgoing: ServerAuthHandler.ServerAuthSender,
                                incoming: util.Iterator[Array[Byte]]): Boolean = true
    }

    FlightGrpcUtils.createFlightService(serverAllocator,
      new FiloDBMultiPartitionFlightProducer(queryPlannerSelector, serverAllocator, location, allConfig),
      noAuthHandler,
      executor)
  }

}

