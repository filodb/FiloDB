package filodb.coordinator.flight

import kamon.trace.Span
import monix.eval.Task
import monix.execution.Scheduler
import org.apache.arrow.flight.Location

import filodb.core.DatasetRef
import filodb.core.query.{PromQlQueryParams, QueryContext, QuerySession}
import filodb.grpc.GrpcMultiPartitionQueryService.Request
import filodb.query.ProtoConverters.{PlannerParamsToProtoConverter, QueryParamsToProtoConversion}
import filodb.query.QueryResponse
import filodb.query.exec._

case class PromQLFlightRemoteExec(queryContext: QueryContext,
                                  override val dispatcher: PlanDispatcher,
                                  queryEndpoint: String,
                                  requestTimeoutMs: Long,
                                  dataset: DatasetRef,
                                  plannerSelector: String,
                                  destinationTsdbWorkUnit: String) extends RemoteExec {

  require(dispatcher.isInstanceOf[InProcessPlanDispatcher], "PromQLFlightRemoteExec should only be used with " +
    "an InProcessPlanDispatcher since the client is invoked locally")

  def grpcRequest: Request = {
    val builder = Request.newBuilder()
    builder.setQueryParams(queryContext.origQueryParams.asInstanceOf[PromQlQueryParams].toProto)
    builder.setPlannerParams(queryContext.plannerParams.copy(processMultiPartition = false).toProto)
    builder.setPlannerSelector(plannerSelector)
    builder.build()
  }

  override def sendRequest(span: Span, timeoutMs: Long,
                           querySession: QuerySession)(implicit sched: Scheduler): Task[QueryResponse] = {
    // Here we create a new flight dispatcher to send the query off. The dispatcher has
    // logic for flight client
    val dispatcher = FlightPlanDispatcher(new Location(queryEndpoint), "promql-flight")
    val planWithParams = ExecPlanWithClientParams(
      this,
      ClientParams(queryContext.plannerParams.queryTimeoutMillis),
      querySession)
    dispatcher.dispatch(planWithParams, UnsupportedChunkSource())
  }

  override def remoteExecHttpClient: RemoteExecHttpClient = ???
  override def urlParams: Map[String, String] = ???
}
