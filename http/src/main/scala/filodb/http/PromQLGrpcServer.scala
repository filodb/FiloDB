package filodb.http


import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

import com.typesafe.scalalogging.StrictLogging
import io.grpc.{Metadata, ServerBuilder, ServerCall, ServerCallHandler, ServerInterceptor}
import io.grpc.ServerCall.Listener
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import kamon.Kamon
import kamon.trace.{Identifier, Span, Trace}
import kamon.trace.Trace.SamplingDecision
import monix.execution.Scheduler
import net.ceedubs.ficus.Ficus._

import filodb.coordinator.FilodbSettings
import filodb.coordinator.queryplanner.QueryPlanner
import filodb.core.metrics.{FilodbMetrics, MetricsHistogram}
import filodb.core.query.{IteratorBackedRangeVector, QueryConfig, QueryContext, QuerySession,
                          QueryStats, SerializedRangeVector}
import filodb.grpc.GrpcMultiPartitionQueryService
import filodb.grpc.RemoteExecGrpc.RemoteExecImplBase
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query._
import filodb.query.exec.{ExecPlanWithClientParams, UnsupportedChunkSource}




/**
 *
 * @param queryPlannerSelector a function that will map the datasetId (usually cluster-dataset but not always true) to
 *                             a planner. Different planners will use different selectors. For example, HA planner will
 *                             use raw-<dataset> for raw HA planner but multi partition planner uses
 *                             singlepartition-<dataset> as the plannerSelector input to resolve the appropriate planner
 *                             to use for materializing the query.
 * @param filoSettings         FiloDB settings.
 * @param scheduler            Scheduler used to dispatch the exec plan
 */
class PromQLGrpcServer(queryPlannerSelector: String => QueryPlanner,
                       filoSettings: FilodbSettings, scheduler: Scheduler)
  extends StrictLogging {

  private val port  = if (filoSettings.grpcPortList.isDefined && filoSettings.localhostOrdinal.isDefined) {
    val ordinal = filoSettings.localhostOrdinal.get
    filoSettings.grpcPortList.get(ordinal)
  } else if (filoSettings.allConfig.hasPath("filodb.grpc.bind-grpc-port")) {
      filoSettings.allConfig.getInt("filodb.grpc.bind-grpc-port")
  } else {
    throw new IllegalArgumentException(
      "[ClusterV2] Grpc port is not defined, " +
      "neither bind-grpc-port nor grpc-port-list with localhost-ordinal are defined"
    )
  }
  private val maxInboundMessageSizeBytes = filoSettings.allConfig.getInt("filodb.grpc.max-inbound-message-size")
  private val server = ServerBuilder.forPort(this.port)
    .intercept(TracingInterceptor).asInstanceOf[ServerBuilder[NettyServerBuilder]]
    .maxInboundMessageSize(maxInboundMessageSizeBytes)
    //.executor(scheduler).asInstanceOf[ServerBuilder[NettyServerBuilder]]
    .addService(new PromQLGrpcService()).asInstanceOf[ServerBuilder[NettyServerBuilder]].build()

  val queryConfig = QueryConfig(filoSettings.allConfig.getConfig("filodb.query"))

  private val queryAskTimeout = filoSettings.allConfig.as[FiniteDuration]("filodb.query.ask-timeout")

  private val queryResponseLatency = FilodbMetrics.timeHistogram("grpc-query-latency", TimeUnit.NANOSECONDS)

  private class PromQLGrpcService extends RemoteExecImplBase {

        trait QueryResponseProcessor {
          def processQueryResponse(qr: QueryResponse): Unit
          def span: Span
        }

        class StreamingResponseProcessor(
          val responseObserver: StreamObserver[GrpcMultiPartitionQueryService.StreamingResponse],
          val span: Span,
          val dataset: String,
          val startNs: Long
        ) extends QueryResponseProcessor {
          // scalastyle:off method.length
          def processQueryResponse(qr: QueryResponse): Unit = {
            import filodb.query.ProtoConverters._
            import filodb.query.QueryResponseConverter._
            // Catch all error
            Try {
              lazy val rb = SerializedRangeVector.newBuilder()
              qr.toStreamingResponse(queryConfig).foreach {
                case footer: StreamQueryResultFooter =>
                  responseObserver.onNext(footer.toProto)
                  span.mark("Received the footer of streaming response")
                  span.finish()
                  val endNs = System.nanoTime()
                  queryResponseLatency.record(endNs - startNs, Map("status" -> "success", "dataset" -> dataset))
                  responseObserver.onCompleted()
                case error: StreamQueryError =>
                  responseObserver.onNext(error.toProto)
                  span.fail(error.t)
                  span.finish()
                  val endNs = System.nanoTime()
                  queryResponseLatency.record(endNs - startNs, Map("status" -> "error", "dataset" -> dataset))
                  responseObserver.onCompleted()
                case header: StreamQueryResultHeader =>
                  responseObserver.onNext(header.toProto)
                  span.mark("Received the header of streaming response")
                case result: StreamQueryResult =>
                  // Not the cleanest way, but we need to convert these IteratorBackedRangeVectors to a
                  // serializable one If we have a result, its definitely is a QueryResult
                  val qres = qr.asInstanceOf[QueryResult]
                  val strQueryResult = result.copy(result = result.result.map {
                    case irv: IteratorBackedRangeVector =>
                      SerializedRangeVector.apply(irv, rb,
                        SerializedRangeVector.toSchema(qres.resultSchema.columns, qres.resultSchema.brSchemas),
                        "GrpcServer", qres.queryStats)
                    case result => result
                  })
                  span.mark("onNext of the streaming result called")
                  responseObserver.onNext(strQueryResult.toProto)
              }
            } match {
              // Catch all to ensure onError is invoked
              case Failure(t) =>
                logger.error("Caught failure while executing query", t)
                span.fail(t)
                span.finish()
                responseObserver.onError(t)
              case Success(_) =>
            }
          }
        }

        class ResponseProcessor(
          val responseObserver: StreamObserver[GrpcMultiPartitionQueryService.Response],
          val span: Span,
          val startNs: Long,
          val hist: MetricsHistogram,
          val metricTags: Map[String, String]
        ) extends QueryResponseProcessor {
          def processQueryResponse(qr: QueryResponse): Unit = {
            import filodb.query.ProtoConverters._
            Try {
              val queryResponse = qr match {
                case err: QueryError => err
                case res: QueryResult =>
                  lazy val rb = SerializedRangeVector.newBuilder()
                  val rvs = res.result.map {
                    case irv: IteratorBackedRangeVector =>
                      val resultSchema = res.resultSchema
                      SerializedRangeVector.apply(irv, rb,
                        SerializedRangeVector.toSchema(resultSchema.columns, resultSchema.brSchemas),
                        "GrpcServer", res.queryStats)
                    case rv => rv
                  }
                  res.copy(result = rvs)
              }
              responseObserver.onNext(queryResponse.toProto)
            } match {
              case Failure(t) =>
                logger.error("Caught failure while executing query", t)
                span.fail(t)
                span.finish()
                hist.record(System.nanoTime() - startNs, metricTags ++ Map("status" -> "error"))
                responseObserver.onError(t)
              case Success(_) =>
                span.finish()
                hist.record(System.nanoTime() - startNs, metricTags ++ Map("status" -> "success"))
                responseObserver.onCompleted()
            }
          }
        }

        private def executeQuery(
          request: GrpcMultiPartitionQueryService.Request,
          rp: QueryResponseProcessor
        ): Unit = {
          import filodb.query.ProtoConverters._
          implicit val timeout: FiniteDuration = queryAskTimeout
          implicit val dispatcherScheduler: Scheduler = scheduler
          rp.span.mark("Sending query request")
          val queryParams = request.getQueryParams
          val qContext = QueryContext(origQueryParams = request.getQueryParams.fromProto,
            plannerParams = request.getPlannerParams.fromProto)
          val eval = Try {
            val queryPlanner = queryPlannerSelector(request.getPlannerSelector)
            // Catch parsing errors, query materialization and errors in dispatch
            val logicalPlan = Parser.queryRangeToLogicalPlan(
              queryParams.getPromQL,
              TimeStepParams(queryParams.getStart, queryParams.getStep, queryParams.getEnd))

            val querySession = QuerySession(qContext, queryConfig, catchMultipleLockSetErrors = true)
            val exec = queryPlanner.materialize(logicalPlan, qContext)
            queryPlanner.dispatchExecPlan(exec, querySession, rp.span).foreach(
              (qr: QueryResponse) => rp.processQueryResponse(qr)
            )
          }
          eval match {
            case Failure(t)   =>
              logger.error("Caught failure while executing query", t)
              rp.processQueryResponse(QueryError(qContext.queryId, QueryStats(), t))
              rp.span.fail("Query execution failed", t)
            case _            =>  rp.span.mark("query execution successful")
          }
        }

        override def execStreaming(
          request: GrpcMultiPartitionQueryService.Request,
          responseObserver: StreamObserver[GrpcMultiPartitionQueryService.StreamingResponse]
        ): Unit = {
          val dataset = request.getDataset
          val span = Kamon.currentSpan()
          val startNs = System.nanoTime()
          val rp = new StreamingResponseProcessor(responseObserver, span, dataset, startNs)
          executeQuery(request, rp)
        }

        override def exec(request: GrpcMultiPartitionQueryService.Request,
                         responseObserver: StreamObserver[GrpcMultiPartitionQueryService.Response]): Unit = {
          val span = Kamon.currentSpan()
          val startNs = System.nanoTime()
          val rp = new ResponseProcessor(responseObserver, span, startNs, queryResponseLatency,
            Map("dataset" -> request.getDataset))
          executeQuery(request, rp)
        }

        override def executePlan(
          remoteExecPlanProto: GrpcMultiPartitionQueryService.RemoteExecPlan,
          responseObserver: StreamObserver[GrpcMultiPartitionQueryService.StreamingResponse]
        ): Unit = {
          implicit val dispatcherScheduler: Scheduler = scheduler
          import filodb.coordinator.ProtoConverters._
          val queryContextProto = remoteExecPlanProto.getQueryContext
          val queryContext = queryContextProto.fromProto
          val querySession = QuerySession(queryContext, queryConfig, catchMultipleLockSetErrors = true)
          val execPlan = remoteExecPlanProto.getExecPlan().fromProto(queryContext)
          val span = Kamon.currentSpan()
          val startNs = System.nanoTime()
          val dataset = execPlan.dataset.dataset
          val rp = new StreamingResponseProcessor(responseObserver, span, dataset, startNs)
          val eval = Try {
            val execPlanWParams = ExecPlanWithClientParams(
              execPlan, filodb.query.exec.ClientParams(execPlan.queryContext.plannerParams.queryTimeoutMillis),
              querySession)
            execPlan.dispatcher.dispatch(execPlanWParams, UnsupportedChunkSource()).foreach(
              (qr: QueryResponse) => rp.processQueryResponse(qr)
            )
          }
          eval match {
            case Failure(t) =>
              logger.error("Caught failure while dispatching execution plan", t)
              rp.processQueryResponse(QueryError(execPlan.queryContext.queryId, QueryStats(), t))
            case _            =>  rp.span.mark("exec plan dispatched successful")
          }
        }

        override def executePlan2(
          remoteExecPlanProto: GrpcMultiPartitionQueryService.RemoteExecPlan,
          responseObserver: StreamObserver[GrpcMultiPartitionQueryService.StreamingResponse]
        ): Unit = {
          throw new NotImplementedError(
            "FiloDB intercluster communication is not implemented yet"
          )
        }
      }


  def stop(): Unit = {
    if (server != null) {
      server.awaitTermination(1, TimeUnit.MINUTES)
    }
  }

  def start(): Unit = {
    server.start()
    logger.info("Server started, listening on " + this.port)
    Runtime.getRuntime.addShutdownHook(new Thread() {
      () => PromQLGrpcServer.this.stop()
    })
  }

}

object TracingInterceptor extends ServerInterceptor with StrictLogging {
  override def interceptCall[ReqT, RespT]
        (call: ServerCall[ReqT, RespT], headers: Metadata,
         next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    val span = TracingUtil.startAndGetCurrentSpan(headers)
    val listener = next.startCall(call, headers)


    new Listener[ReqT]() {

      override def onHalfClose(): Unit =
        Kamon.runWithSpan(span, finishSpan = false) {
          listener.onHalfClose()
        }

      override def onCancel(): Unit =
        Kamon.runWithSpan(span, finishSpan = false) {
          listener.onCancel()
        }

      override def onComplete(): Unit =
        Kamon.runWithSpan(span, finishSpan = false) {
          listener.onComplete()
        }

      override def onReady(): Unit =
        Kamon.runWithSpan(span, finishSpan = false) {
          listener.onReady()
        }

      override def onMessage(message: ReqT): Unit =
        Kamon.runWithSpan(span, finishSpan = false) {
          listener.onMessage(message)
        }
    }
  }
}
object TracingUtil extends StrictLogging  {
  // Current supported traces are openzipkin trace identifiers, in future support for others will be added
  private val TRACE_ID_HEADER = "X-B3-TraceId"
  private val SPAN_ID_HEADER = "X-B3-SpanId"
  private val TRACE_SAMPLED_HEADER = "X-B3-Sampled"
  private val PARENT_SPAN_ID_HEADER = "X-B3-ParentSpanId"

  def startAndGetCurrentSpan(md: Metadata): Span = {
    val parentSpan = getRemoteTrace(md)
    val spanBuilder = Kamon.spanBuilder("query_grpc").tag("method", "execStreaming").asChildOf(parentSpan)
    spanBuilder.start()
  }

  private def getRemoteTrace(md: Metadata): Span.Remote = {
    val traceIdKey = Metadata.Key.of(TRACE_ID_HEADER, Metadata.ASCII_STRING_MARSHALLER)
    val traceIdentifier =
      if (md.containsKey(traceIdKey))
        Identifier.Scheme.Single.traceIdFactory.from(md.get(traceIdKey))
      else
        Identifier.Scheme.Single.traceIdFactory.generate()

    val spanIdKey = Metadata.Key.of(SPAN_ID_HEADER, Metadata.ASCII_STRING_MARSHALLER)
    val spanIdentifier =
      if (md.containsKey(spanIdKey))
        Identifier.Scheme.Single.traceIdFactory.from(md.get(spanIdKey))
      else
        Identifier.Scheme.Single.traceIdFactory.generate()

    val parentSpanId = Metadata.Key.of(PARENT_SPAN_ID_HEADER, Metadata.ASCII_STRING_MARSHALLER)
    val parentSpanIdentifier =
      if (md.containsKey(parentSpanId))
        Identifier.Scheme.Single.traceIdFactory.from(md.get(parentSpanId))
      else
        Identifier.Empty

    val sampled = Metadata.Key.of(TRACE_SAMPLED_HEADER, Metadata.ASCII_STRING_MARSHALLER)
    val sampleTrace = if (md.containsKey(sampled)) "1".equals(md.get(sampled)) else false

    if (sampleTrace)
      Span.Remote(spanIdentifier, parentSpanIdentifier, Trace.create(traceIdentifier, SamplingDecision.Sample))
    else
      Span.Remote(spanIdentifier, parentSpanIdentifier, Trace.create(traceIdentifier, SamplingDecision.DoNotSample))
  }
}
