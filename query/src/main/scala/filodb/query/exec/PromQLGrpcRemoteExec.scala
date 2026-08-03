package filodb.query.exec

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.StrictLogging
import io.grpc.{Channel, Metadata}
import io.grpc.stub.{MetadataUtils, StreamObserver}
import kamon.trace.Span
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{MulticastStrategy, Observable}
import monix.reactive.subjects.ConcurrentSubject

import filodb.core.DatasetRef
import filodb.core.GlobalConfig
import filodb.core.metrics.FilodbMetrics
import filodb.core.query.{PromQlQueryParams, QueryContext, QuerySession}
import filodb.grpc._
import filodb.grpc.GrpcMultiPartitionQueryService._
import filodb.query.ProtoConverters._
import filodb.query.QueryResponse


trait GrpcRemoteExec extends RemoteExec {


    override def promQlQueryParams: PromQlQueryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]

    // TODO: RemoteExec is not truly remote exec makes assumption of using Http for remoting, for now we will mark these
    //  as unimplemented but the intent os extending from RemoteExec to to group all Execs that arent serialized.
    //  RemoteExec should not be making any assumptions on transport and HttpRemoteExec is the implementation that
    //  uses Http for remoting. No refactoring will be done as part of this PR and can be done in future
    def urlParams: Map[String, String] = ???

    def remoteExecHttpClient: RemoteExecHttpClient = ???

    def sendRequest(span: Span, timeoutMs: Long, querySession: QuerySession)(implicit sched: Scheduler):
      Task[QueryResponse] = sendGrpcRequest(span, requestTimeoutMs).toListL.map(_.iterator.toQueryResponse)

    override def args: String = s"${promQlQueryParams.toString}, ${queryContext.plannerParams}, " +
      s"queryEndpoint=$queryEndpoint, " +
      s"requestTimeoutMs=$requestTimeoutMs"

    def sendGrpcRequest(span: Span, requestTimeoutMs: Long)(implicit sched: Scheduler):
    Observable[GrpcMultiPartitionQueryService.StreamingResponse]

    def requestTimeoutMs: Long

    def queryEndpoint: String


    def getGrpcRequest(plannerSelector: String): Request = {
        val builder = Request.newBuilder()
        builder.setQueryParams(promQlQueryParams.toProto)
        builder.setPlannerParams(queryContext.plannerParams.copy(processMultiPartition = false).toProto)
        builder.setPlannerSelector(plannerSelector)
        builder.build()
    }


}
case class PromQLGrpcRemoteExec(channel: Channel,
                           requestTimeoutMs: Long,
                           queryContext: QueryContext,
                           dispatcher: filodb.query.exec.PlanDispatcher,
                           dataset: DatasetRef,
                           plannerSelector: String,
                           destinationTsdbWorkUnit: String) extends GrpcRemoteExec {
    override def sendGrpcRequest(span: Span, requestTimeoutMs: Long)(implicit sched: Scheduler):
        // Todo add asset for thread name
    Observable[GrpcMultiPartitionQueryService.StreamingResponse] = {
        val subject = ConcurrentSubject[GrpcMultiPartitionQueryService.StreamingResponse](MulticastStrategy.Publish)
        val startNs = System.nanoTime()
        subject
          .doOnSubscribe(Task.eval {
              val nonBlockingStub = RemoteExecGrpc.newStub(channel)
              val md = new Metadata();
              queryContext.traceInfo.foreach {
                  case (key, value)   => md.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value)
              }
              nonBlockingStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md))
              .withDeadlineAfter(requestTimeoutMs, TimeUnit.MILLISECONDS)
                .execStreaming(getGrpcRequest(plannerSelector),
                    new StreamObserver[GrpcMultiPartitionQueryService.StreamingResponse] {
                        override def onNext(value: StreamingResponse): Unit = subject.onNext(value)
                        override def onError(t: java.lang.Throwable): Unit = {
                            PromQLGrpcRemoteExec.logError(destinationTsdbWorkUnit, queryContext, t)
                            val elapsedMs = (System.nanoTime() - startNs) / 1000000
                            if (PromQLGrpcRemoteExec.latencyMetricsEnabled) {
                                PromQLGrpcRemoteExec.grpcRemoteExecErrorLatency.record(
                                  elapsedMs,
                                  Map("destination" -> destinationTsdbWorkUnit))
                            }
                            subject.onError(t)
                        }
                        override def onCompleted(): Unit = {
                            if (PromQLGrpcRemoteExec.latencyMetricsEnabled) {
                                val elapsedMs = (System.nanoTime() - startNs) / 1000000
                                PromQLGrpcRemoteExec.grpcRemoteExecSuccessLatency.record(
                                  elapsedMs,
                                  Map("destination" -> destinationTsdbWorkUnit))
                            }
                            subject.onComplete()
                        }
                    })
          })
    }

    override def queryEndpoint: String = channel.authority() + ".execStreaming"
}

object PromQLGrpcRemoteExec extends StrictLogging {
    val latencyMetricsEnabled: Boolean = {
        GlobalConfig.systemConfig.hasPath("filodb.query.grpc.promql-remote-exec-latency-metrics-enabled") &&
          GlobalConfig.systemConfig.getBoolean("filodb.query.grpc.promql-remote-exec-latency-metrics-enabled")
    }
    lazy val grpcRemoteExecSuccessLatency = FilodbMetrics.timeHistogram(
      "grpc-remote-exec-success-latency", TimeUnit.MILLISECONDS)
    lazy val grpcRemoteExecErrorLatency = FilodbMetrics.timeHistogram(
      "grpc-remote-exec-error-latency", TimeUnit.MILLISECONDS)

    def logError(destination: String, queryContext: QueryContext, t: java.lang.Throwable): Unit = {
        logger.error(s"gRPC remote exec to destination=$destination failed, queryContext=$queryContext", t)
    }
}


