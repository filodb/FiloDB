package filodb.query.exec

import java.util.concurrent.TimeUnit

import scala.concurrent.Future
import scala.concurrent.duration.Duration

import io.grpc.{Channel, Metadata}
import io.grpc.stub.{MetadataUtils, StreamObserver}
import kamon.Kamon
import kamon.trace.Span
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{MulticastStrategy, Observable}
import monix.reactive.subjects.ConcurrentSubject

import filodb.core.DatasetRef
import filodb.core.query.{PromQlQueryParams, QueryContext, QuerySession}
import filodb.core.store.ChunkSource
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

    override def sendHttpRequest(execPlan2Span: Span, httpTimeoutMs: Long)(implicit sched: Scheduler):
    Future[QueryResponse] = ???

    // Since execute of overridden no one will invoke this
    // TODO: note PromQLRemoteExec and this implementation should be using doExecute and not override execute.
    //  execute in ExecPlan needs to be final
    override def doExecute(source: ChunkSource, querySession: QuerySession)
                          (implicit sched: Scheduler): ExecResult = ???

    override def execute(source: ChunkSource,
                         querySession: QuerySession)(implicit sched: Scheduler): Task[QueryResponse] = {
        val span = Kamon.currentSpan()
        // Dont finish span since this code didnt create it
        Kamon.runWithSpan(span, finishSpan = false) {
            sendGrpcRequest(span, requestTimeoutMs)
              .toListL
              .map(_.toIterator.toQueryResponse)
              .map (applyTransformers(_, querySession, source))
        }
    }

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
                           dispatcher: PlanDispatcher,
                           dataset: DatasetRef,
                           plannerSelector: String) extends GrpcRemoteExec {
    override def sendGrpcRequest(span: Span, requestTimeoutMs: Long)(implicit sched: Scheduler):
        // Todo add asset for thread name
    Observable[GrpcMultiPartitionQueryService.StreamingResponse] = {
        val subject = ConcurrentSubject[GrpcMultiPartitionQueryService.StreamingResponse](MulticastStrategy.Publish)
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
                        override def onError(t: java.lang.Throwable): Unit = subject.onError(t)
                        override def onCompleted(): Unit = subject.onComplete()
                    })
          })
    }

    override def queryEndpoint: String = channel.authority() + ".execStreaming"
}


