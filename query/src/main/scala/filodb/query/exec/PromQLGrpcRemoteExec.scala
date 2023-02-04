package filodb.query.exec

import java.util.concurrent.TimeUnit

import io.grpc.Channel
import io.grpc.stub.StreamObserver
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



trait GrpcRemoteExec extends LeafExecPlan {


    def promQlQueryParams: PromQlQueryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]

    // Since execute of overridden no one will invoke this
    override def doExecute(source: ChunkSource, querySession: QuerySession)
                          (implicit sched: Scheduler): ExecResult = ???

    override def execute(source: ChunkSource,
                         querySession: QuerySession)(implicit sched: Scheduler): Task[QueryResponse] = {
        val span = Kamon.currentSpan()
        // Dont finish span since this code didnt create it
        Kamon.runWithSpan(span, false) {
            sendGrpcRequest(span, requestTimeoutMs).toListL.map(_.toIterator.toQueryResponse)
        }
    }


    def args: String = s"${promQlQueryParams.toString}, ${queryContext.plannerParams}, queryEndpoint=$queryEndpoint, " +
      s"requestTimeoutMs=$requestTimeoutMs"

    def sendGrpcRequest(span: Span, requestTimeoutMs: Long)(implicit sched: Scheduler):
    Observable[GrpcMultiPartitionQueryService.StreamingResponse]

    def requestTimeoutMs: Long

    def queryEndpoint: String


    def getGrpcRequest: Request = {
        val builder = Request.newBuilder()
        builder.setQueryParams(promQlQueryParams.toProto)
        builder.setPlannerParams(queryContext.plannerParams.copy(processMultiPartition = false).toProto)
        builder.build()
    }


}
case class PromQLGrpcRemoteExec(channel: Channel,
                           requestTimeoutMs: Long,
                           queryContext: QueryContext,
                           dispatcher: PlanDispatcher,
                           dataset: DatasetRef) extends GrpcRemoteExec {
    override def sendGrpcRequest(span: Span, requestTimeoutMs: Long)(implicit sched: Scheduler):
        // Todo add asset for thread name
    Observable[GrpcMultiPartitionQueryService.StreamingResponse] = {
        val subject = ConcurrentSubject[GrpcMultiPartitionQueryService.StreamingResponse](MulticastStrategy.Publish)
        subject
          .doOnSubscribe(Task.eval {
              val nonBlockingStub = RemoteExecGrpc.newStub(channel)
              nonBlockingStub.withDeadlineAfter(requestTimeoutMs, TimeUnit.MILLISECONDS)
                .execStreaming(getGrpcRequest,
                    new StreamObserver[GrpcMultiPartitionQueryService.StreamingResponse] {
                        override def onNext(value: StreamingResponse): Unit = subject.onNext(value)
                        override def onError(t: java.lang.Throwable): Unit = subject.onError(t)
                        override def onCompleted(): Unit = subject.onComplete()
                    })
          })
    }

    override def queryEndpoint: String = channel.authority() + ".execStreaming"
}


