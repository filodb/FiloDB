package filodb.query.exec

import java.util.concurrent.TimeUnit

import io.grpc.Channel
import kamon.Kamon
import kamon.trace.Span
import monix.eval.Task
import monix.execution.Scheduler
import scala.concurrent.Future

import filodb.core.DatasetRef
import filodb.core.query.{PromQlQueryParams, QueryContext, QuerySession}
import filodb.core.store.ChunkSource
import filodb.grpc.GrpcMultiPartitionQueryService.Request
import filodb.grpc.RemoteExecGrpc
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
            Task.fromFuture(sendGrpcRequest(span, requestTimeoutMs))
        }
    }


    def args: String = s"${promQlQueryParams.toString}, ${queryContext.plannerParams}, queryEndpoint=$queryEndpoint, " +
      s"requestTimeoutMs=$requestTimeoutMs"

    def sendGrpcRequest(span: Span, requestTimeoutMs: Long)(implicit sched: Scheduler): Future[QueryResponse]

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
    override def sendGrpcRequest(span: Span, requestTimeoutMs: Long)(implicit sched: Scheduler): Future[QueryResponse]
    = Future {
        val blockingStub = RemoteExecGrpc.newBlockingStub(channel)
        import scala.collection.JavaConverters._
        import filodb.query.ProtoConverters._
        blockingStub.withDeadlineAfter(requestTimeoutMs, TimeUnit.MILLISECONDS)
          .execStreaming(getGrpcRequest).asScala.toQueryResponse
    }

    override def queryEndpoint: String = channel.authority() + ".execStreaming"
}


