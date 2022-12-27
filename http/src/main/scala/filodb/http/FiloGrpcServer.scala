package filodb.http

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import com.typesafe.scalalogging.StrictLogging
import io.grpc.ServerBuilder
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import monix.execution.Scheduler
import net.ceedubs.ficus.Ficus._

import filodb.coordinator.FilodbSettings
import filodb.coordinator.queryplanner.QueryPlanner
import filodb.core.query.QueryContext
import filodb.grpc.GrpcMultiPartitionQueryService
import filodb.grpc.RemoteExecGrpc.RemoteExecImplBase
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.{QueryResponse, StreamQueryResponse, StreamQueryResultFooter}



class FiloGrpcServer(queryPlanner: QueryPlanner, filoSettings: FilodbSettings, scheduler: Scheduler)
  extends StrictLogging{

  val port  = filoSettings.allConfig.getInt("filodb.grpc.bind-grpc-port")
  val server = ServerBuilder.forPort(this.port)
    .addService(new PromQLGrpcService()).asInstanceOf[ServerBuilder[NettyServerBuilder]].build()

  val queryAskTimeout = filoSettings.allConfig.as[FiniteDuration]("filodb.query.ask-timeout")

  private class PromQLGrpcService extends RemoteExecImplBase {

        private def executeQuery(request: GrpcMultiPartitionQueryService.Request)(f: QueryResponse => Unit): Unit = {
          import filodb.query.ProtoConverters._
          implicit val timeout = queryAskTimeout
          implicit val monixScheduler  = scheduler
          val queryParams = request.getQueryParams();
          val logicalPlan = Parser.queryRangeToLogicalPlan(
            queryParams.getPromQL(),
            TimeStepParams(queryParams.getStart(), queryParams.getStep(), queryParams.getEnd()));

          val config = QueryContext(origQueryParams = request.getQueryParams.fromProto,
            plannerParams = request.getPlannerParams.fromProto)
          val exec = queryPlanner.materialize(logicalPlan, config)

          // TODO: make sure trace is propagated from remote call
          //TODO: is queryAskTimeout the right timeout?
          queryPlanner.dispatchExecPlan(exec, kamon.Kamon.currentSpan()).foreach(f)
        }

        override def execStreaming(request: GrpcMultiPartitionQueryService.Request,
                           responseObserver: StreamObserver[GrpcMultiPartitionQueryService.StreamingResponse]): Unit = {
          import filodb.query.ProtoConverters._
          import filodb.query.QueryResponseConverter._
          executeQuery(request) {
            qr: QueryResponse =>
              qr.toStreamingResponse.foreach {
                case footer: StreamQueryResultFooter =>
                  responseObserver.onNext(footer.toProto)
                  responseObserver.onCompleted()
                case others: StreamQueryResponse   =>
                  responseObserver.onNext(others.toProto)
              }
          }
        }

        override def exec(request: GrpcMultiPartitionQueryService.Request,
                         responseObserver: StreamObserver[GrpcMultiPartitionQueryService.Response]): Unit = {
           import filodb.query.ProtoConverters._
           executeQuery(request) {
             qr: QueryResponse => responseObserver.onNext(qr.toProto)
           }
           responseObserver.onCompleted()
        }

  }


  def stop(): Unit = {
    if (server != null) {
      server.awaitTermination(1, TimeUnit.MINUTES)
    }
  }

  def start(): Unit = {
    server.start();
    logger.info("Server started, listening on " + this.port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      () => FiloGrpcServer.this.stop()
    })
  }

}
