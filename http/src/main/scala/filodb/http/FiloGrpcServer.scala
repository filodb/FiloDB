package filodb.http

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

import com.typesafe.scalalogging.StrictLogging
import io.grpc.ServerBuilder
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import monix.execution.Scheduler
import net.ceedubs.ficus.Ficus._

import filodb.coordinator.FilodbSettings
import filodb.coordinator.queryplanner.QueryPlanner
import filodb.core.query.{IteratorBackedRangeVector, QueryContext, QueryStats, SerializedRangeVector}
import filodb.grpc.GrpcMultiPartitionQueryService
import filodb.grpc.RemoteExecGrpc.RemoteExecImplBase
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query._


class FiloGrpcServer(queryPlanner: QueryPlanner, filoSettings: FilodbSettings, scheduler: Scheduler)
  extends StrictLogging{

  val port  = filoSettings.allConfig.getInt("filodb.grpc.bind-grpc-port")
  val server = ServerBuilder.forPort(this.port)
    .executor(scheduler).asInstanceOf[ServerBuilder[NettyServerBuilder]]
    .addService(new PromQLGrpcService()).asInstanceOf[ServerBuilder[NettyServerBuilder]].build()

  val queryAskTimeout = filoSettings.allConfig.as[FiniteDuration]("filodb.query.ask-timeout")

  private class PromQLGrpcService extends RemoteExecImplBase {

        private def executeQuery(request: GrpcMultiPartitionQueryService.Request)(f: QueryResponse => Unit): Unit = {
          import filodb.query.ProtoConverters._
          implicit val timeout = queryAskTimeout
          implicit val monixScheduler  = scheduler
          val queryParams = request.getQueryParams()
          val config = QueryContext(origQueryParams = request.getQueryParams.fromProto,
            plannerParams = request.getPlannerParams.fromProto)
          val eval = Try {
            // Catch parsing errors, query materialization and errors in dispatch
            val logicalPlan = Parser.queryRangeToLogicalPlan(
              queryParams.getPromQL(),
              TimeStepParams(queryParams.getStart(), queryParams.getStep(), queryParams.getEnd()))

            val exec = queryPlanner.materialize(logicalPlan, config)
            // TODO: make sure trace is propagated from remote call
            //TODO: is queryAskTimeout the right timeout?
            queryPlanner.dispatchExecPlan(exec, kamon.Kamon.currentSpan()).foreach(f)
          }
          eval match {
            case Failure(t)   => f(QueryError(config.queryId, QueryStats(), t))
            case _            => //Nop, for success we dont care as the response is already notified
          }
        }

        override def execStreaming(request: GrpcMultiPartitionQueryService.Request,
                           responseObserver: StreamObserver[GrpcMultiPartitionQueryService.StreamingResponse]): Unit = {
          import filodb.query.ProtoConverters._
          import filodb.query.QueryResponseConverter._
          executeQuery(request) {
                // Catch all error
            qr: QueryResponse =>
              Try {
                lazy val rb = SerializedRangeVector.newBuilder()
                qr.toStreamingResponse.foreach {
                  case footer: StreamQueryResultFooter =>
                    responseObserver.onNext(footer.toProto)
                    responseObserver.onCompleted()
                  case error: StreamQueryError =>
                    responseObserver.onNext(error.toProto)
                    responseObserver.onCompleted()
                  case header: StreamQueryResultHeader =>
                    responseObserver.onNext(header.toProto)
                  case result: StreamQueryResult =>
                    // Not the cleanest way, but we need to convert these IteratorBackedRangeVectors to a
                    // serializable one If we have a result, its definitely is a QueryResult
                    val strQueryResult = (result.result, qr) match {
                      case (irv: IteratorBackedRangeVector, QueryResult(_, resultSchema, _, _, _, _)) =>
                        result.copy(result = SerializedRangeVector.apply(irv, rb,
                          SerializedRangeVector.toSchema(resultSchema.columns, resultSchema.brSchemas), "GrpcServer"))
                      case _ => result
                    }
                    Try(strQueryResult.toProto) match {
                      case Failure(exception) =>
                        //TODO: Need to rework when executeQuery is streaming as stats arrive in footer
                        // This also means, that we may see StreamQueryError after some body messages are sent
                        val stats = qr match {
                          case err: QueryError => err.queryStats
                          case QueryResult(_, _, _, queryStats, _, _) => queryStats
                        }
                        val error = StreamQueryError(strQueryResult.id, stats, exception)
                        responseObserver.onNext(error.toProto)
                        responseObserver.onCompleted()
                      case Success(value) => responseObserver.onNext(value)
                    }
                }
              } match {
                // Catch all to ensure connection is closed
                case Failure(t)            => responseObserver.onError(t)
                case Success(_)            =>
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
