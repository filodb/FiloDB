package filodb.query.exec

import com.typesafe.scalalogging.StrictLogging
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.core.query._
import filodb.query._
import filodb.grpc.GrpcMultiPartitionQueryService
import filodb.grpc.RemoteExecGrpc.RemoteExecImplBase
import filodb.query.ProtoConverters._
import filodb.query.{StreamQueryResponse, StreamQueryResult, StreamQueryResultFooter, StreamQueryResultHeader}
import filodb.memory.format.ZeroCopyUTF8String._
import io.grpc.netty.NettyServerBuilder
import io.grpc.{Server, ManagedChannelBuilder, ManagedChannel, ServerBuilder}
import io.grpc.stub.StreamObserver
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Span}


import java.net.ServerSocket
import scala.util.Using

class PromQLGrpcRemoteExecSpec extends AnyFunSpec with Matchers with ScalaFutures
                                with StrictLogging with BeforeAndAfter with BeforeAndAfterAll {

  implicit val scheduler = monix.execution.Scheduler.Implicits.global
  implicit val defaultPatience = PatienceConfig(timeout = Span(60000, Millis))


  private def toRv(samples: Seq[(Long, Double)],
                   rangeVectorKey: RangeVectorKey,
                   rvPeriod: RvRange): RangeVector = {
    new RangeVector {
      override def key: RangeVectorKey = rangeVectorKey
      override def rows(): RangeVectorCursor = NoCloseCursor
        .NoCloseCursor(samples.map(r => new TransientRow(r._1, r._2)).iterator)

      override def outputRange: Option[RvRange] = Some(rvPeriod)
    }
  }



  val resultSchema = ResultSchema(List(ColumnInfo("ts", ColumnType.DoubleColumn),
    ColumnInfo("val", ColumnType.DoubleColumn)), 1, Map.empty)

  val timeseriesDataset = Dataset.make("timeseries",
    Seq("tags:map"),
    Seq("timestamp:ts", "value:double:detectDrops=true"),
    options = DatasetOptions(Seq("__name__", "job"), "__name__")).get



  var channel: ManagedChannel = _
  var service: Server = _
  var freePort: Option[Int] = _

  before  {
    freePort = Using(new ServerSocket(0)) (_.getLocalPort).toOption
    channel = freePort match {
      case Some(port)    => ManagedChannelBuilder.forAddress("127.0.0.1", port)
        .usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build()
      case None          =>
        logger.warn("No free port found to run PromQLGrpcRemoteExecSpec, cancelling this test")
        cancel()
    }
    service = ServerBuilder.forPort(freePort.get)
      .addService(new TestGrpcServer()).asInstanceOf[ServerBuilder[NettyServerBuilder]].build()
    service.start()
  }


  val dispatcher = InProcessPlanDispatcher(QueryConfig.unitTestingQueryConfig)



  val keysMap = Map("key1".utf8 -> "val1".utf8,
    "key2".utf8 -> "val2".utf8)

  val rvKey = CustomRangeVectorKey(keysMap)

//  val stat = Stat()
//  stat.resultBytes.addAndGet(100)
//  stat.dataBytesScanned.addAndGet(1000)
//  stat.timeSeriesScanned.addAndGet(5)
//
//  val qStats = QueryStats()
//  qStats.stat.put(List(), stat)

  class TestGrpcServer extends RemoteExecImplBase {
    override def execStreaming(request: GrpcMultiPartitionQueryService.Request,
                          responseObserver: StreamObserver[GrpcMultiPartitionQueryService.StreamingResponse]): Unit = {

          val queryParams  = request.getQueryParams.fromProto.asInstanceOf[PromQlQueryParams]
          queryParams.promQl match {
            case """foo{app="app1"}"""  => sendNonEmptyTestResponse.foreach(x => responseObserver.onNext(x.toProto))
            case """error_metric{app="app1"}""" => responseObserver.onNext(
                  StreamQueryError("errorId", "planId", QueryStats(), new Throwable("Inevitable has happened")).toProto)
            case _                      => // empty results
          }
          responseObserver.onCompleted()
    }
  }

  private def sendNonEmptyTestResponse: Seq[StreamQueryResponse] = {
    val header = StreamQueryResultHeader("someId", "planId", resultSchema)


    val builder = SerializedRangeVector.newBuilder()
    val recSchema = new RecordSchema(Seq(ColumnInfo("time", ColumnType.TimestampColumn),
      ColumnInfo("value", ColumnType.DoubleColumn)))

    val rv = toRv(Seq((0, Double.NaN), (100, 1.0), (200, Double.NaN),
      (300, 3.0), (400, Double.NaN),
      (500, 5.0), (600, 6.0),
      (700, Double.NaN), (800, Double.NaN),
      (900, Double.NaN), (1000, Double.NaN)), rvKey,
      RvRange(0, 100, 1000))
    val stats = QueryStats()
    val srv = SerializedRangeVector.apply(rv, builder, recSchema, "someExecPlan", stats)
    val streamingQueryBody = StreamQueryResult("someId", "planId", Seq(srv))

    val warnings = QueryWarnings()

    val footer = StreamQueryResultFooter("someId", "planId", stats, warnings, true, Some("Reason"))
    Seq(header, streamingQueryBody, footer)
  }



  override def afterAll(): Unit = {
    logger.info(s"Shutting down channel on port $freePort")
    channel.shutdown()
    service.shutdown()
  }

  it ("should convert the streaming records from gRPC service to a QueryResponse with data") {

    val params = PromQlQueryParams("""foo{app="app1"}""", 0, 0, 0)
    val queryContext = QueryContext(origQueryParams = params)
    val session = QuerySession(queryContext, QueryConfig.unitTestingQueryConfig)

    val exec = PromQLGrpcRemoteExec(channel, 60000, queryContext, dispatcher, timeseriesDataset.ref, "plannerSelector")

    val qr = exec.execute(UnsupportedChunkSource(), session).runToFuture.futureValue.asInstanceOf[QueryResult]
    qr.resultSchema shouldEqual resultSchema

    qr.id shouldEqual "someId"
    qr.result.size shouldEqual 1
    qr.result.head.isInstanceOf[SerializedRangeVector] shouldEqual true
    val deserializedSrv = qr.result.head.asInstanceOf[SerializedRangeVector]
    deserializedSrv.numRows shouldEqual Some(11)
    deserializedSrv.numRowsSerialized shouldEqual 4
    val res = deserializedSrv.rows.map(r => (r.getLong(0), r.getDouble(1))).toList
    deserializedSrv.key shouldEqual rvKey
    qr.queryStats.getResultBytesCounter(List()).get()shouldEqual 96
    (qr.queryStats.getCpuNanosCounter(List()).get() > 0) shouldEqual true
    res.length shouldEqual 11
    res.map(_._1) shouldEqual (0 to 1000 by 100)
    res.map(_._2).filterNot(_.isNaN) shouldEqual Seq(1.0, 3.0, 5.0, 6.0)
  }

  it ("should convert the streaming records from gRPC service to a empty QueryResponse") {
    val params = PromQlQueryParams("""foo{app="app2"}""", 0, 0 , 0)
    val queryContext = QueryContext(origQueryParams = params)
    val session = QuerySession(queryContext, QueryConfig.unitTestingQueryConfig)

    val exec = PromQLGrpcRemoteExec(channel, 60000, queryContext, dispatcher, timeseriesDataset.ref, "plannerSelector")
    val qr = exec.execute(UnsupportedChunkSource(), session).runToFuture.futureValue.asInstanceOf[QueryResult]
    qr.resultSchema shouldEqual ResultSchema.empty
    qr.result shouldEqual Nil
    qr.queryStats shouldEqual QueryStats()
  }

  it ("should convert the streaming records from gRPC service to a error ") {
    val params = PromQlQueryParams("""error_metric{app="app1"}""", 0, 0 , 0)
    val queryContext = QueryContext(origQueryParams = params)
    val session = QuerySession(queryContext, QueryConfig.unitTestingQueryConfig)

    val exec = PromQLGrpcRemoteExec(channel, 60000, queryContext, dispatcher, timeseriesDataset.ref, "plannerSelector")
    val er = exec.execute(UnsupportedChunkSource(), session).runToFuture.futureValue.asInstanceOf[QueryError]
    er.id shouldEqual "errorId"
    er.queryStats shouldEqual QueryStats()
    er.t.getMessage shouldEqual "Inevitable has happened"
  }

}
