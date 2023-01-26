package filodb.query.exec

import com.typesafe.config.ConfigFactory
import filodb.core.MetricsTestData
import filodb.core.metadata.Column.ColumnType
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.core.query.{ColumnInfo, CustomRangeVectorKey, PromQlQueryParams, QueryConfig, QueryContext, QuerySession, RangeParams, ResultSchema, RvRange}
import filodb.core.store.ChunkSource
import filodb.memory.format.ZeroCopyUTF8String.StringToUTF8
import filodb.memory.format.vectors.MutableHistogram
import filodb.query
import filodb.query.{BinaryOperator, Data, HistSampl, MetadataMapSampl, MetadataSuccessResponse, QueryResponse, QueryResult, Sampl, StreamQueryResponse, SuccessResponse}
import monix.execution.Scheduler.global

import scala.concurrent.Await
import scala.concurrent.duration.Duration


class PromQlRemoteExecSpec extends AnyFunSpec with Matchers with ScalaFutures {
  val timeseriesDataset = Dataset.make("timeseries",
    Seq("tags:map"),
    Seq("timestamp:ts", "value:double:detectDrops=true"),
    options = DatasetOptions(Seq("__name__", "job"), "__name__")).get

  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlanWithClientParams, source: ChunkSource)
                         (implicit sched: Scheduler): Task[QueryResponse] = ???

    override def clusterName: String = ???

    override def isLocalCall: Boolean = ???

    override def dispatchStreaming(plan: ExecPlanWithClientParams,
                                   source: ChunkSource)(implicit sched: Scheduler): Observable[StreamQueryResponse] = ???
  }

  val params = PromQlQueryParams("", 0, 0 , 0)
  val queryContext = QueryContext(origQueryParams = params)
  val config = ConfigFactory.load("application_test.conf")
  val queryConfig = QueryConfig(config.getConfig("filodb.query"))

  it ("should convert matrix Data to QueryResponse ") {
    val expectedResult = List((1000000, 1.0), (2000000, 2.0), (3000000, 3.0))
    val exec = PromQlRemoteExec("", 60000, queryContext, dummyDispatcher, timeseriesDataset.ref, RemoteHttpClient.defaultClient)
    val result = query.Result (Map("instance" -> "inst1"), Some(Seq(Sampl(1000, 1), Sampl(2000, 2), Sampl(3000, 3))),
      None)
    val res = exec.toQueryResponse(SuccessResponse(Data("vector", Seq(result)), queryStats = None), "id", Kamon.currentSpan())
    res.isInstanceOf[QueryResult] shouldEqual true
    val queryResult = res.asInstanceOf[QueryResult]
    queryResult.result(0).numRows.get shouldEqual(3)
    val data = queryResult.result.flatMap(x=>x.rows.map{ r => (r.getLong(0) , r.getDouble(1)) }.toList)
    data.shouldEqual(expectedResult)
  }

  it ("should convert vector Data to QueryResponse ") {
    val expectedResult = List((1000000, 1.0))
    val exec = PromQlRemoteExec("", 60000, queryContext, dummyDispatcher, timeseriesDataset.ref, RemoteHttpClient.defaultClient)
    val result = query.Result (Map("instance" -> "inst1"), None, Some(Sampl(1000, 1)))
    val res = exec.toQueryResponse(SuccessResponse(Data("vector", Seq(result)), queryStats = None), "id", Kamon.currentSpan())
    res.isInstanceOf[QueryResult] shouldEqual true
    val queryResult = res.asInstanceOf[QueryResult]
    queryResult.result(0).numRows.get shouldEqual(1)
    val data = queryResult.result.flatMap(x=>x.rows.map{ r => (r.getLong(0) , r.getDouble(1)) }.toList)
    data.shouldEqual(expectedResult)

  }

  it ("should convert vector Data to QueryResponse for MetadataQuery") {
    val exec = MetadataRemoteExec("", 60000, Map.empty,
      queryContext, dummyDispatcher, timeseriesDataset.ref, RemoteHttpClient.defaultClient)
    val map1 = Map("instance" -> "inst-1", "last-sample" -> "6377838" )
    val map2 = Map("instance" -> "inst-2", "last-sample" -> "6377834" )
    val res = exec.toQueryResponse(MetadataSuccessResponse(Seq(MetadataMapSampl(map1), MetadataMapSampl(map2))), "id", Kamon.currentSpan())
    res.isInstanceOf[QueryResult] shouldEqual true
    val queryResult = res.asInstanceOf[QueryResult]
    val data = queryResult.result.flatMap(x=>x.rows.map{ r => r.getAny(0) }.toList)
    data(0) shouldEqual(map1)
    data(1) shouldEqual(map2)
  }

  it ("should convert vector Data to QueryResponse for Metadata series query") {
    val exec = MetadataRemoteExec("", 60000, Map.empty, queryContext,
      dummyDispatcher, timeseriesDataset.ref, RemoteHttpClient.defaultClient)
    val map1 = Map("instance" -> "inst-1", "last-sample" -> "6377838" )
    val map2 = Map("instance" -> "inst-2", "last-sample" -> "6377834" )
    val res = exec.toQueryResponse(MetadataSuccessResponse(Seq(MetadataMapSampl(map1), MetadataMapSampl(map2))), "id", Kamon.currentSpan())
    res.isInstanceOf[QueryResult] shouldEqual true
    val queryResult = res.asInstanceOf[QueryResult]
    val data = queryResult.result.flatMap(x => x.rows.map(r => r.getAny(0)).toList)
    data(0) shouldEqual(map1)
    data(1) shouldEqual(map2)
  }

  it ("should convert histogram to QueryResponse ") {
    val exec = PromQlRemoteExec("", 60000, queryContext, dummyDispatcher, timeseriesDataset.ref, RemoteHttpClient.defaultClient)
    val result = query.Result (Map("instance" -> "inst1"), None, Some(HistSampl(1000, Map("1" -> 2, "+Inf" -> 3))))
    val res = exec.toQueryResponse(SuccessResponse(Data("vector", Seq(result)), queryStats = None), "id", Kamon.currentSpan())
    res.isInstanceOf[QueryResult] shouldEqual true
    val queryResult = res.asInstanceOf[QueryResult]
    queryResult.result(0).numRows.get shouldEqual(1)
    val data = queryResult.result.flatMap(x => x.rows.map{r => (r.getLong(0) , r.getHistogram(1))}.toList)
    val hist = data.head._2.asInstanceOf[MutableHistogram]
    data.head._1 shouldEqual 1000000
    hist.buckets.allBucketTops shouldEqual Array(1, Double.PositiveInfinity)
    hist.numBuckets shouldEqual(2)
    hist.bucketValue(0) shouldEqual(2.0)
    hist.bucketValue(1) shouldEqual(3)
  }

  it ("should correctly apply RVT") {
    val data = Seq(
      (CustomRangeVectorKey(Map("foo".utf8 -> "bar".utf8)),
        Seq((1000L, 1.0), (2000L, 2.0))),
      (CustomRangeVectorKey(Map("bat".utf8 -> "baz".utf8)),
        Seq((1000L, 3.0), (2000L, 4.0)))
    )
    val range = RvRange(1000, 1000, 2000)
    val resultSchema1 = ResultSchema(
      Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
        ColumnInfo("value", ColumnType.DoubleColumn)),
      numRowKeyColumns = 1,
      fixedVectorLen = Some(123))
    // Override doExecute to return the above data.
    val exec = new PromQlRemoteExec("", 60000, queryContext, dummyDispatcher,
                                 timeseriesDataset.ref, RemoteHttpClient.defaultClient) {
      override def doExecute(source: ChunkSource, querySession: QuerySession)(implicit sched: Scheduler): ExecResult = {
        val rvs = data.map { case (key, tsValPairs) =>
          MetricsTestData.makeRv(key, tsValPairs, range)
        }
        ExecResult(Observable.fromIterable(rvs), Task.now(resultSchema1))
      }
    }
    // Use an RVT to add `diff` to each value.
    val diff = 10.0
    exec.addRangeVectorTransformer(
      ScalarOperationMapper(
        BinaryOperator.ADD,
        scalarOnLhs = false,
        Seq(StaticFuncArgs(diff, RangeParams(1000, 1000, 1000)))))
    val fut = exec.execute(UnsupportedChunkSource(), QuerySession(queryContext, queryConfig))(global)
      .runToFuture(global)
    val qres = Await.result(fut, Duration.Inf).asInstanceOf[QueryResult]
    // Convert the result back to simple key / time-value tuples.
    qres.result.map { rv =>
      val key = rv.key.labelValues
      val pairs = rv.rows().toSeq.map { r =>
        // Subtract the diff from the result-- this should again equal the original data.
        (r.getLong(0), r.getDouble(1) - diff)
      }
      (key, pairs)
    } shouldEqual data.map{ rv => (rv._1.labelValues, rv._2)}
  }
}
