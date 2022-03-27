package filodb.query.exec

import com.softwaremill.sttp.{Response, StatusCodes, SttpBackend}
import com.softwaremill.sttp.testing.SttpBackendStub
import com.typesafe.config.ConfigFactory
import filodb.core.MetricsTestData._
import filodb.core.binaryrecord2.BinaryRecordRowReader
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SomeData, TimeSeriesMemStore}
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.core.query.Filter.Equals
import filodb.core.store.{ChunkSource, InMemoryMetaStore, NullColumnStore}
import filodb.core.TestData
import filodb.memory.format.SeqRowReader
import filodb.query._
import filodb.query.exec.RemoteHttpClient.configBuilder
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import filodb.memory.format.ZeroCopyUTF8String._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration._

class RemoteMetadataExecSpec extends AnyFunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)

  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))

  val now = System.currentTimeMillis()
  val numRawSamples = 1000
  val reportingInterval = 10000
  val limit = 2
  val tuples = (numRawSamples until 0).by(-1).map { n =>
    (now - n * reportingInterval, n.toDouble)
  }

  val shardPartKeyLabelValues = Seq(
    Seq(  // shard 0
      ("http_req_total", Map("instance"->"someHost:8787", "job"->"myCoolService",
        "unicode_tag" -> "uni\u03C0tag", "_ws_" -> "demo", "_ns_" -> "App-0")),
      ("http_foo_total", Map("instance"->"someHost:8787", "job"->"myCoolService",
        "unicode_tag" -> "uni\u03BCtag", "_ws_" -> "demo", "_ns_" -> "App-0"))
    ),
    Seq (  // shard 1
      ("http_req_total", Map("instance"->"someHost:9090", "job"->"myCoolService",
        "unicode_tag" -> "uni\u03C0tag", "_ws_" -> "demo", "_ns_" -> "App-0")),
      ("http_bar_total", Map("instance"->"someHost:8787", "job"->"myCoolService",
        "unicode_tag" -> "uni\u03C0tag", "_ws_" -> "demo", "_ns_" -> "App-0")),
      ("http_req_total-A", Map("instance"->"someHost:9090", "job"->"myCoolService",
        "unicode_tag" -> "uni\u03C0tag", "_ws_" -> "demo-A", "_ns_" -> "App-A")),
    )
  )

  val addlLabels = Map("_type_" -> "prom-counter")
  val expectedLabelValues = shardPartKeyLabelValues.flatMap { shardSeq =>
    shardSeq.map(pair => pair._2 + ("_metric_" -> pair._1) ++ addlLabels)
  }

  implicit val execTimeout = 5.seconds

  def initShard(memStore: TimeSeriesMemStore,
                partKeyLabelValues: Seq[Tuple2[String, Map[String, String]]],
                ishard: Int): Unit = {
    val partTagsUTF8s = partKeyLabelValues.map{case (m, t) => (m,  t.map { case (k, v) => (k.utf8, v.utf8)})}
    // NOTE: due to max-chunk-size in storeConf = 100, this will make (numRawSamples / 100) chunks
    // Be sure to reset the builder; it is in an Object so static and shared amongst tests
    builder.reset()
    partTagsUTF8s.map { case (metric, partTagsUTF8) =>
      tuples.map { t => SeqRowReader(Seq(t._1, t._2, metric, partTagsUTF8)) }
        .foreach(builder.addFromReader(_, Schemas.promCounter))
    }
    memStore.setup(timeseriesDatasetMultipleShardKeys.ref, Schemas(Schemas.promCounter), ishard, TestData.storeConf)
    memStore.ingest(timeseriesDatasetMultipleShardKeys.ref, ishard, SomeData(builder.allContainers.head, 0))
  }

  val jobQueryResult1 = ArrayBuffer(("job", "myCoolService"), ("unicode_tag", "uni\u03BCtag"))
  val jobQueryResult2 = Array("http_req_total", "http_resp_time")
  val jobQueryResult3 = Array("job", "__name__", "unicode_tag", "instance")

  override def beforeAll(): Unit = {
    for (ishard <- 0 until shardPartKeyLabelValues.size) {
      initShard(memStore, shardPartKeyLabelValues(ishard), ishard)
    }
    memStore.refreshIndexForTesting(timeseriesDatasetMultipleShardKeys.ref)
  }

  override def afterAll(): Unit = {
    memStore.shutdown()
  }

  val executeDispatcher = new PlanDispatcher {
    override def isLocalCall: Boolean = ???
    override def clusterName: String = ???
    override def dispatch(plan: ExecPlan, source: ChunkSource)
                         (implicit sched: Scheduler): Task[QueryResponse] = {
      plan.execute(memStore, querySession)(sched)
    }
  }

  implicit val testingBackend: SttpBackend[Future, Nothing] = SttpBackendStub.asynchronousFuture
    .whenRequestMatches(request =>
      request.body.toString.indexOf("empty=true") > -1
    )
    .thenRespondWrapped(Future {
      Response(Right(Right(MetadataSuccessResponse(Seq.empty, "success", Option.empty, Option.empty))), StatusCodes.PartialContent, "", Nil, Nil)
    })
    .whenRequestMatches(request =>
      request.uri.path.startsWith(List("api","v1","label")) && request.uri.path.last == "values"
    )
    .thenRespondWrapped(Future {
      Response(Right(Right(MetadataSuccessResponse(Seq(LabelSampl("http_req_total"), LabelSampl("http_resp_time")), "success", Option.empty, Option.empty))), StatusCodes.PartialContent, "", Nil, Nil)
    })
    .whenRequestMatches(request =>
      request.uri.path.startsWith(List("api","v1","series"))
    )
    .thenRespondWrapped(Future {
      Response(Right(Right(MetadataSuccessResponse(Seq(MetadataMapSampl(Map(("job" -> "myCoolService"), ("unicode_tag" -> "uniμtag")))), "success", Option.empty, Option.empty))), StatusCodes.PartialContent, "", Nil, Nil)
    })
    .whenRequestMatches(_.uri.path.startsWith(List("api","v1","labels"))
    )
    .thenRespondWrapped(Future {
      Response(Right(Right(MetadataSuccessResponse(Seq(LabelSampl("job"), LabelSampl("__name__"), LabelSampl("unicode_tag"), LabelSampl("instance")), "success", Option.empty, Option.empty))), StatusCodes.PartialContent, "", Nil, Nil)
    })

  it ("series matcher remote exec") {
    val exec: MetadataRemoteExec = MetadataRemoteExec("http://localhost:31007/api/v1/series", 10000L, Map("filter" -> "a=b,c=d"),
      QueryContext(origQueryParams=PromQlQueryParams("test", 123L, 234L, 15L, Option("http://localhost:31007/api/v1/series"))),
      InProcessPlanDispatcher(queryConfig), timeseriesDataset.ref, RemoteHttpClient(configBuilder.build(), testingBackend))

    val resp = exec.execute(memStore, querySession).runToFuture.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        val rv = response(0)
        rv.rows.size shouldEqual 1
        val record = rv.rows.next.asInstanceOf[BinaryRecordRowReader]
        rv.asInstanceOf[SerializedRangeVector].schema.toStringPairs(record.recordBase, record.recordOffset)
      }
    }
    result shouldEqual jobQueryResult1
  }

  it ("empty response series matcher remote exec") {
    val exec: MetadataRemoteExec = MetadataRemoteExec("http://localhost:31007/api/v1/series", 10000L, Map("filter" -> "a=b,c=d", "empty" -> "true"),
      QueryContext(origQueryParams=PromQlQueryParams("test", 123L, 234L, 15L, Option("http://localhost:31007/api/v1/series"))),
      InProcessPlanDispatcher(queryConfig), timeseriesDataset.ref, RemoteHttpClient(configBuilder.build(), testingBackend))

    val resp = exec.execute(memStore, querySession).runToFuture.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        val rv = response(0)
        rv.rows.size shouldEqual 0
        rv.rows.map { row =>
          val record = row.asInstanceOf[BinaryRecordRowReader]
          rv.asInstanceOf[SerializedRangeVector].schema.toStringPairs(record.recordBase, record.recordOffset)
        }
      }
    }
    result.toArray shouldEqual Array.empty
  }

  it ("label values remote metadata exec") {
    val exec: MetadataRemoteExec = MetadataRemoteExec("http://localhost:31007/api/v1/label/__name__/values", 10000L, Map("filter" -> "a=b,c=d"),
      QueryContext(origQueryParams=PromQlQueryParams("test", 123L, 234L, 15L, Option("http://localhost:31007/api/v1/label"))),
      InProcessPlanDispatcher(queryConfig), timeseriesDataset.ref, RemoteHttpClient(configBuilder.build(), testingBackend))

    val exec2: LabelValuesExec = LabelValuesExec(QueryContext(), executeDispatcher,
      timeseriesDataset.ref, 1, Seq(ColumnFilter("a", Equals("b"))), Seq("__name__"), 123L, 234L)
    val distConcatExec: LabelValuesDistConcatExec = LabelValuesDistConcatExec(QueryContext(), InProcessPlanDispatcher(queryConfig), Seq(exec2))

    val rootDistConcatExec: LabelValuesDistConcatExec = LabelValuesDistConcatExec(QueryContext(), InProcessPlanDispatcher(queryConfig) , Seq(distConcatExec, exec))
    val resp = rootDistConcatExec.execute(memStore, querySession).runToFuture.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        val rv = response(0)
        rv.rows.size shouldEqual 2
        rv.rows.map(row => {
          val record = row.asInstanceOf[BinaryRecordRowReader]
          rv.asInstanceOf[SerializedRangeVector].schema.toStringPairs(record.recordBase, record.recordOffset).head._2
        })
      }
    }
    result.toArray shouldEqual jobQueryResult2
  }

  it ("empty response label values remote metadata exec") {
    val exec: MetadataRemoteExec = MetadataRemoteExec("http://localhost:31007/api/v1/label/__name__/values", 10000L, Map("filter" -> "a=b,c=d", "empty" -> "true"),
      QueryContext(origQueryParams=PromQlQueryParams("test", 123L, 234L, 15L, Option("http://localhost:31007/api/v1/label"))),
      InProcessPlanDispatcher(queryConfig), timeseriesDataset.ref, RemoteHttpClient(configBuilder.build(), testingBackend))

    val exec2: LabelValuesExec = LabelValuesExec(QueryContext(), executeDispatcher,
      timeseriesDataset.ref, 1, Seq(ColumnFilter("a", Equals("b"))), Seq("__name__"), 123L, 234L)
    val distConcatExec: LabelValuesDistConcatExec = LabelValuesDistConcatExec(QueryContext(), InProcessPlanDispatcher(queryConfig), Seq(exec2))

    val rootDistConcatExec: LabelValuesDistConcatExec = LabelValuesDistConcatExec(QueryContext(), InProcessPlanDispatcher(queryConfig) , Seq(distConcatExec, exec))
    val resp = rootDistConcatExec.execute(memStore, querySession).runToFuture.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        response.flatMap(rv => {
          rv.rows.size shouldEqual 0
          rv.rows.map(row => {
            val record = row.asInstanceOf[BinaryRecordRowReader]
            rv.asInstanceOf[SerializedRangeVector].schema.toStringPairs(record.recordBase, record.recordOffset).head._2
          })
        })
      }
    }
    result.toArray shouldEqual Array.empty
  }

  it ("labels metadata remote exec") {
    val exec: MetadataRemoteExec = MetadataRemoteExec("http://localhost:31007/api/v1/labels", 10000L, Map("filter" -> "a=b,c=d"),
      QueryContext(origQueryParams=PromQlQueryParams("test", 123L, 234L, 15L, Option("http://localhost:31007/api/v1/labels"))),
      InProcessPlanDispatcher(queryConfig), timeseriesDataset.ref, RemoteHttpClient(configBuilder.build(), testingBackend))

    val resp = exec.execute(memStore, querySession).runToFuture.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        val rv = response(0)
        rv.rows.size shouldEqual 4
        rv.rows.map(row => {
          val record = row.asInstanceOf[BinaryRecordRowReader]
          rv.asInstanceOf[SerializedRangeVector].schema.toStringPairs(record.recordBase, record.recordOffset).head._2
        })
      }
    }
    result.toArray shouldEqual jobQueryResult3
  }

}

