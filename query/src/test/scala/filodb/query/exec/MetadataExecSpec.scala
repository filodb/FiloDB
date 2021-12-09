package filodb.query.exec

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import filodb.core.MetricsTestData._
import filodb.core.TestData
import filodb.core.binaryrecord2.BinaryRecordRowReader
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SomeData, TimeSeriesMemStore}
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.core.store.{InMemoryMetaStore, NullColumnStore}
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.query._
import filodb.query.exec.TsCardExec.CardCounts
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class MetadataExecSpec extends AnyFunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  import ZeroCopyUTF8String._

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

  val jobQueryResult1 = ArrayBuffer(("job", "myCoolService"), ("unicode_tag", "uni\u03C0tag"))
  val jobQueryResult2 = ArrayBuffer(("job", "myCoolService"), ("unicode_tag", "uni\u03BCtag"))

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

  override def beforeAll(): Unit = {
    for (ishard <- 0 until shardPartKeyLabelValues.size) {
      initShard(memStore, shardPartKeyLabelValues(ishard), ishard)
    }
    memStore.refreshIndexForTesting(timeseriesDatasetMultipleShardKeys.ref)
  }

  override def afterAll(): Unit = {
    memStore.shutdown()
  }

  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlan)
                         (implicit sched: Scheduler): Task[QueryResponse] = plan.execute(memStore,
      QuerySession(QueryContext(), queryConfig))(sched)

    override def clusterName: String = ???

    override def isLocalCall: Boolean = ???
  }

  val executeDispatcher = new PlanDispatcher {
    override def isLocalCall: Boolean = ???
    override def clusterName: String = ???
    override def dispatch(plan: ExecPlan)
                         (implicit sched: Scheduler): Task[QueryResponse] = {
      plan.execute(memStore, querySession)(sched)
    }
  }

  it ("should read the job names from timeseriesindex matching the columnfilters") {
    import ZeroCopyUTF8String._
    val filters = Seq(ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val leaves = (0 until shardPartKeyLabelValues.size).map{ ishard =>
      LabelValuesExec(QueryContext(), executeDispatcher, timeseriesDatasetMultipleShardKeys.ref,
                      ishard, filters, Seq("job", "unicode_tag"), now-5000, now)
    }.toSeq

    val execPlan = LabelValuesDistConcatExec(QueryContext(), executeDispatcher, leaves)

    val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        val rv = response(0)
        rv.rows.size shouldEqual 1
        val record = rv.rows.next().asInstanceOf[BinaryRecordRowReader]
        rv.asInstanceOf[SerializedRangeVector].schema.toStringPairs(record.recordBase, record.recordOffset)
      }
    }
    result shouldEqual jobQueryResult1
  }

  it("should not return any rows for wrong column filters") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total1".utf8)),
                       ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val leaves = (0 until shardPartKeyLabelValues.size).map { ishard =>
      PartKeysExec(QueryContext(), executeDispatcher, timeseriesDatasetMultipleShardKeys.ref,
                   ishard, filters, false, now-5000, now)
    }.toSeq

    val execPlan = PartKeysDistConcatExec(QueryContext(), executeDispatcher, leaves)

    val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
    (resp: @unchecked) match {
      case QueryResult(_, _, results, _, _, _) => results.size shouldEqual 1
        results(0).rows.size shouldEqual 0
    }
  }

  it("should read the label names/values from timeseriesindex matching the columnfilters") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val leaves = (0 until shardPartKeyLabelValues.size).map{ ishard =>
      PartKeysExec(QueryContext(), executeDispatcher, timeseriesDatasetMultipleShardKeys.ref,
                   ishard, filters, false, now-5000, now)
    }.toSeq

    val execPlan = PartKeysDistConcatExec(QueryContext(), executeDispatcher, leaves)

    val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) =>
        response.size shouldEqual 1
        response(0).rows.map { row =>
          val r = row.asInstanceOf[BinaryRecordRowReader]
          response(0).asInstanceOf[SerializedRangeVector]
            .schema.toStringPairs(r.recordBase, r.recordOffset).toMap
        }.toSet
      }
    result shouldEqual expectedLabelValues.toSet
  }

  it("should return one matching row (limit 1)") {
    import ZeroCopyUTF8String._
    val filters = Seq(ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    // Reducing limit results in truncated metadata response
    val execPlan = PartKeysExec(QueryContext(plannerParams = PlannerParams(sampleLimit = limit - 1)), executeDispatcher,
                                timeseriesDatasetMultipleShardKeys.ref, 0, filters, false, now - 5000, now)

    val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        response.size shouldEqual 1
        response(0).rows.map { row =>
          val r = row.asInstanceOf[BinaryRecordRowReader]
          response(0).asInstanceOf[SerializedRangeVector]
            .schema.toStringPairs(r.recordBase, r.recordOffset).toMap
        }.toList
      }
    }
    result shouldEqual List(expectedLabelValues(0))
  }

  it ("should be able to query labels with filter") {
    val expectedLabels = Set("job", "_metric_", "unicode_tag", "instance", "_ws_", "_ns_")
    val filters = Seq (ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val leaves = (0 until shardPartKeyLabelValues.size).map{ ishard =>
      LabelNamesExec(QueryContext(), executeDispatcher,
        timeseriesDatasetMultipleShardKeys.ref, ishard, filters, now-5000, now)
    }.toSeq

    val execPlan = LabelNamesDistConcatExec(QueryContext(), executeDispatcher, leaves)

    val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        val rv = response(0)
        rv.rows.size shouldEqual expectedLabels.size
        rv.rows.map(row => {
          val br = row.asInstanceOf[BinaryRecordRowReader]
          br.schema.colValues(br.recordBase, br.recordOffset, br.schema.colNames).head
        })
      }
    }
    result.toSet shouldEqual expectedLabels
  }

  it ("should be able to query with unicode filter") {
    val filters = Seq (ColumnFilter("unicode_tag", Filter.Equals("uni\u03BCtag".utf8)))

    val leaves = (0 until shardPartKeyLabelValues.size).map{ ishard =>
      LabelValuesExec(QueryContext(), executeDispatcher, timeseriesDatasetMultipleShardKeys.ref,
                      ishard, filters, Seq("job", "unicode_tag"), now-5000, now)
    }.toSeq

    val execPlan = LabelValuesDistConcatExec(QueryContext(), executeDispatcher, leaves)

    val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        val rv = response(0)
        rv.rows.size shouldEqual 1
        val record = rv.rows.next().asInstanceOf[BinaryRecordRowReader]
        rv.asInstanceOf[SerializedRangeVector].schema.toStringPairs(record.recordBase, record.recordOffset)
      }
    }
    result shouldEqual jobQueryResult2
  }

  it("should be able to query label cardinality") {
    // Tests all, LabelCardinalityExec, LabelCardinalityDistConcatExec and LabelCardinalityPresenter
    // Though we will search by ns, ws and metric name, technically we can search by any label in index
    val filters = Seq(ColumnFilter("instance", Filter.Equals("someHost:8787".utf8)))
    val qContext = QueryContext()

    val leaves = (0 until shardPartKeyLabelValues.size).map{ ishard =>
      LabelCardinalityExec(qContext, dummyDispatcher,
        timeseriesDatasetMultipleShardKeys.ref, ishard, filters, now - 5000, now)
    }.toSeq

    val execPlan = LabelCardinalityReduceExec(qContext, dummyDispatcher, leaves)
    execPlan.addRangeVectorTransformer(new LabelCardinalityPresenter())

    val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
    (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        response.size shouldEqual 2
        val rv1 = response(0)
        val rv2 = response(1)
        rv1.rows.size shouldEqual 1
        rv2.rows.size shouldEqual 1
        val record1 = rv1.rows.next().asInstanceOf[BinaryRecordRowReader]
        val result1 = rv1.asInstanceOf[SerializedRangeVector]
                          .schema.toStringPairs(record1.recordBase, record1.recordOffset).toMap

        val record2 = rv2.rows.next().asInstanceOf[BinaryRecordRowReader]
        val result2 = rv2.asInstanceOf[SerializedRangeVector]
                            .schema.toStringPairs(record2.recordBase, record2.recordOffset).toMap

        result1 shouldEqual Map("_ns_" -> "1",
          "unicode_tag" -> "1",
          "_type_" -> "1",
          "job" -> "1",
          "instance" -> "1",
          "_metric_" -> "1",
          "_ws_" -> "1")

        result2 shouldEqual Map("_ns_" -> "1",
          "unicode_tag" -> "1",
          "_type_" -> "1",
          "job" -> "1",
          "instance" -> "1",
          "_metric_" -> "1",
          "_ws_" -> "1")
      }
    }
  }

  it ("should correctly execute TsCardExec") {
    import filodb.query.exec.TsCardExec._

    case class TestSpec(shardKeyPrefix: Seq[String], groupDepth: Int, exp: Map[Seq[String], CardCounts])

    // Note: expected strings are eventually concatenated with a delimiter
    //   and converted to ZeroCopyUTF8Strings.
    Seq(
      TestSpec(Seq(), 0, Map(
        Seq("demo-A") -> CardCounts(1,1),
        Seq("demo") -> CardCounts(4,4))),
      TestSpec(Seq(), 1, Map(
        Seq("demo", "App-0") -> CardCounts(4,4),
        Seq("demo-A", "App-A") -> CardCounts(1,1))),
      TestSpec(Seq(), 2, Map(
        Seq("demo", "App-0", "http_foo_total") -> CardCounts(1,1),
        Seq("demo", "App-0", "http_req_total") -> CardCounts(2,2),
        Seq("demo", "App-0", "http_bar_total") -> CardCounts(1,1),
        Seq("demo-A", "App-A", "http_req_total-A") -> CardCounts(1,1))),
      TestSpec(Seq("demo"), 0, Map(
        Seq("demo") -> CardCounts(4,4))),
      TestSpec(Seq("demo"), 1, Map(
        Seq("demo", "App-0") -> CardCounts(4,4))),
      TestSpec(Seq("demo"), 2, Map(
        Seq("demo", "App-0", "http_foo_total") -> CardCounts(1,1),
        Seq("demo", "App-0", "http_req_total") -> CardCounts(2,2),
        Seq("demo", "App-0", "http_bar_total") -> CardCounts(1,1))),
      TestSpec(Seq("demo", "App-0"), 1, Map(
        Seq("demo", "App-0") -> CardCounts(4,4))),
      TestSpec(Seq("demo", "App-0"), 2, Map(
        Seq("demo", "App-0", "http_foo_total") -> CardCounts(1,1),
        Seq("demo", "App-0", "http_req_total") -> CardCounts(2,2),
        Seq("demo", "App-0", "http_bar_total") -> CardCounts(1,1))),
      TestSpec(Seq("demo", "App-0", "http_req_total"), 2, Map(
        Seq("demo", "App-0", "http_req_total") -> CardCounts(2,2)))
    ).foreach{ testSpec =>

      val leaves = (0 until shardPartKeyLabelValues.size).map{ ishard =>
        new TsCardExec(QueryContext(), executeDispatcher,
          timeseriesDatasetMultipleShardKeys.ref, ishard, testSpec.shardKeyPrefix, testSpec.groupDepth)
      }.toSeq

      val execPlan = TsCardReduceExec(QueryContext(), executeDispatcher, leaves)

      val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
      val result = (resp: @unchecked) match {
        case QueryResult(id, _, response, _, _, _) =>
          // should only have a single RangeVector
          response.size shouldEqual 1

          val resultMap = response(0).rows().map{r =>
            val data = RowData.fromRowReader(r)
            data.group -> data.counts
          }.toMap

          resultMap shouldEqual testSpec.exp.map { case (prefix, counts) =>
            prefixToGroup(prefix) -> counts
          }.toMap
      }
    }
  }
}
