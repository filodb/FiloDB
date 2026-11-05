package filodb.query.exec

import scala.concurrent.duration._

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core.MetricsTestData.timeseriesDatasetMultipleShardKeys
import filodb.core.TestData
import filodb.core.binaryrecord2.{BinaryRecordRowReader, RecordBuilder}
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SomeData, TimeSeriesMemStore}
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.core.store.{ChunkSource, InMemoryMetaStore, NullColumnStore}
import filodb.memory.MemFactory
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.query._

/**
 * Compares LabelCardinalityExec using index facets with LabelCardinalityExec reading the labels of the
 * matching part keys (query.label-cardinality-partkey-scan-enabled), the latter with faceting disabled
 * for all labels.
 */
class LabelCardinalityPartKeyScanSpec extends AnyFunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  import ZeroCopyUTF8String._

  implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val baseConfig: Config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val noFacetsConfig: Config = baseConfig
    .withValue("memstore.index-faceting-enabled-for-all-labels", ConfigValueFactory.fromAnyRef(false))
  val facetQueryConfig = QueryConfig(baseConfig.getConfig("query"))
  val scanQueryConfig = facetQueryConfig.copy(labelCardinalityPartKeyScanEnabled = true)

  val facetMemStore = new TimeSeriesMemStore(baseConfig, new NullColumnStore, new NullColumnStore,
    new InMemoryMetaStore(), Some(new FixedMaxPartitionsEvictionPolicy(1000)))
  val scanMemStore = new TimeSeriesMemStore(noFacetsConfig, new NullColumnStore, new NullColumnStore,
    new InMemoryMetaStore(), Some(new FixedMaxPartitionsEvictionPolicy(1000)))

  val ref = timeseriesDatasetMultipleShardKeys.ref
  val now = System.currentTimeMillis()
  val samples = (10 until 0).by(-1).map(n => (now - n * 10000L, n.toDouble))

  val commonTags = Map("_ws_" -> "demo", "_ns_" -> "App-0")
  val longValue = "v" * 1200

  val shardSeries: Seq[Seq[(String, Map[String, String])]] = Seq(
    Seq( // shard 0
      ("http_req_total", commonTags ++ Map("instance" -> "host1:8787", "job" -> "svc", "unicode_tag" -> "uniπtag")),
      ("http_req_total", commonTags ++ Map("instance" -> "host2:8787", "job" -> "svc", "unicode_tag" -> "uniπtag")),
      ("http_foo_total", commonTags ++ Map("instance" -> "host1:8787", "job" -> "svc", "unicode_tag" -> "uniμtag")),
      ("long_value_total", commonTags ++ Map("instance" -> "host1:8787", "long" -> longValue)),
      ("long_value_total", commonTags ++ Map("instance" -> "host2:8787", "long" -> (longValue + "2")))
    ) ++ (0 until 150).map { i =>
      // every series has its own label, as with dynamic labels
      ("dynamic_labels", commonTags ++ Map("instance" -> s"host$i:8787", s"label_$i" -> "value"))
    },
    Seq( // shard 1
      ("http_req_total", commonTags ++ Map("instance" -> "host3:9090", "job" -> "svc", "unicode_tag" -> "uniπtag")),
      ("http_bar_total", commonTags ++ Map("instance" -> "host1:8787", "job" -> "svc", "unicode_tag" -> "uniπtag")),
      ("http_req_total", Map("_ws_" -> "demo-A", "_ns_" -> "App-A", "instance" -> "host4:9090", "job" -> "svc"))
    ) ++ (0 until 250).map { i =>
      // more labels than maxLabels, with long names
      ("many_labels", commonTags ++ Map("instance" -> s"host$i:8787", s"kubernetes_io_dynamic_label_name_$i" -> "v"))
    }
  )

  private def ingest(memStore: TimeSeriesMemStore): Unit = {
    shardSeries.zipWithIndex.foreach { case (series, shard) =>
      memStore.setup(ref, Schemas(Schemas.promCounter), shard, TestData.storeConf, 1)
      series.foreach { case (metric, tags) =>
        val builder = new RecordBuilder(MemFactory.onHeapFactory)
        val tagsUtf8 = tags.map { case (k, v) => (k.utf8, v.utf8) }
        samples.foreach { case (ts, v) =>
          builder.addFromReader(SeqRowReader(Seq(ts, v, metric, tagsUtf8)), Schemas.promCounter)
        }
        builder.allContainers.foreach(c => memStore.ingest(ref, shard, SomeData(c, 0)))
      }
    }
    memStore.refreshIndexForTesting(ref)
  }

  override def beforeAll(): Unit = {
    ingest(facetMemStore)
    ingest(scanMemStore)
  }

  override def afterAll(): Unit = {
    facetMemStore.shutdown()
    scanMemStore.shutdown()
  }

  private def dispatcher(memStore: TimeSeriesMemStore, queryConfig: QueryConfig) = new PlanDispatcher {
    override def dispatch(plan: ExecPlanWithClientParams, source: ChunkSource)
                         (implicit sched: Scheduler): Task[QueryResponse] =
      plan.execPlan.execute(memStore, QuerySession(plan.execPlan.queryContext, queryConfig,
        preventRangeVectorSerialization = plan.clientParams.preventRangeVectorSerialization))(sched)
    override def clusterName: String = ???
    override def isLocalCall: Boolean = true
    override def dispatchStreaming(plan: ExecPlanWithClientParams, source: ChunkSource)
                                  (implicit sched: Scheduler): Observable[StreamQueryResponse] = ???
  }

  private def labelCardinality(scan: Boolean, filters: Seq[ColumnFilter]): (Map[String, Int], Boolean, Option[String]) = {
    val (memStore, queryConfig) = if (scan) (scanMemStore, scanQueryConfig) else (facetMemStore, facetQueryConfig)
    val qContext = QueryContext()
    val leaves = shardSeries.indices.map { shard =>
      LabelCardinalityExec(qContext, dispatcher(memStore, queryConfig), ref, shard, filters, now - 5000, now)
    }
    val plan = LabelCardinalityReduceExec(qContext, dispatcher(memStore, queryConfig), leaves)
    plan.addRangeVectorTransformer(new LabelCardinalityPresenter())
    (plan.execute(memStore, QuerySession(qContext, queryConfig)).runToFuture.futureValue: @unchecked) match {
      case QueryResult(_, _, response, _, _, mayBePartial, partialReason) =>
        val result = response.headOption.map { rv =>
          val record = rv.rows().next().asInstanceOf[BinaryRecordRowReader]
          rv.asInstanceOf[SerializedRangeVector].schema.toStringPairs(record.recordBase, record.recordOffset)
            .map { case (label, count) => (label, count.toInt) }.toMap
        }.getOrElse(Map.empty[String, Int])
        (result, mayBePartial, partialReason)
    }
  }

  private def filtersFor(metric: String, ws: String = "demo", ns: String = "App-0") =
    Seq(ColumnFilter("_ws_", Filter.Equals(ws)), ColumnFilter("_ns_", Filter.Equals(ns)),
      ColumnFilter("_metric_", Filter.Equals(metric)))

  it("should compute the same label cardinality as the faceted lookups") {
    val expected = Map("_ws_" -> 1, "_ns_" -> 1, "_metric_" -> 1, "_type_" -> 1,
      "instance" -> 3, "job" -> 1, "unicode_tag" -> 1)
    labelCardinality(scan = false, filtersFor("http_req_total")) shouldEqual (expected, false, None)
    labelCardinality(scan = true, filtersFor("http_req_total")) shouldEqual (expected, false, None)

    Seq(filtersFor("http_foo_total"), filtersFor("http_bar_total"), filtersFor("http_req_total", "demo-A", "App-A"),
      Seq(ColumnFilter("_ws_", Filter.Equals("demo")), ColumnFilter("_ns_", Filter.Equals("App-0")),
        ColumnFilter("_metric_", Filter.EqualsRegex("http_req_total|http_foo_total|http_bar_total"))),
      Seq(ColumnFilter("_ws_", Filter.Equals("demo")), ColumnFilter("job", Filter.Equals("svc")))
    ).foreach { filters =>
      val scanResult = labelCardinality(scan = true, filters)
      scanResult._1 should not be empty
      scanResult shouldEqual labelCardinality(scan = false, filters)
    }
    labelCardinality(scan = true, filtersFor("no_such_metric")) shouldEqual (Map.empty, false, None)
  }

  it("should count label values of 1000 or more chars that are not faceted") {
    labelCardinality(scan = false, filtersFor("long_value_total"))._1.get("long") shouldEqual None
    labelCardinality(scan = true, filtersFor("long_value_total"))._1("long") shouldEqual 2
  }

  it("should find all label names of the matching series") {
    val (result, mayBePartial, _) = labelCardinality(scan = true, filtersFor("dynamic_labels"))
    result.keySet shouldEqual (0 until 150).map(i => s"label_$i").toSet ++
      Set("_ws_", "_ns_", "_metric_", "_type_", "instance")
    mayBePartial shouldEqual false
  }

  it("should return at most maxLabels labels and mark the result partial") {
    val (result, mayBePartial, partialReason) = labelCardinality(scan = true, filtersFor("many_labels"))
    result.size shouldEqual 200
    Seq("_ws_", "_ns_", "_metric_", "_type_", "instance").foreach(label => result.contains(label) shouldEqual true)
    mayBePartial shouldEqual true
    partialReason shouldEqual Some("Matching time series have more than 200 labels")
  }

  it("should return label names with filters when faceting is disabled for all labels") {
    val exec = LabelNamesExec(QueryContext(), dispatcher(scanMemStore, scanQueryConfig), ref, 0,
      filtersFor("http_req_total"), now - 5000, now)
    (exec.execute(scanMemStore, QuerySession(QueryContext(), scanQueryConfig)).runToFuture.futureValue: @unchecked) match {
      case QueryResult(_, _, response, _, _, _, _) =>
        response.flatMap(_.rows().map { row =>
          val br = row.asInstanceOf[BinaryRecordRowReader]
          br.schema.colValues(br.recordBase, br.recordOffset, br.schema.colNames).head
        }.toList).toSet shouldEqual Set("_ws_", "_ns_", "_metric_", "_type_", "instance", "job", "unicode_tag")
    }
  }
}
