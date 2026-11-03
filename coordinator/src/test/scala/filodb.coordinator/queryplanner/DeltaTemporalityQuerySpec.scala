package filodb.coordinator.queryplanner

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler.Implicits.global
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.core.{DatasetRef, SpreadChange, TestData}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.{SomeData, TimeSeriesMemStore}
import filodb.core.metadata.{Dataset, Schema, Schemas}
import filodb.core.query._
import filodb.core.store.{InMemoryMetaStore, NullColumnStore}
import filodb.memory.MemFactory
import filodb.memory.format.{ZeroCopyUTF8String, vectors => bv}
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.QueryResult

/**
 * End to end PromQL tests that check delta temporality survives every step of a query plan.
 * A column with delta=true holds per-interval values. rate/increase must add them up. If a plan step resets
 * the column to cumulative, rate/increase compare first and last value of a counter that never grows.
 * Each delta test has a cumulative twin, so a fix for one type cannot break the other.
 */
class DeltaTemporalityQuerySpec extends AnyFunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  import ZeroCopyUTF8String._

  implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(30, Seconds),
                                                                interval = Span(250, Millis))
  implicit val system: ActorSystem = ActorSystem()

  private val config = ConfigFactory.load("application_test.conf").resolve()
  private val queryConfig = QueryConfig(config.getConfig("filodb.query"))
  private val memStore = new TimeSeriesMemStore(config.getConfig("filodb"), new NullColumnStore,
                                                new NullColumnStore, new InMemoryMetaStore())

  private val dsRef = DatasetRef("prometheus")
  private val dataset = Dataset(dsRef.dataset, Schemas.promHistogram)

  private val mapper = new ShardMapper(1)
  mapper.registerNode(Seq(0), TestProbe().ref)
  private val planner = new SingleClusterPlanner(dataset, Schemas.global, mapper, earliestRetainedTimestampFn = 0,
    queryConfig, "raw", spreadProvider = StaticSpreadProvider(SpreadChange(0, 0)))

  // Use artificially early timestamp so data is well within the memstore and no time based routing kicks in
  private val startMs = 1000000000000L
  private val stepMs = 10000L
  private val numSamples = 200
  private val buckets = bv.CustomBuckets(Array(10.0, 34.0, Double.PositiveInfinity))
  private val seriesTags = Map("_ws_".utf8 -> "demo".utf8, "_ns_".utf8 -> "testapp".utf8)

  // Every sample adds 5 to the counter. The cumulative counter reports running totals, the delta counter
  // reports the per-interval increment.
  private def cumulCounterData = (0 until numSamples).map { n =>
    Seq(startMs + n * stepMs, 5.0 * (n + 1), "cumul_counter", seriesTags)
  }
  private def deltaCounterData = (0 until numSamples).map { n =>
    Seq(startMs + n * stepMs, 5.0, "delta_counter", seriesTags)
  }

  // Every sample adds 1, 2, 3 observations to the 10, 34, +Inf buckets
  private def cumulHistData = (0 until numSamples).map { n =>
    val i = n + 1
    Seq(startMs + n * stepMs, 50.0 * i, 3.0 * i,
        bv.LongHistogram(buckets, Array(1L * i, 2L * i, 3L * i)), "cumul_latency", seriesTags)
  }
  private def deltaHistData = (0 until numSamples).map { n =>
    Seq(startMs + n * stepMs, 50.0, 3.0, bv.LongHistogram(buckets, Array(1L, 2L, 3L)), "delta_latency", seriesTags)
  }

  private def records(schema: Schema, rows: Seq[Seq[Any]]): SomeData = {
    val builder = new RecordBuilder(MemFactory.onHeapFactory)
    rows.foreach { row =>
      builder.startNewRecord(schema)
      row.foreach(builder.addSlowly)
      builder.endRecord()
    }
    SomeData(builder.allContainers.head, 0)
  }

  override def beforeAll(): Unit = {
    memStore.setup(dsRef, Schemas.global, 0, TestData.storeConf, 1)
    memStore.ingest(dsRef, 0, records(Schemas.promCounter, cumulCounterData))
    memStore.ingest(dsRef, 0, records(Schemas.deltaCounter, deltaCounterData))
    memStore.ingest(dsRef, 0, records(Schemas.promHistogram, cumulHistData))
    memStore.ingest(dsRef, 0, records(Schemas.deltaHistogram, deltaHistData))
    memStore.refreshIndexForTesting(dsRef)
  }

  override def afterAll(): Unit = {
    memStore.shutdown()
    system.terminate()
  }

  // Query a range well inside the ingested data so every window is full
  private val queryStartSec = startMs / 1000 + 600
  private val queryEndSec = startMs / 1000 + 1200
  private val queryStepSec = 60

  private def query(promQL: String): Seq[Double] = {
    val lp = Parser.queryRangeToLogicalPlan(promQL, TimeStepParams(queryStartSec, queryStepSec, queryEndSec))
    val qContext = QueryContext(origQueryParams = PromQlQueryParams(promQL, queryStartSec, queryStepSec, queryEndSec))
    val querySession = QuerySession(qContext, queryConfig)
    val plans = planner.walkLogicalPlanTree(lp, qContext, forceInProcess = true).plans
    val rvs = plans.flatMap { plan =>
      plan.execute(memStore, querySession).runToFuture.futureValue match {
        case qr: QueryResult => qr.result
        case other           => fail(s"Unexpected response for $promQL: $other")
      }
    }
    rvs.size shouldEqual 1
    rvs.head.rows().map(_.getDouble(1)).toList
  }

  private def assertValues(actual: Seq[Double], expected: Double): Unit = {
    actual.size shouldEqual ((queryEndSec - queryStartSec) / queryStepSec + 1)
    actual.foreach(_ shouldEqual (expected +- 0.01))
  }

  private val sel = """{_ws_="demo",_ns_="testapp"}"""

  describe("delta counter") {
    it("should compute rate and increase from the per-interval values") {
      // 1m window is inclusive on both ends: 7 samples of 5 each = 35 over 60s
      assertValues(query(s"rate(delta_counter$sel[1m])"), 35.0 / 60)
      assertValues(query(s"increase(delta_counter$sel[1m])"), 35.0)
    }

    it("should keep delta temporality through the subquery step") {
      // The inner step samples the counter every 1m. The window has 5 inner samples of 5 each = 25 over 300s.
      // If the subquery step resets the column to cumulative, the samples never grow, and the result is 0.
      assertValues(query(s"rate(delta_counter$sel[5m:1m])"), 25.0 / 300)
      assertValues(query(s"increase(delta_counter$sel[5m:1m])"), 25.0)
    }

    it("should keep delta temporality through an aggregate over the subquery") {
      assertValues(query(s"sum(rate(delta_counter$sel[5m:1m]))"), 25.0 / 300)
      assertValues(query(s"sum by (_ns_) (increase(delta_counter$sel[5m:1m]))"), 25.0)
    }
  }

  describe("cumulative counter") {
    it("should compute rate and increase from the running total") {
      // counter grows by 5 every 10s
      assertValues(query(s"rate(cumul_counter$sel[1m])"), 0.5)
      assertValues(query(s"increase(cumul_counter$sel[1m])"), 30.0)
    }

    it("should keep cumulative temporality through the subquery step") {
      // inner samples are running totals, one per minute: the counter grows by 30 per inner step
      assertValues(query(s"rate(cumul_counter$sel[5m:1m])"), 0.5)
      assertValues(query(s"increase(cumul_counter$sel[5m:1m])"), 150.0)
      assertValues(query(s"sum(rate(cumul_counter$sel[5m:1m]))"), 0.5)
    }
  }

  describe("delta histogram bucket followed by a range function") {
    it("should keep delta temporality through histogram_bucket and the subquery step") {
      // bucket 34 holds 2 observations per sample. The window has 5 inner samples = 10 over 300s.
      // Both histogram_bucket and the subquery step must keep the delta temporality.
      assertValues(query(s"rate(histogram_bucket(34, delta_latency$sel)[5m:1m])"), 10.0 / 300)
      assertValues(query(s"increase(histogram_bucket(34, delta_latency$sel)[5m:1m])"), 10.0)
    }

    it("should keep delta temporality through histogram_quantile and the subquery step") {
      // Every sample has the same shape, so the quantile is constant. The delta rate sums 5 samples over 300s.
      val q = query(s"histogram_quantile(0.5, delta_latency$sel)")
      val p50 = q.head
      q.foreach(_ shouldEqual (p50 +- 0.0001))
      assertValues(query(s"increase(histogram_quantile(0.5, delta_latency$sel)[5m:1m])"), 5 * p50)
    }
  }

  describe("cumulative histogram bucket followed by a range function") {
    it("should keep cumulative temporality through histogram_bucket and the subquery step") {
      assertValues(query(s"rate(histogram_bucket(34, cumul_latency$sel)[5m:1m])"), 0.2)
      assertValues(query(s"increase(histogram_bucket(34, cumul_latency$sel)[5m:1m])"), 60.0)
    }
  }
}
