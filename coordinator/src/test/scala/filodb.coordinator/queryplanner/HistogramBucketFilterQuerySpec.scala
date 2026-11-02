package filodb.coordinator.queryplanner

import scala.concurrent.duration._

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
 * End to end PromQL tests for the `_bucket_` histogram bucket filter on delta and cumulative histograms.
 * The bucket is extracted from raw samples before the range function is applied, so the range function
 * (rate/increase) must still see the temporality of the source histogram column.
 */
class HistogramBucketFilterQuerySpec extends AnyFunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
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
  private val numSamples = 100
  private val buckets = bv.CustomBuckets(Array(10.0, 34.0, Double.PositiveInfinity))
  private val seriesTags = Map("_ws_".utf8 -> "demo".utf8, "_ns_".utf8 -> "testapp".utf8)

  // Every sample adds 1, 2, 3 observations to the 10, 34, +Inf buckets respectively (and sum of 50)
  // Cumulative histogram reports running totals; delta histogram reports per-interval increments
  private def cumulativeHistData = (0 until numSamples).map { n =>
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
    memStore.ingest(dsRef, 0, records(Schemas.promHistogram, cumulativeHistData))
    memStore.ingest(dsRef, 0, records(Schemas.deltaHistogram, deltaHistData))
    memStore.refreshIndexForTesting(dsRef)
  }

  override def afterAll(): Unit = {
    memStore.shutdown()
    system.terminate()
  }

  // Query a range well inside the ingested data so every 1m window is full
  private val queryStartSec = startMs / 1000 + 300
  private val queryEndSec = startMs / 1000 + 900
  private val numSteps = ((queryEndSec - queryStartSec) / 60 + 1).toInt

  private def query(promQL: String): Seq[Double] = {
    val lp = Parser.queryRangeToLogicalPlan(promQL, TimeStepParams(queryStartSec, 60, queryEndSec))
    val qContext = QueryContext(origQueryParams = PromQlQueryParams(promQL, queryStartSec, 60, queryEndSec))
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
    actual.size shouldEqual numSteps
    actual.foreach(_ shouldEqual (expected +- 0.000001))
  }

  private def assertSame(actual: Seq[Double], expected: Seq[Double]): Unit = {
    actual.size shouldEqual expected.size
    actual.zip(expected).foreach { case (a, e) => a shouldEqual (e +- 0.000001) }
  }

  // Two ways of selecting a single bucket. _bucket_ extracts the bucket from raw samples before the range function,
  // whereas the Prometheus style le filter on <metric>_bucket extracts the bucket after the range function.
  private val bucketSelectors: Seq[(String, String => String)] = Seq(
    "_bucket_ filter" -> (metric => s"""$metric{_ws_="demo",_ns_="testapp",_bucket_="34"}"""),
    "le filter"       -> (metric => s"""${metric}_bucket{_ws_="demo",_ns_="testapp",le="34"}""")
  )

  bucketSelectors.foreach { case (filterType, selectorFor) =>
    describe(s"$filterType on delta histogram") {
      val selector = selectorFor("delta_latency")

      it("should return the raw delta bucket value") {
        assertValues(query(selector), 2.0)
      }

      it("should compute rate using delta temporality") {
        // window [t-1m, t] is inclusive on both ends, so it has 7 samples, each with delta of 2 => 14 / 60s
        // If temporality were lost, the bucket would be treated as a cumulative counter that never increases => 0
        val result = query(s"rate($selector[1m])")
        assertValues(result, 14.0 / 60)
        assertSame(result, query("""histogram_bucket(34, rate(delta_latency{_ws_="demo",_ns_="testapp"}[1m]))"""))
      }

      it("should compute increase using delta temporality") {
        val result = query(s"increase($selector[1m])")
        assertValues(result, 14.0)
        assertSame(result,
                   query("""histogram_bucket(34, increase(delta_latency{_ws_="demo",_ns_="testapp"}[1m]))"""))
      }
    }

    describe(s"$filterType on cumulative histogram") {
      val selector = selectorFor("cumul_latency")

      it("should return the raw cumulative bucket value") {
        // value at time t is 2 * (sample number), with sample number = (t - start) / step + 1
        val expected = (0 until numSteps).map { s => 2.0 * ((queryStartSec + s * 60 - startMs / 1000) / 10 + 1) }
        assertSame(query(selector), expected)
      }

      it("should compute rate using cumulative temporality") {
        // counter grows by 2 every 10s
        // If treated as delta, the window's running totals would be summed instead => a much larger value
        val result = query(s"rate($selector[1m])")
        assertValues(result, 0.2)
        assertSame(result, query("""histogram_bucket(34, rate(cumul_latency{_ws_="demo",_ns_="testapp"}[1m]))"""))
      }

      it("should compute increase using cumulative temporality") {
        val result = query(s"increase($selector[1m])")
        assertValues(result, 12.0)
        assertSame(result,
                   query("""histogram_bucket(34, increase(cumul_latency{_ws_="demo",_ns_="testapp"}[1m]))"""))
      }
    }
  }
}
