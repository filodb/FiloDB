package filodb.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.TestProbe
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport

import scala.concurrent.duration._
import filodb.coordinator._
import filodb.core.{AsyncTest, DatasetRef, TestData}
import filodb.core.metadata.{Dataset, Schemas}
import filodb.query.{ExplainPlanResponse, HistSampl, Sampl, SuccessResponse}
import org.scalatest.Ignore
import org.scalatest.funspec.AnyFunSpec


object PrometheusApiRouteSpec extends ActorSpecConfig {
  override lazy val configString = s"""
  filodb {
    memstore.groups-per-shard = 4
    inline-dataset-configs = [
      {
        dataset = "prometheus"
        schema = "gauge"
        num-shards = 4
        min-num-nodes = 1
        sourcefactory = "${classOf[NoOpStreamFactory].getName}"
        sourceconfig {
          shutdown-ingest-after-stopped = false
          ${TestData.sourceConfStr}
        }
      }
    ]
  }"""
}

@Ignore
// Prevent the test from getting detected, add a constructor arg
class PrometheusApiRouteSpec(ignore: String) extends AnyFunSpec with ScalatestRouteTest with AsyncTest {

  import FailFastCirceSupport._
  import io.circe.generic.auto._
  import filodb.core.{MachineMetricsData => MMD}

  // Use our own ActorSystem with our test config so we can init cluster properly
  // Dataset will be created and ingestion started
  override def createActorSystem(): ActorSystem = PrometheusApiRouteSpec.getNewSystem

  override def afterAll(): Unit = {
    FilodbSettings.reset()
    super.afterAll()
  }

  val cluster = FilodbCluster(system)
  val probe = TestProbe()
  implicit val timeout = RouteTestTimeout(20.minute)
  cluster.coordinatorActor
  cluster.join()
  val clusterProxy = cluster.clusterSingleton(ClusterRole.Server, None)

  val settings = new HttpSettings(PrometheusApiRouteSpec.config, cluster.settings)
  val prometheusAPIRoute = (new PrometheusApiRoute(cluster.coordinatorActor, settings)).route

  // Wait for cluster actor to start up and register datasets
  val ref = DatasetRef("prometheus")
  probe.send(clusterProxy, NodeClusterActor.GetShardMap(ref))
  probe.expectMsgPF(10.seconds) {
    case CurrentShardSnapshot(dsRef, mapper) => info(s"Got mapper for $dsRef => ${mapper.prettyPrint}")
  }

  // Use artifically early timestamp to ensure SingleCLusterPlanner magic doesn't kick in
  val histDataStart = 1000000000L
  val histData = MMD.linearPromHistSeries(startTs = histDataStart).take(100)
  val histDS = Dataset("histogram", Schemas.promHistogram)
  // NOTE: data gets sharded to shards 1 and 3
  cluster.memStore.ingest(ref, 1, MMD.records(histDS, histData))
  cluster.memStore.refreshIndexForTesting(ref)

  it("should get explainPlan for query") {
    val query = "heap_usage{_ws_=\"demo\",_ns_=\"App-0\"}"

    Get(s"/promql/prometheus/api/v1/query_range?query=$query&" +
      s"start=1000&end=1500&step=15&explainOnly=true") ~> prometheusAPIRoute ~> check {

      handled shouldBe true
      status shouldEqual StatusCodes.OK
      contentType shouldEqual ContentTypes.`application/json`
      val resp = responseAs[ExplainPlanResponse]
      resp.status shouldEqual "success"

      resp.debugInfo(0).toString should startWith("E~LocalPartitionDistConcatExec()")
      resp.debugInfo(1) should startWith("-T~PeriodicSamplesMapper")
      resp.debugInfo(2) should startWith("--E~MultiSchemaPartitionsExec")
      resp.debugInfo(3) should startWith("-T~PeriodicSamplesMapper")
      resp.debugInfo(4) should startWith("--E~MultiSchemaPartitionsExec")
    }
  }

  it("should take spread override value from config for app") {
    val query = "heap_usage{_ws_=\"demo\",_ns_=\"App-0\"}"

    Get(s"/promql/prometheus/api/v1/query_range?query=$query&" +
      s"start=1000&end=1500&step=15&explainOnly=true") ~> prometheusAPIRoute ~> check {

      handled shouldBe true
      status shouldEqual StatusCodes.OK
      contentType shouldEqual ContentTypes.`application/json`
      val resp = responseAs[ExplainPlanResponse]
      resp.status shouldEqual "success"
      resp.debugInfo.count(_.startsWith("--E~MultiSchemaPartitionsExec")) shouldEqual 4
    }
  }

  it("should get explainPlan for query based on spread as query parameter") {
    val query = "heap_usage{_ws_=\"demo\",_ns_=\"App-1\"}"

    Get(s"/promql/prometheus/api/v1/query_range?query=$query&" +
      s"start=1000&end=1500&step=15&explainOnly=true&spread=2") ~> prometheusAPIRoute ~> check {

      handled shouldBe true
      status shouldEqual StatusCodes.OK
      contentType shouldEqual ContentTypes.`application/json`
      val resp = responseAs[ExplainPlanResponse]
      resp.status shouldEqual "success"
      resp.debugInfo.count(_.startsWith("--E~MultiSchemaPartitionsExec")) shouldEqual 4
    }
  }

  it("should take default spread value if there is no override") {
    val query = "heap_usage{_ws_=\"demo\",_ns_=\"App-1\"}"

    Get(s"/promql/prometheus/api/v1/query_range?query=$query&" +
      s"start=1000&end=1500&step=15&explainOnly=true") ~> prometheusAPIRoute ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
      contentType shouldEqual ContentTypes.`application/json`
      val resp = responseAs[ExplainPlanResponse]
      resp.status shouldEqual "success"
      resp.debugInfo.filter(_.startsWith("--E~MultiSchemaPartitionsExec")).length shouldEqual 2
    }
  }

  import filodb.query.PromCirceSupport._

  it("should auto convert Histogram data to Prom bucket vectors") {
    val query = "request-latency{_ws_=\"demo\",_ns_=\"testapp\"}"

    val start = histDataStart / 1000
    val end   = start + 20000
    Get(s"/promql/prometheus/api/v1/query_range?query=${query}&" +
      s"start=$start&end=$end&step=15") ~> prometheusAPIRoute ~> check {

      handled shouldBe true
      status shouldEqual StatusCodes.OK
      contentType shouldEqual ContentTypes.`application/json`
      val resp = responseAs[SuccessResponse]
      resp.status shouldEqual "success"
      resp.data.result should have length (80)    // 8 vectors, one for each bucket * 10 series
      resp.data.result.foreach { res =>
        res.value shouldEqual None
        res.values.get.length should be > (1)
        res.values.get.foreach(_.isInstanceOf[Sampl])
        res.metric.contains("le") shouldEqual true
        res.metric("_metric_").endsWith("_bucket") shouldEqual true
      }
    }
  }

  it("should output native Histogram maps if histogramMap=true") {
    val query = "request-latency{_ws_=\"demo\",_ns_=\"testapp\"}"

    val start = histDataStart / 1000
    val end   = start + 20000
    Get(s"/promql/prometheus/api/v1/query_range?query=${query}&" +
      s"start=$start&end=$end&step=15&histogramMap=true") ~> prometheusAPIRoute ~> check {

      handled shouldBe true
      status shouldEqual StatusCodes.OK
      contentType shouldEqual ContentTypes.`application/json`
      val resp = responseAs[SuccessResponse]
      resp.status shouldEqual "success"
      resp.data.result should have length (10)    // 10 series, one histogram each, each with 8 buckets
      resp.data.result.foreach { res =>
        res.value shouldEqual None
        res.values.get.length should be > (1)
        res.values.get.foreach(_.asInstanceOf[HistSampl].buckets.size shouldEqual 8)
        res.metric.contains("le") shouldEqual false
        res.metric("_metric_").endsWith("_bucket") shouldEqual false
      }
    }
  }
}

