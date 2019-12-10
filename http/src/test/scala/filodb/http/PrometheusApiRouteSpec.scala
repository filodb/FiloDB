package filodb.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.TestProbe
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import org.scalatest.FunSpec

import scala.concurrent.duration._
import filodb.coordinator._
import filodb.core.{AsyncTest, DatasetRef, TestData}
import filodb.query.ExplainPlanResponse


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

class PrometheusApiRouteSpec extends FunSpec with ScalatestRouteTest with AsyncTest {

  import FailFastCirceSupport._
  import io.circe.generic.auto._

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

  val settings = new HttpSettings(PrometheusApiRouteSpec.config)
  val prometheusAPIRoute = (new PrometheusApiRoute(cluster.coordinatorActor, settings)).route

  // Wait for cluster actor to start up and register datasets
  val ref = DatasetRef("prometheus")
  probe.send(clusterProxy, NodeClusterActor.GetShardMap(ref))
  probe.expectMsgPF(10.seconds) {
    case CurrentShardSnapshot(ref, mapper) => info(s"Got mapper for $ref => ${mapper.prettyPrint}")
  }

  it("should get explainPlan for query") {
    val query = "heap_usage{_ws_=\"demo\",_ns_=\"App-0\"}"

    Get(s"/promql/prometheus/api/v1/query_range?query=${query}&" +
      s"start=1555427432&end=1555447432&step=15&explainOnly=true") ~> prometheusAPIRoute ~> check {

      handled shouldBe true
      status shouldEqual StatusCodes.OK
      contentType shouldEqual ContentTypes.`application/json`
      val resp = responseAs[ExplainPlanResponse]
      resp.status shouldEqual "success"

      resp.debugInfo(0).toString should startWith("E~DistConcatExec()")
      resp.debugInfo(1) should startWith("-T~PeriodicSamplesMapper")
      resp.debugInfo(2) should startWith("--E~MultiSchemaPartitionsExec")
      resp.debugInfo(3) should startWith("-T~PeriodicSamplesMapper")
      resp.debugInfo(4) should startWith("--E~MultiSchemaPartitionsExec")
    }
  }

  it("should take spread override value from config for app") {
    val query = "heap_usage{_ws_=\"demo\",_ns_=\"App-0\"}"

    Get(s"/promql/prometheus/api/v1/query_range?query=${query}&" +
      s"start=1555427432&end=1555447432&step=15&explainOnly=true") ~> prometheusAPIRoute ~> check {

      handled shouldBe true
      status shouldEqual StatusCodes.OK
      contentType shouldEqual ContentTypes.`application/json`
      val resp = responseAs[ExplainPlanResponse]
      resp.status shouldEqual "success"
      resp.debugInfo.filter(_.startsWith("--E~MultiSchemaPartitionsExec")).length shouldEqual 4
    }
  }

  it("should get explainPlan for query based on spread as query parameter") {
    val query = "heap_usage{_ws_=\"demo\",_ns_=\"App-1\"}"

    Get(s"/promql/prometheus/api/v1/query_range?query=${query}&" +
      s"start=1555427432&end=1555447432&step=15&explainOnly=true&spread=2") ~> prometheusAPIRoute ~> check {

      handled shouldBe true
      status shouldEqual StatusCodes.OK
      contentType shouldEqual ContentTypes.`application/json`
      val resp = responseAs[ExplainPlanResponse]
      resp.status shouldEqual "success"
      resp.debugInfo.filter(_.startsWith("--E~MultiSchemaPartitionsExec")).length shouldEqual 4
    }
  }

    it("should take default spread value if there is no override") {
      val query = "heap_usage{_ws_=\"demo\",_ns_=\"App-1\"}"

      Get(s"/promql/prometheus/api/v1/query_range?query=${query}&" +
        s"start=1555427432&end=1555447432&step=15&explainOnly=true") ~> prometheusAPIRoute ~> check {

        handled shouldBe true
        status shouldEqual StatusCodes.OK
        contentType shouldEqual ContentTypes.`application/json`
        val resp = responseAs[ExplainPlanResponse]
        resp.status shouldEqual "success"
        resp.debugInfo.filter(_.startsWith("--E~MultiSchemaPartitionsExec")).length shouldEqual 2
      }
    }
}

