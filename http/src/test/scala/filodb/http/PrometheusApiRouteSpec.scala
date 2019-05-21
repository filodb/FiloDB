package filodb.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import org.scalatest.FunSpec
import scala.concurrent.duration._

import filodb.coordinator._
import filodb.core.{AsyncTest, Success, TestData}
import filodb.prometheus.FormatConversion
import filodb.prometheus.query.PrometheusModel.ExplainPlanResponse

object PrometheusApiRouteSpec extends ActorSpecConfig

class PrometheusApiRouteSpec extends FunSpec with ScalatestRouteTest with AsyncTest {

  import FailFastCirceSupport._
  import NodeClusterActor._
  import io.circe.generic.auto._

  // Use our own ActorSystem with our test config so we can init cluster properly
  override def createActorSystem(): ActorSystem = ClusterApiRouteSpec.getNewSystem

  val cluster = FilodbCluster(system)
  val probe = TestProbe()
  implicit val timeout = RouteTestTimeout(10.minute)
  cluster.coordinatorActor
  cluster.join()
  val clusterProxy = cluster.clusterSingleton(ClusterRole.Server, None)
  val filoServerConfig = ConfigFactory.load("application_test.conf")
  val config = GlobalConfig.systemConfig

  val settings = new HttpSettings(config)
  val prometheusAPIRoute = (new PrometheusApiRoute(cluster.coordinatorActor, settings)).route

  private def setupDataset(): Unit = {
    val command = SetupDataset(FormatConversion.dataset.ref, DatasetResourceSpec(8, 1), noOpSource, TestData.storeConf)
    probe.send(clusterProxy, command)
    probe.expectMsg(DatasetVerified)
    // Give the coordinator nodes some time to get started
    Thread sleep 5000
  }

  before {
    probe.send(cluster.coordinatorActor, NodeProtocol.ResetState)
    probe.expectMsg(NodeProtocol.StateReset)
    // Note: at this point all ingestor actors are shut down
    cluster.metaStore.clearAllData().futureValue
    cluster.metaStore.newDataset(FormatConversion.dataset).futureValue shouldEqual Success
    probe.send(clusterProxy, NodeProtocol.ResetState)
    probe.expectMsg(NodeProtocol.StateReset)
  }

  it("should get explainPlan for query") {
    setupDataset()
    val query = "heap_usage{_ns=\"App-0\"}"

    Get(s"/promql/prometheus/api/v1/query_range?query=${query}&" +
      s"start=1555427432&end=1555447432&step=15&explainOnly=true") ~> prometheusAPIRoute ~> check {

      handled shouldBe true
      status shouldEqual StatusCodes.OK
      contentType shouldEqual ContentTypes.`application/json`
      val resp = responseAs[ExplainPlanResponse]
      resp.status shouldEqual "success"

      resp.debugInfo(0).toString should startWith("E~DistConcatExec()")
      resp.debugInfo(1) should startWith("-T~PeriodicSamplesMapper")
      resp.debugInfo(2) should startWith("--E~SelectRawPartitionsExec")
      resp.debugInfo(3) should startWith("-T~PeriodicSamplesMapper")
      resp.debugInfo(4) should startWith("--E~SelectRawPartitionsExec")
    }
  }

  it("should take spread override value from config for app") {
    setupDataset()
    val query = "heap_usage{_ns=\"App-0\"}"

    Get(s"/promql/prometheus/api/v1/query_range?query=${query}&" +
      s"start=1555427432&end=1555447432&step=15&explainOnly=true") ~> prometheusAPIRoute ~> check {

      handled shouldBe true
      status shouldEqual StatusCodes.OK
      contentType shouldEqual ContentTypes.`application/json`
      val resp = responseAs[ExplainPlanResponse]
      resp.status shouldEqual "success"
      resp.debugInfo.filter(_.startsWith("--E~SelectRawPartitionsExec")).length shouldEqual 4
    }
  }

  it("should get explainPlan for query based on spread as query parameter") {
    setupDataset()
    val query = "heap_usage{_ns=\"App-1\"}"

    Get(s"/promql/prometheus/api/v1/query_range?query=${query}&" +
      s"start=1555427432&end=1555447432&step=15&explainOnly=true&spread=3") ~> prometheusAPIRoute ~> check {

      handled shouldBe true
      status shouldEqual StatusCodes.OK
      contentType shouldEqual ContentTypes.`application/json`
      val resp = responseAs[ExplainPlanResponse]
      resp.status shouldEqual "success"
      resp.debugInfo.filter(_.startsWith("--E~SelectRawPartitionsExec")).length shouldEqual 8
    }
  }

    it("should take default spread value if no there is override") {
      setupDataset()
      val query = "heap_usage{_ns=\"App-1\"}"

      Get(s"/promql/prometheus/api/v1/query_range?query=${query}&" +
        s"start=1555427432&end=1555447432&step=15&explainOnly=true") ~> prometheusAPIRoute ~> check {

        handled shouldBe true
        status shouldEqual StatusCodes.OK
        contentType shouldEqual ContentTypes.`application/json`
        val resp = responseAs[ExplainPlanResponse]
        resp.status shouldEqual "success"
        resp.debugInfo.filter(_.startsWith("--E~SelectRawPartitionsExec")).length shouldEqual 2
      }

    }
}

