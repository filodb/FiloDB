package filodb.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestProbe
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport

import filodb.coordinator._
import filodb.core.{AsyncTest, GdeltTestData, TestData}
import filodb.core.store.{AssignShardConfig, UnassignShardConfig}
import org.scalatest.funspec.AnyFunSpec

object ClusterApiRouteSpec extends ActorSpecConfig

class ClusterApiRouteSpec extends AnyFunSpec with ScalatestRouteTest with AsyncTest {
  import FailFastCirceSupport._
  import io.circe.generic.auto._

  import GdeltTestData.dataset6
  import NodeClusterActor._
  import filodb.http.apiv1._

  // Use our own ActorSystem with our test config so we can init cluster properly
  override def createActorSystem(): ActorSystem = ClusterApiRouteSpec.getNewSystem

  override def afterAll(): Unit = {
    FilodbSettings.reset()
    super.afterAll()
  }

  val cluster = FilodbCluster(system)
  val probe = TestProbe()

  cluster.coordinatorActor
  cluster.join()
  val clusterProxy = cluster.clusterSingleton(ClusterRole.Server, None)
  val clusterRoute = (new ClusterApiRoute(clusterProxy)).route
  Thread sleep 2000

  private def setupDataset(): Unit = {
    val command = SetupDataset(dataset6, DatasetResourceSpec(4, 2), noOpSource, TestData.storeConf)
    probe.send(clusterProxy, command)
    probe.expectMsg(DatasetVerified)
    probe.send(cluster.coordinatorActor, command)
    Thread.sleep(2500)
  }

  before {
    probe.send(cluster.coordinatorActor, NodeProtocol.ResetState)
    probe.expectMsg(NodeProtocol.StateReset)
    // Note: at this point all ingestor actors are shut down
    probe.send(clusterProxy, NodeProtocol.ResetState)
    probe.expectMsg(NodeProtocol.StateReset)
  }

  describe("get datasets route") {
    it("should return empty list when no datasets registered") {
      Get("/api/v1/cluster") ~> clusterRoute ~> check {
        handled shouldBe true
        status shouldEqual StatusCodes.OK
        responseAs[HttpList[String]] shouldEqual HttpList("success", Seq.empty[String])
      }
    }

    it("should return list of registered datasets") {
      setupDataset()
      Get("/api/v1/cluster") ~> clusterRoute ~> check {
        handled shouldBe true
        status shouldEqual StatusCodes.OK
        contentType shouldEqual ContentTypes.`application/json`
        responseAs[HttpList[String]] shouldEqual HttpList("success", Seq(dataset6.ref.toString))
      }
    }
  }

  describe("/api/v1/cluster/<dataset>/status") {
    it("should return 404 dataset not found if dataset not registered") {
      Get("/api/v1/cluster/foobar/status") ~> clusterRoute ~> check {
        handled shouldBe true
        status shouldEqual StatusCodes.NotFound
        contentType shouldEqual ContentTypes.`application/json`
        responseAs[HttpError].status shouldEqual "error"
      }
    }

    it("should return shard status after dataset is setup") {
      setupDataset()

      Get(s"/api/v1/cluster/${dataset6.ref}/status") ~> clusterRoute ~> check {
        handled shouldBe true
        status shouldEqual StatusCodes.OK
        contentType shouldEqual ContentTypes.`application/json`
        val resp = responseAs[HttpList[HttpShardState]]
        resp.status shouldEqual "success"
        resp.data should have length 4
        // Exact status of assigned nodes doesn't matter much.  This is an HTTP route test, not a sharding test
        resp.data.map(_.status).filter(_ contains "Unassigned") should have length 2  // Two unassigned nodes
      }
    }

    it("should return shard status groupByAddress after dataset is setup") {
      setupDataset()
      Get(s"/api/v1/cluster/${dataset6.ref}/statusByAddress") ~> clusterRoute ~> check {
        handled shouldBe true
        status shouldEqual StatusCodes.OK
        contentType shouldEqual ContentTypes.`application/json`
        val resp = responseAs[HttpList[HttpShardStateByAddress]]
        resp.status shouldEqual "success"
      }
    }
  }

  describe("/api/v1/cluster/<dataset>/startshards") {
    it("should return 200 with valid config") {
      val conf = AssignShardConfig("akka.tcp://filo-standalone@127.0.0.1:25523", Seq(2, 5))
      Post("/api/v1/cluster/gdelt/startshards", conf).
        withHeaders(RawHeader("Content-Type", "application/json")) ~> clusterRoute ~> check {
        handled shouldBe true
        status shouldEqual StatusCodes.BadRequest
        responseAs[HttpError].status shouldEqual "error"
        responseAs[HttpError].error shouldEqual "DatasetUnknown(gdelt)"
      }
    }
  }

  describe("/api/v1/cluster/<dataset>/stopshards") {
    it("should return 200 with valid config") {
      val conf = UnassignShardConfig(Seq(2, 5))
      Post("/api/v1/cluster/gdelt/stopshards", conf).
        withHeaders(RawHeader("Content-Type", "application/json")) ~> clusterRoute ~> check {
        handled shouldBe true
        status shouldEqual StatusCodes.BadRequest
        responseAs[HttpError].status shouldEqual "error"
        responseAs[HttpError].error shouldEqual "DatasetUnknown(gdelt)"
      }
    }
  }

}