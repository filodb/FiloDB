package filodb.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{StatusCodes, ContentTypes}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestProbe
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}
import org.scalatest.concurrent.ScalaFutures

import filodb.coordinator.{ActorSpecConfig, FilodbCluster, ClusterRole, NodeProtocol, NodeClusterActor}
import filodb.core.{GdeltTestData, TestData, Success}

object ClusterApiRouteSpec extends ActorSpecConfig

class ClusterApiRouteSpec extends FunSpec with Matchers with BeforeAndAfter
with ScalatestRouteTest with ScalaFutures {
  import FailFastCirceSupport._
  import io.circe.generic.auto._
  import NodeClusterActor._
  import GdeltTestData.dataset6
  import filodb.http.apiv1._

  // Use our own ActorSystem with our test config so we can init cluster properly
  override def createActorSystem(): ActorSystem = ClusterApiRouteSpec.getNewSystem

  val cluster = FilodbCluster(system)
  val probe = TestProbe()

  cluster.coordinatorActor
  cluster.join()
  val clusterProxy = cluster.clusterSingleton(ClusterRole.Server, None)
  val clusterRoute = (new ClusterApiRoute(clusterProxy)).route

  private def setupDataset(): Unit = {
    val command = SetupDataset(dataset6.ref, DatasetResourceSpec(4, 2), noOpSource, TestData.storeConf)
    probe.send(clusterProxy, command)
    probe.expectMsg(DatasetVerified)
  }

  before {
    probe.send(cluster.coordinatorActor, NodeProtocol.ResetState)
    cluster.metaStore.clearAllData().futureValue
    cluster.metaStore.newDataset(dataset6).futureValue shouldEqual Success
    probe.send(clusterProxy, NodeProtocol.ResetState)
    probe.expectMsg(NodeProtocol.StateReset)
    // Give enough time for old ingestor/query actors to die
    Thread sleep 500
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
      // Give the coordinator nodes some time to get started
      Thread sleep 1000

      Get(s"/api/v1/cluster/${dataset6.ref}/status") ~> clusterRoute ~> check {
        handled shouldBe true
        status shouldEqual StatusCodes.OK
        contentType shouldEqual ContentTypes.`application/json`
        val resp = responseAs[HttpList[HttpShardState]]
        resp.status shouldEqual "success"
        resp.data should have length 4
        resp.data.map(_.status).filter(_ contains "Active") should have length 2  // Two active nodes
      }
    }
  }

  describe("POST /api/v1/cluster/<dataset>") {
    it("should return 400 if config is not HOCON or JSON") {
      // Try sending YAML or something
      Post("/api/v1/cluster/timeseries",
           """datasettimeseries
             |  - numshards: 4
             |     - min-num-nodes: 2""".stripMargin) ~> clusterRoute ~> check {
        handled shouldBe true
        status shouldEqual StatusCodes.BadRequest
        contentType shouldEqual ContentTypes.`application/json`
        responseAs[HttpError].status shouldEqual "error"
        responseAs[HttpError].errorType should include ("ConfigException$Parse")
      }
    }

    it("should return 400 if config is HOCON/JSON but does not have needed members") {
      Post("/api/v1/cluster/timeseries",
           """dataset = "timeseries"
             |num-shards = 4""".stripMargin) ~> clusterRoute ~> check {
        handled shouldBe true
        status shouldEqual StatusCodes.BadRequest
        contentType shouldEqual ContentTypes.`application/json`
        responseAs[HttpError].status shouldEqual "error"
        responseAs[HttpError].errorType should include ("ConfigException$Missing")
      }
    }

    it("should return 200 if valid HOCON config, 409 if dataset already exists") {
      // Send the initial config, validate get back 200 and success
      val goodSourceConf = """dataset = "gdelt"
                             |num-shards = 4
                             |min-num-nodes = 2
                             |sourceconfig.store {
                             |  max-chunks-size = 100
                             |  demand-paged-chunk-retention-period = 10 hours
                             |  shard-mem-size = 256MB
                             |  groups-per-shard = 4
                             |  ingestion-buffer-mem-size = 50MB
                             |  flush-interval = 10 minutes
                             |} """.stripMargin
      Post("/api/v1/cluster/gdelt", goodSourceConf) ~> clusterRoute ~> check {
        handled shouldBe true
        status shouldEqual StatusCodes.OK
        contentType shouldEqual ContentTypes.`application/json`
        responseAs[HttpList[String]].status shouldEqual "success"
      }

      // POST original config again.  Should now get 409, "error" and DatasetExists
      Post("/api/v1/cluster/gdelt", goodSourceConf) ~> clusterRoute ~> check {
        handled shouldBe true
        status shouldEqual StatusCodes.getForKey(409).get
        contentType shouldEqual ContentTypes.`application/json`
        responseAs[HttpError].errorType should startWith ("DatasetExists")
      }
    }

    it("should return 200 if valid JSON config") {
      // Send the initial config, validate get back 200 and success
      val sourceJson = """{"dataset": "gdelt",
                             |"num-shards": 4,
                             |"min-num-nodes": 2,
                             |"sourceconfig": { "store": {
                             |  "flush-interval": "1h",
                             |  "shard-mem-size": "100MB"
                             |}}}""".stripMargin
      Post("/api/v1/cluster/gdelt", sourceJson) ~> clusterRoute ~> check {
        handled shouldBe true
        status shouldEqual StatusCodes.OK
        contentType shouldEqual ContentTypes.`application/json`
        responseAs[HttpList[String]].status shouldEqual "success"
      }

      // POST original config again.  Should now get 409, "error" and DatasetExists
      Post("/api/v1/cluster/gdelt", sourceJson) ~> clusterRoute ~> check {
        handled shouldBe true
        status shouldEqual StatusCodes.getForKey(409).get
        contentType shouldEqual ContentTypes.`application/json`
        responseAs[HttpError].errorType should startWith ("DatasetExists")
      }
    }
  }
}