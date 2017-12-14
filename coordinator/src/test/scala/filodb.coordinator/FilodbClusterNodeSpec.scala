package filodb.coordinator

import scala.concurrent.duration._

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.concurrent.ScalaFutures

import filodb.core.NamesTestData
import filodb.coordinator.client.LocalClient

class FilodbClusterNodeSpec extends RunnableSpec with ScalaFutures {
  import NodeClusterActor._

  "A FiloServer Node" must {
    val app = new FiloServerApp
    app.main(Array.empty)

    "join the cluster" in {
      TestKit.awaitCond(app.cluster.isJoined, 10.seconds)
    }
    "create and setup the coordinatorActor and clusterActor" in {
      val coordinatorActor = app.coordinatorActor
      val clusterActor = app.clusterActor

      implicit val system = app.system
      val probe = TestProbe()
      probe.send(coordinatorActor, CoordinatorRegistered(clusterActor))
      probe.send(coordinatorActor, MiscCommands.GetClusterActor)
      probe.expectMsgPF() {
        case Some(ref: ActorRef) => ref shouldEqual clusterActor
      }
    }
    "shutdown cleanly" in {
      app.shutdown()
      TestKit.awaitCond(app.cluster.isTerminated, 3.seconds)
    }
  }
}

/**
 * Initiates cluster singleton recovery sequence by populating guardian with some initial non-empty
 * shardmap and subscription state, and checking that it is recovered properly on startup
 */
class FilodbClusterNodeRecoverySpec extends RunnableSpec with ScalaFutures {
  import NodeClusterActor._
  import NodeProtocol._
  import NamesTestData._

  "A FiloServer Node" must {
    val app = new FiloServerApp
    app.main(Array.empty)

    implicit val system = app.system
    val probe = TestProbe()
    val map = new ShardMapper(8)

    "join the cluster" in {
      TestKit.awaitCond(app.cluster.isJoined, 10.seconds)
    }
    "create and setup the coordinatorActor and clusterActor" in {
      val coordinatorActor = app.coordinatorActor

      // Now, pre-populate Guardian with shard state and subscribers before starting cluster actor
      // send stuff to guardian
      (0 to 3).foreach { s => map.updateFromEvent(IngestionStarted(dataset.ref, s, coordinatorActor)) }
      probe.send(app.cluster.guardian, CurrentShardSnapshot(dataset.ref, map))

      probe.send(app.cluster.guardian, GetShardMapsSubscriptions)
      probe.expectMsgPF() {
        case MapsAndSubscriptions(mappers, subscriptions) =>
          mappers.size shouldEqual 1
          mappers(dataset.ref) shouldEqual map
          subscriptions.subscriptions.isEmpty shouldEqual true
      }

      // recover from Guardian
      val clusterActor = app.clusterActor

      probe.send(coordinatorActor, CoordinatorRegistered(clusterActor))
      probe.send(coordinatorActor, MiscCommands.GetClusterActor)
      probe.expectMsgPF() {
        case Some(ref: ActorRef) => ref shouldEqual clusterActor
      }
    }
    "recover proper subscribers and shard map state" in {
      Thread sleep 1500
      // check that NodeCluster/ShardCoordinator singletons have the right state too now
      probe.send(app.clusterActor, GetShardMap(dataset.ref))
      probe.expectMsg(CurrentShardSnapshot(dataset.ref, map))
    }
    "shutdown cleanly" in {
      app.shutdown()
      TestKit.awaitCond(app.cluster.isTerminated, 3.seconds)
    }
  }
}

// This needs to be a class and not object, otherwise multiple tests cannot reuse it  :(
class FiloServerApp extends FilodbClusterNode with StrictLogging {
  override val role = ClusterRole.Server

  override lazy val system = ActorSystem(systemName, AkkaSpec.settings.allConfig)

  override val cluster = FilodbCluster(system)

  lazy val clusterActor = cluster.clusterSingleton(roleName, withManager = true)

  lazy val client = new LocalClient(coordinatorActor)

  def main(args: Array[String]): Unit = {
    // NOTE: we don't want to start the coordinator and clusterActor here, it has to be done later so it can be tested
    cluster.joinSeedNodes()
    cluster.kamonInit(role)
    scala.concurrent.Await.result(metaStore.initialize(), cluster.settings.InitializationTimeout)
    cluster._isInitialized.set(true)

    client
  }

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = cluster.shutdown()
  })
}
