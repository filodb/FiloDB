package filodb.coordinator

import scala.concurrent.duration._

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.concurrent.ScalaFutures

import filodb.coordinator.client.LocalClient

class FilodbClusterNodeSpec extends RunnableSpec with ScalaFutures {

  import NodeClusterActor._

  "A FiloServer Node" must {
    FiloServerApp.main(Array.empty)

    "join the cluster" in {
      TestKit.awaitCond(FiloServerApp.cluster.isJoined, 10.seconds)
    }
    "create and setup the coordinatorActor and clusterActor" in {
      val coordinatorActor = FiloServerApp.coordinatorActor
      val clusterActor = FiloServerApp.clusterActor

      implicit val system = FiloServerApp.system
      val probe = TestProbe()
      probe.send(coordinatorActor, CoordinatorRegistered(clusterActor, TestProbe().ref))
      probe.send(coordinatorActor, MiscCommands.GetClusterActor)
      probe.expectMsgPF() {
        case Some(ref: ActorRef) => ref shouldEqual clusterActor
      }
    }
    "shutdown cleanly" in {
      FiloServerApp.shutdown()
      TestKit.awaitCond(FiloServerApp.cluster.isTerminated, 3.seconds)
    }
  }
}

/* scala.DelayedInit issues: extends App */
object FiloServerApp extends FilodbClusterNode with StrictLogging {
  override val role = ClusterRole.Server

  override lazy val system = ActorSystem(systemName, AkkaSpec.settings.allConfig)

  override val cluster = FilodbCluster(system)

  lazy val clusterActor = cluster.clusterSingleton(roleName, withManager = true)

  lazy val client = new LocalClient(coordinatorActor)

  def main(args: Array[String]): Unit = {
    clusterActor
    coordinatorActor
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
