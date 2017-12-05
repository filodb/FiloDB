package filodb.standalone

import akka.actor.ActorRef
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.concurrent.ScalaFutures

import filodb.coordinator.{MiscCommands, RunnableSpec}
import filodb.coordinator.NodeClusterActor.CoordinatorRegistered

class FiloServerSpec extends RunnableSpec with ScalaFutures {
  val server = new FiloServer()

  "A FiloServer Node" must {
    val timeout = server.cluster.settings.DefaultTaskTimeout

    "initialize" in {
      server.start()
      TestKit.awaitCond(server.cluster.isInitialized,  timeout)
    }
    "create and setup the coordinatorActor and clusterActor" in {
      implicit val system = server.system
      val coordinatorActor = server.coordinatorActor
      server.cluster.clusterActor.isDefined shouldEqual true
      val probe = TestProbe()

      server.cluster.clusterActor foreach { clusterActor =>
        probe.send(coordinatorActor, CoordinatorRegistered(clusterActor, probe.ref))
        probe.send(coordinatorActor, MiscCommands.GetClusterActor)
        probe.expectMsgPF() {
          case Some(ref: ActorRef) => ref shouldEqual clusterActor
        }
      }
    }
    "shutdown cleanly" in {
      server.shutdown()
      TestKit.awaitCond(server.cluster.isTerminated, timeout)
    }
  }
}
