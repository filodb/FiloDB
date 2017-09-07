package filodb.standalone

import akka.actor.ActorRef
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.concurrent.ScalaFutures

import filodb.coordinator.NodeClusterActor.CoordinatorRegistered
import filodb.coordinator.{ActorName, FiloServerApp, MiscCommands, RunnableSpec}

class FiloServerSpec extends RunnableSpec with ScalaFutures {
  "A FiloServer Node" must {
    val timeout = FiloServer.cluster.settings.DefaultTaskTimeout

    "initialize" in {
      FiloServer.main(Array.empty)
      TestKit.awaitCond(FiloServer.cluster.isInitialized,  timeout)
    }
    "create and setup the coordinatorActor and clusterActor" in {
      implicit val system = FiloServer.system
      val coordinatorActor = FiloServer.coordinatorActor
      val clusterActor = FiloServer.cluster.clusterActor.get

      val probe = TestProbe()
      probe.send(coordinatorActor, CoordinatorRegistered(clusterActor, probe.ref))
      probe.send(coordinatorActor, MiscCommands.GetClusterActor)
      probe.expectMsgPF() {
        case Some(ref: ActorRef) => ref shouldEqual clusterActor
      }
    }
    "shutdown cleanly" in {
      FiloServer.shutdown()
      TestKit.awaitCond(FiloServer.cluster.isTerminated, timeout)
    }
  }
}
