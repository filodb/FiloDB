package filodb.standalone

import scala.concurrent.duration._
import akka.actor.ActorRef
import akka.testkit.{TestKit, TestProbe}
import filodb.coordinator.{MiscCommands, RunnableSpec}
import filodb.coordinator.NodeCoordinatorActor.ClusterHello

class FiloServerSpec extends RunnableSpec {
  "A FiloServer Node" must {
    "initialize" in {
      FiloServer.main(Array.empty)
      TestKit.awaitCond(FiloServer.cluster.isInitialized, 5.seconds)
    }
    "create and setup the coordinatorActor and clusterActor" in {
      val coordinatorActor = FiloServer.coordinatorActor
      val clusterActor = FiloServer.cluster.clusterActor.get

      implicit val system = FiloServer.system
      val probe = TestProbe()
      probe.send(coordinatorActor, ClusterHello(clusterActor))
      probe.send(coordinatorActor, MiscCommands.GetClusterActor)
      probe.expectMsgPF() {
        case Some(ref: ActorRef) => ref shouldEqual clusterActor
      }
    }
    "shutdown cleanly" in {
      FiloServer.shutdown()
      TestKit.awaitCond(FiloServer.cluster.isTerminated, 5.seconds)
    }
  }
}
