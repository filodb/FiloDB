package filodb.coordinator

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.concurrent.ScalaFutures

import filodb.coordinator.client.{Client, LocalClient}

class FilodbClusterCliSpec extends RunnableSpec with ScalaFutures {

  import NodeClusterActor.CoordinatorRegistered

  private val conf = ConfigFactory.parseString(
    s"""
      header = true
      batch-size = 10
      resource = "/GDELT-sample-test.csv"""")


  "A Cli Node" must {
    FiloCliApp.main(Array.empty)
    import FiloCliApp.cluster.settings._

    "become initialized" in {
      TestKit.awaitCond(FiloCliApp.cluster.isInitialized, DefaultTaskTimeout)
    }
    "create and setup the coordinatorActor and clusterActor" in {
      val coordinatorActor = FiloCliApp.coordinatorActor

      implicit val system = FiloCliApp.system
      val probe = TestProbe()

      probe.send(coordinatorActor, CoordinatorRegistered(probe.ref))
      probe.send(coordinatorActor, MiscCommands.GetClusterActor)
      probe.expectMsgPF() {
        case Some(ref: ActorRef) => ref shouldEqual probe.ref
      }
    }
    "shutdown cleanly" in {
      FiloCliApp.shutdown()
      TestKit.awaitCond(FiloCliApp.cluster.isTerminated, GracefulStopTimeout)
    }
  }
}

/* scala.DelayedInit issues: extends App */
object FiloCliApp extends FilodbClusterNode with StrictLogging {

  override val role: ClusterRole = ClusterRole.Cli

  lazy val system = ActorSystem(systemName, AkkaSpec.settings.allConfig)

  lazy val cluster = FilodbCluster(system)

  lazy val client = new LocalClient(coordinatorActor)

  cluster._isInitialized.set(true)

  def main(args: Array[String]): Unit = {
    coordinatorActor
    client

    Client.parse(metaStore.initialize(), cluster.settings.DefaultTaskTimeout) {
      case filodb.core.Success => logger.debug("Succeeded.")
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = shutdown()
  })
}