package filodb.coordinator

import scala.concurrent.duration._

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.client.LocalClient
import filodb.core.metadata.{DataColumn, Dataset}

class FilodbClusterNodeSpec extends RunnableSpec {

  import NodeClusterActor._
  import NodeCoordinatorActor._

  "A FiloServer Node" must {
    FiloServerApp.main(Array.empty)

    "join the cluster" in {
      TestKit.awaitCond(FiloServerApp.cluster.isJoined, 5.seconds)
    }
    "create and setup the coordinatorActor and clusterActor" in {
      val coordinatorActor = FiloServerApp.coordinatorActor
      val clusterActor = FiloServerApp.clusterActor

      implicit val system = FiloServerApp.system
      val probe = TestProbe()
      probe.send(coordinatorActor, ClusterHello(clusterActor))
      probe.send(coordinatorActor, MiscCommands.GetClusterActor)
      probe.expectMsgPF() {
        case Some(ref: ActorRef) => ref shouldEqual clusterActor
      }

      probe.send(clusterActor, GetRefs("worker"))
      probe.expectMsgPF(2.seconds) {
        case refs =>
          refs.asInstanceOf[Set[ActorRef]].head shouldEqual FiloServerApp.coordinatorActor
      }
    }
    "shutdown cleanly" in {
      FiloServerApp.shutdown()
      TestKit.awaitCond(FiloServerApp.cluster.isTerminated, 5.seconds)
    }
  }
}

/* scala.DelayedInit issues: extends App */
object FiloServerApp extends FilodbClusterNode with StrictLogging {

  import filodb.core.metadata.Column.ColumnType._
  import net.ceedubs.ficus.Ficus._

  override val role = ClusterRole.Server

  override lazy val system = ActorSystem(systemName, AkkaSpec.settings.allConfig)

  override lazy val cluster = FilodbCluster(system)

  lazy val clusterActor = cluster.clusterSingletonProxy(roleName, withManager = true)

  lazy val client = new LocalClient(coordinatorActor)

  def main(args: Array[String]): Unit = {
    cluster.kamonInit(role)
    coordinatorActor
    scala.concurrent.Await.result(metaStore.initialize(), cluster.settings.InitializationTimeout)
    cluster.joinSeedNodes()
    cluster.clusterSingletonProxy(roleName, withManager = true)
    cluster._isInitialized.set(true)

    clusterActor
    client
  }

  def createDatasetFromConfig(): Unit = {
    cluster.settings.DatasetDefinitions.foreach { case (datasetName, datasetConf) =>
      createDatasetFromConfig(datasetName, datasetConf)
    }
  }

  def createDatasetFromConfig(datasetName: String, config: Config): Unit = {
    val partKeys = config.as[Seq[String]]("partition-keys")
    val rowKeys = config.as[Seq[String]]("row-keys")

    val columns =
      config.as[Seq[String]]("string-columns").map(n => DataColumn(0, n, datasetName, 0, StringColumn)) ++
        config.as[Seq[String]]("double-columns").map(n => DataColumn(0, n, datasetName, 0, DoubleColumn)) ++
        config.as[Seq[String]]("long-columns").map(n => DataColumn(0, n, datasetName, 0, LongColumn)) ++
        config.as[Seq[String]]("int-columns").map(n => DataColumn(0, n, datasetName, 0, IntColumn)) ++
        config.as[Seq[String]]("map-columns").map(n => DataColumn(0, n, datasetName, 0, MapColumn))

    val dataset = Dataset(datasetName, rowKeys, ":string 0", partKeys)
    logger.info(s"Creating dataset $dataset with columns $columns...")
    client.createNewDataset(dataset, columns)
  }

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = cluster.shutdown()
  })
}
