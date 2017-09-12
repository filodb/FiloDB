package filodb.coordinator

import scala.concurrent.duration._

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.concurrent.ScalaFutures

import filodb.coordinator.client.LocalClient
import filodb.core.metadata.{DataColumn, Dataset}

class FilodbClusterNodeSpec extends RunnableSpec with ScalaFutures {

  import NodeClusterActor._

  "A FiloServer Node" must {
    FiloServerApp.main(Array.empty)

    "join the cluster" in {
      TestKit.awaitCond(FiloServerApp.cluster.isJoined, 50.seconds)
    }
    "create and setup the coordinatorActor and clusterActor" in {
      val coordinatorActor = FiloServerApp.coordinatorActor
      val clusterActor = FiloServerApp.clusterActor

      implicit val system = FiloServerApp.system
      val shardPath = ActorName.shardStatusPath(FiloServerApp.cluster.selfAddress)
      val shardActor = system.actorSelection(shardPath).resolveOne(2.seconds).futureValue
      val probe = TestProbe()
      probe.send(coordinatorActor, CoordinatorRegistered(clusterActor, shardActor))
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

  override val cluster = FilodbCluster(system)

  val clusterActor = cluster.clusterSingletonProxy(roleName, withManager = true)
  cluster.joinSeedNodes()

  lazy val client = new LocalClient(coordinatorActor)

  def main(args: Array[String]): Unit = {
    coordinatorActor
    cluster.kamonInit(role)
    scala.concurrent.Await.result(metaStore.initialize(), cluster.settings.InitializationTimeout)
    cluster._isInitialized.set(true)

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

    val dataset = Dataset(datasetName, rowKeys, partKeys)
    logger.info(s"Creating dataset $dataset with columns $columns...")
    client.createNewDataset(dataset, columns)
  }

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = cluster.shutdown()
  })
}
