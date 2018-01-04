package filodb.standalone

import scala.collection.mutable.HashSet
import scala.concurrent.duration._

import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import filodb.coordinator._
import filodb.coordinator.client.LocalClient
import filodb.coordinator.NodeClusterActor.{DatasetResourceSpec, IngestionSource}
import filodb.coordinator.QueryCommands.{MostRecentTime, QueryArgs}
import filodb.core.{DatasetRef, ErrorResponse}
import filodb.core.query.ColumnFilter
import filodb.core.query.Filter.Equals

/**
 * A trait used for MultiJVM tests based on starting the standalone FiloServer using timeseries-dev config
 * (ie pretty much the same as deployed setup)
 */
abstract class StandaloneMultiJvmSpec(config: MultiNodeConfig) extends MultiNodeSpec(config)
  with Suite with StrictLogging
  with ScalaFutures with FlatSpecLike with ImplicitSender
  with Matchers with BeforeAndAfterAll {
  override def initialParticipants: Int = roles.size

  import akka.testkit._

  // Ingestion Source section
  val source = ConfigFactory.parseFile(new java.io.File("conf/timeseries-dev-source.conf"))
  val dataset = DatasetRef(source.getString("dataset"))
  val numShards = source.getInt("num-shards")
  val resourceSpec = DatasetResourceSpec(numShards, source.getInt("min-num-nodes"))
  val sourceconfig = source.getConfig("sourceconfig")
  val ingestionSource = source.as[Option[String]]("sourcefactory").map { factory =>
    IngestionSource(factory, sourceconfig)
  }.get
  val chunkDuration = sourceconfig.getDuration("chunk-duration")


  def waitAllShardsIngestionActive(): Unit = {
    val activeShards = new HashSet[Int]()
    while (activeShards.size < numShards) {
      expectMsgPF(5.seconds.dilated) {
        case ShardAssignmentStarted(_, shard, _) =>
        case IngestionStarted(_, shard, _) => activeShards += shard
      }
    }
  }

  def validateShardStatus(client: LocalClient)(statusValidator: ShardStatus => Boolean): Unit = {
    client.getShardMapper(dataset) match {
      case Some(map) =>
        info(s"Shard map:  $map")
        info(s"Shard map nodes: ${map.allNodes}")

        map.shardValues.size shouldBe numShards
        map.shardValues.forall { case (_, status) =>
          statusValidator(status)
        } shouldEqual true
        // only two nodes assigned
        map.allNodes.size shouldEqual 2
      case _ =>
        fail(s"Unable to obtain status for dataset $dataset")
    }
  }

  def setupDataset(client: LocalClient): Unit = {
    client.setupDataset(dataset, resourceSpec, ingestionSource).foreach {
      case e: ErrorResponse => fail(s"Errors setting up dataset $dataset: $e")
    }
  }

  def runQuery(client: LocalClient): Double = {
    // This is the promQL equivalent: sum(heap_usage{partition="P0"}[1000m])
    val query = QueryArgs("sum", "value", Nil, MostRecentTime(60000000), "simple", List())
    val filters = Vector(ColumnFilter("partition", Equals("P0")), ColumnFilter("__name__", Equals("heap_usage")))
    val response1 = client.partitionFilterAggregate(dataset, query, filters)
    val answer = response1.elements.head.asInstanceOf[Double]
    info(s"Query Response was: $answer")
    answer.asInstanceOf[Double]
  }
}

