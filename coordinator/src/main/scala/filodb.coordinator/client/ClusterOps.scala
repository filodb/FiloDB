package filodb.coordinator.client

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator._
import filodb.core._

case object NoClusterActor extends ErrorResponse

trait ClusterOps extends ClientBase with StrictLogging {
  import NodeClusterActor._

  def nodeCoordinator: ActorRef

  def clusterActor: Option[ActorRef]

  /**
   * Obtains a list of all datasets registered in the cluster for ingestion and querying via
   * `setupDataset` above or the `SetupDataset` API to `NodeClusterActor`.
   * @return The list of registered datasets, or Nil if the clusterActor ref is not available
   */
  def getDatasets(v2Enabled: Boolean, timeout: FiniteDuration = 30.seconds): Seq[DatasetRef] = {
    val actor = if (v2Enabled) Some(nodeCoordinator) else clusterActor
    actor.map { ref =>
      Client.actorAsk(ref, ListRegisteredDatasets, timeout) {
        case refs: Seq[DatasetRef] @unchecked => refs
      }
    }.getOrElse(Nil)
  }

  /**
   * Obtains the `filodb.coordinator.ShardMapper` instance for a registered dataset,
   * and thus the current `ShardStatus` for every shard, and a reference to the
   * `NodeCoordinatorActor` / node address for each shard.
   * @return Some(shardMapper) if the dataset is registered, None if dataset not found
   */
  def getShardMapper(dataset: DatasetRef, v2Enabled: Boolean,
                     timeout: FiniteDuration = 30.seconds): Option[ShardMapper] = {
    val actor = if (v2Enabled) Some(nodeCoordinator) else clusterActor
    actor.flatMap { ref =>
      Client.actorAsk(ref, GetShardMap(dataset), timeout) {
        case CurrentShardSnapshot(_, m) => Some(m)
        case _ => None
      }
    }
  }

  /**
   * ShardMapperV2 is an optimization of the response size over the ShardMapper and GetShardMap ask call
   * @return Some(ShardMapperV2) if the dataset is registered, None if dataset not found
   */
  def getShardMapperV2(dataset: DatasetRef, v2Enabled: Boolean,
                     timeout: FiniteDuration = 10.seconds): Option[ShardMapperV2] = {
    require(v2Enabled, s"ClusterV2 ShardAssignment is must for this operation")
    val actor = Some(nodeCoordinator)
    actor.flatMap { ref =>
      Client.actorAsk(ref, GetShardMapV2(dataset), timeout) {
        case ShardSnapshot(shardMapperV2) => Some(shardMapperV2)
        case _ => None
      }
    }
  }

  /**
   * Async version of getShardMapperV2 that returns a Future instead of blocking.
   * This allows non-blocking concurrent calls to multiple nodes.
   * @return Future of Option of ShardMapperV2 - Future that completes with Some(ShardMapperV2) if dataset registered,
   *         None if dataset not found, or fails if actor not available
   */
  def getShardMapperV2Async(dataset: DatasetRef, v2Enabled: Boolean,
                           timeout: FiniteDuration = 10.seconds): Future[Option[ShardMapperV2]] = {
    require(v2Enabled, s"ClusterV2 ShardAssignment is must for this operation")
    implicit val askTimeout: Timeout = Timeout(timeout)
    import scala.concurrent.ExecutionContext.Implicits.global

    (nodeCoordinator ? GetShardMapV2(dataset)).mapTo[ShardSnapshot].map { snapshot =>
      Some(snapshot.map)  // 'map' is the field name in ShardSnapshot case class
    }.recover {
      case _: Exception => None
    }
  }
}