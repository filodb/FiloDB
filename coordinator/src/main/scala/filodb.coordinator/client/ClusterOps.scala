package filodb.coordinator.client

import scala.concurrent.duration._

import akka.actor.ActorRef
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
}