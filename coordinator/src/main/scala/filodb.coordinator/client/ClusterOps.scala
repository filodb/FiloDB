package filodb.coordinator.client

import scala.concurrent.duration._

import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator._
import filodb.core._
import filodb.core.downsample.DownsampleConfig
import filodb.core.store.{IngestionConfig, StoreConfig}

case object NoClusterActor extends ErrorResponse

trait ClusterOps extends ClientBase with StrictLogging {
  import NodeClusterActor._

  /**
   * Sets up the cluster for ingestion and querying of a dataset which must have been defined in the
   * MetaStore already.
   * @param dataset the DatasetRef of the dataset to start ingesting
   * @param spec the DatasetResourceSpec specifying # of shards and nodes for ingest and querying
   * @param source the IngestionSource specifying where to pull data from
   * @return None upon success, Some(ErrorResponse) on error or if dataset already setup
   */
  def setupDataset(dataset: DatasetRef,
                   spec: DatasetResourceSpec,
                   source: IngestionSource,
                   storeConfig: StoreConfig,
                   downsampleConfig: DownsampleConfig = DownsampleConfig.disabled,
                   timeout: FiniteDuration = 30.seconds): Option[ErrorResponse] =
    clusterActor.map { ref =>
      Client.actorAsk(ref, SetupDataset(dataset, spec, source, storeConfig, downsampleConfig), timeout) {
        case DatasetVerified  => None
        case e: ErrorResponse => Some(e)
      }
    }.getOrElse(Some(NoClusterActor))

  def setupDataset(config: IngestionConfig, timeout: FiniteDuration): Option[ErrorResponse] = {
    val command = SetupDataset(config)
    setupDataset(command.ref, command.resources, command.source, config.storeConfig, command.downsampleConfig, timeout)
  }

  /**
   * Obtains a list of all datasets registered in the cluster for ingestion and querying via
   * `setupDataset` above or the `SetupDataset` API to `NodeClusterActor`.
   * @return The list of registered datasets, or Nil if the clusterActor ref is not available
   */
  def getDatasets(timeout: FiniteDuration = 30.seconds): Seq[DatasetRef] =
    clusterActor.map { ref =>
      Client.actorAsk(ref, ListRegisteredDatasets, timeout) {
        case refs: Seq[DatasetRef] @unchecked => refs
      }
    }.getOrElse(Nil)

  /**
   * Obtains the `filodb.coordinator.ShardMapper` instance for a registered dataset,
   * and thus the current `ShardStatus` for every shard, and a reference to the
   * `NodeCoordinatorActor` / node address for each shard.
   * @return Some(shardMapper) if the dataset is registered, None if dataset not found
   */
  def getShardMapper(dataset: DatasetRef, timeout: FiniteDuration = 30.seconds): Option[ShardMapper] =
    clusterActor.map { ref =>
      Client.actorAsk(ref, GetShardMap(dataset), timeout) {
        case CurrentShardSnapshot(_, m) => Some(m)
        case _                          => None
      }
    }.getOrElse(None)
}