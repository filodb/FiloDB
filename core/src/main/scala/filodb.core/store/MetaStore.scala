package filodb.core.store

import scala.concurrent.Future
import scala.util.Try

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

import filodb.core._
import filodb.core.metadata.Dataset

abstract class MetaStoreError(msg: String) extends Exception(msg)

/**
 * Contains all the config needed to recreate `NodeClusterActor.SetupDataset`, set up a dataset for streaming
 * ingestion on FiloDB nodes.   Note: the resources Config needs to be translated by an upper layer.
 */
final case class IngestionConfig(ref: DatasetRef,
                                 resources: Config,
                                 streamFactoryClass: String,
                                 streamConfig: Config) {

  // called by NodeClusterActor, by this point, validation and failure if
  // config parse issue or not available are raised from Cli / HTTP
  def numShards: Int = IngestionConfig.numShards(resources).get
  def minNumNodes: Int = IngestionConfig.minNumNodes(resources).get

}

object IngestionConfig {
  import IngestionKeys.{Dataset => DatasetRefKey, _}

  /* These two are not called until NodeClusterActor creates
     DatasetResourceSpec for SetupData, but they are not specifically written/read via C*,
     only as string. Why not parse early to fail fast and store specifically like 'dataset'. */
  def numShards(c: Config): Try[Int] = c.intT(IngestionKeys.NumShards)
  def minNumNodes(c: Config): Try[Int] = c.intT(IngestionKeys.MinNumNodes)

  /** Creates an IngestionConfig from a "source config" file - see conf/timeseries-dev-source.conf.
    * Allows the caller to decide what to do with configuration parsing errors and when.
    * Fails if no dataset is provided by the config submitter.
    */
  private[core] def apply(sourceConfig: Config): Try[IngestionConfig] =
    for {
      resolved  <- sourceConfig.resolveT
      dataset   <- resolved.stringT(DatasetRefKey) // fail fast if missing
      factory   <- resolved.stringT(SourceFactory) // fail fast if missing
      numShards <- numShards(resolved)             // fail fast if missing
      minNodes  <- minNumNodes(resolved)           // fail fast if missing
      streamConfig = resolved.as[Option[Config]](IngestionKeys.SourceConfig).getOrElse(ConfigFactory.empty)
      ref          = DatasetRef.fromDotString(dataset)
    } yield IngestionConfig(ref, resolved, factory, streamConfig)

  def apply(sourceConfig: Config, backupSourceFactory: String): Try[IngestionConfig] = {
    val backup = ConfigFactory.parseString(s"$SourceFactory = $backupSourceFactory")
    apply(sourceConfig.withFallback(backup))
  }

  def apply(sourceStr: String, backupSourceFactory: String): Try[IngestionConfig] =
    Try(ConfigFactory.parseString(sourceStr))
      .flatMap(apply(_, backupSourceFactory))

  /** Creates an IngestionConfig from `ingestionconfig` Cassandra table. */
  def apply(ref: DatasetRef, factoryclass: String, resources: String, sourceconfig: String): IngestionConfig =
    IngestionConfig(
      ref,
      ConfigFactory.parseString(resources),
      factoryclass,
      ConfigFactory.parseString(sourceconfig))
}

/**
 * The MetaStore defines an API to read and write datasets, checkpoints, and other metadata.
 * It is not responsible for sharding, partitioning, etc. which is the domain of the ColumnStore.
 * Like the ColumnStore, datasets are partitioned into "databases", like keyspaces in Cassandra.
 */
trait MetaStore {
  /**
   * Initializes the MetaStore so it is ready for further commands.
   */
  def initialize(): Future[Response]

  /**
   * Clears all dataset and column metadata from the MetaStore.
   */
  def clearAllData(): Future[Response]

  /**
   * ** Dataset API ***
   */

  /**
   * Writes new dataset metadata to the MetaStore.  If the dataset ref already existed, then nothing
   * should be modified and AlreadyExists returned.  This is to prevent dataset definition from changing
   * after it has been created.
   * @param dataset the Dataset to create.
   * @return Success, or AlreadyExists, or StorageEngineException
   */
  def newDataset(dataset: Dataset): Future[Response]

  /**
   * Retrieves a Dataset object of the given name, with all the existing column definitions
   * @param ref the DatasetRef defining the dataset to retrieve details for
   * @return a Dataset
   */
  def getDataset(ref: DatasetRef): Future[Dataset]

  def getDataset(dataset: String): Future[Dataset] = getDataset(DatasetRef(dataset))

  /**
   * Retrieves the names of all datasets registered in the metastore
   * @param database the name of the database/keyspace to retrieve datasets for.  If None, return all
   *                 datasets across all databases.
   */
  def getAllDatasets(database: Option[String]): Future[Seq[DatasetRef]]

  /**
   * Deletes all dataset metadata.  Does not delete column store data.
   * @param ref the DatasetRef defining the dataset to delete
   * @return Success, or MetadataException, or StorageEngineException; NotFound if dataset not there before
   */
  def deleteDataset(ref: DatasetRef): Future[Response]

  /**
   * Shuts down the MetaStore, including any threads that might be hanging around
   */
  def shutdown(): Unit

  /**
    * Call this method after successfully ingesting data from the given dataset
    * up to a particular offset. This offset can be used during recovery to determine
    * which offset to restart from.
    *
    * The offset stored is per-shard, per-group. A dataset is made of many shards.
    * Group is the set of partitions within a shard.
    *
    * @param dataset checkpoint will be written for this dataset
    * @param shardNum shard identifier
    * @param groupNum group identifier within the shard
    * @param offset the offset of the last record that has been successfully processed
    * @return Success, or MetadataException, or StorageEngineException
    */
  def writeCheckpoint(dataset: DatasetRef, shardNum: Int, groupNum: Int, offset: Long): Future[Response]

  /**
    * Use this method during recovery to figure out the offset to re-start ingestion
    * from. This is calculated by finding the minimum offset from all of the offsets
    * stored for each group of the shard.
    *
    * @param dataset checkpoint will be retrieved for this dataset
    * @param shardNum shard identifier
    * @return the earliest offset for all groups of given shard, or 0 if none exist
    */
  def readEarliestCheckpoint(dataset: DatasetRef, shardNum: Int): Future[Long]

  /**
    * Use this method to retrieve checkpoints for each group of the given shard
    * @param dataset checkpoint will be retrieved for this dataset
    * @param shardNum shard identifier
    * @return a map with the group identifier as key and offset as value
    */
  def readCheckpoints(dataset: DatasetRef, shardNum: Int): Future[Map[Int,Long]]

  /**
   * Writes the ingestion state to the metaStore so it could be recovered later.
   * Note that the entry is keyed on the DatasetRef, so if this is called for the same DatasetRef then the data
   * will be overwritten (perhaps use a different database to distinguish?)
   *
   * @param config the IngestionConfig to write
   * @return Success, or MetadataException, or StorageEngineException
   */
  def writeIngestionConfig(config: IngestionConfig): Future[Response]

  /**
   * Reads back all previously defined IngestionConfigs.
   *
   * @return a Seq of IngestionConfig's, or Nil if no states exist in the table
   */
  def readIngestionConfigs(): Future[Seq[IngestionConfig]]

  /**
   * Removes a previously persisted IngestionConfig.  This might be useful say if one wanted to stop streaming a dataset
   *
   * @return Success if the IngestionConfig was removed successfully, NotFound if it did not exist
   */
  def deleteIngestionConfig(ref: DatasetRef): Future[Response]
}