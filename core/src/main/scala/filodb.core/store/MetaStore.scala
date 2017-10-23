package filodb.core.store

import scala.concurrent.Future

import filodb.core._
import filodb.core.metadata.Dataset

abstract class MetaStoreError(msg: String) extends Exception(msg)

/**
 * The MetaStore defines an API to read and write dataset metadata.
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
   * @return Success, or MetadataException, or StorageEngineException
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

}