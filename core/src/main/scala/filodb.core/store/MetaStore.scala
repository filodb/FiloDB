package filodb.core.store

import scala.concurrent.Future

import filodb.core._

abstract class MetaStoreError(msg: String) extends Exception(msg)

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
  def readCheckpoints(dataset: DatasetRef, shardNum: Int): Future[Map[Int, Long]]

}

object NullMetaStore extends MetaStore {
  override def initialize(): Future[Response] = Future.successful(Success)
  override def clearAllData(): Future[Response] = Future.successful(Success)
  override def shutdown(): Unit = {}
  override def writeCheckpoint(dataset: DatasetRef, shardNum: Int, groupNum: Int,
                               offset: Long): Future[Response] = Future.successful(Success)
  override def readEarliestCheckpoint(dataset: DatasetRef,
                                      shardNum: Int): Future[Long] = Future.successful(0)
  override def readCheckpoints(dataset: DatasetRef,
                               shardNum: Int): Future[Map[Int, Long]] = Future.successful(Map.empty)
}