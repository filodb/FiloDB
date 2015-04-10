package filodb.core.datastore

import scala.concurrent.Future

import filodb.core.messages._
import filodb.core.metadata.Partition

/**
 * Low-level datastore API dealing with persistence of Partition objects and shard state
 */
trait PartitionApi {
  /**
   * Creates a new partition in FiloDB.  Updates both partitions and datasets tables.
   * Does not validate the Partition object.
   * @param partition a Partition, with a name unique within the dataset.  It should be empty.
   * @returns Success, or AlreadyExists
   */
  def newPartition(partition: Partition): Future[Response]

  /**
   * Attempts to get a lock on the partition for writing.  Uses Cassandra's LWT
   * and if exists functionality.
   * @param dataset the name of the dataset
   * @param name the name of the partition
   * @param owner a unique string identifying the owner
   * @returns AlreadyLocked, or Success
   */
  def getPartitionLock(dataset: String, partition: String, owner: String): Future[Response]

  /**
   * Release the lock on the partition explicitly (stream is done, shutting down, etc.)
   * NOTE: there is a TTL on the partition lock, so this should not be needed every time
   * (and in case of failure might not be called anyways), but releasing faster helps
   * failure recovery and distributing work.
   * @param dataset the name of the dataset
   * @param name the name of the partition
   */
  def releasePartitionLock(dataset: String, partition: String): Future[Response]

  /**
   * Reads all of the current Partition state information, including existing shards.
   * TODO: maybe when # of shards gets huge, this will be expensive, and we need a version
   * that reads a subset.
   * @param dataset the name of the dataset
   * @param name the name of the partition
   * @returns ThePartition
   */
  def getPartition(dataset: String, partition: String): Future[Response]

  /**
   * Adds a shard and version to an existing partition.  Checks to see that the new shard results in a valid
   * shard, and also updates using ONLY IF to ensure data on disk is consistent and we are really
   * adding the shard to what we think.
   * @param partition the existing Partition object before the shard was added
   * @param firstRowId the first rowID of the new shard
   * @param version the version number to add
   * @return Success, InconsistentState (hash mismatch - somebody else updated the partition!)
   * Note that if partition/dataset is not found, InconsistentState will be returned.
   */
  def addShardVersion(partition: Partition,
                      firstRowId: Long,
                      version: Int): Future[Response]

  /**
   * Deletes a partition.  NOTE: need to clarify exact behavior.  Is this permanent?
   * @param dataset the name of the dataset
   * @param name the name of the partition
   * @returns Success
   */
  def deletePartition(dataset: String, partition: String): Future[Response]
}