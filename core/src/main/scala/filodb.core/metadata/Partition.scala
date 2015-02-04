package filodb.core.metadata

import filodb.core.messages.{Command, ErrorResponse, Response}

/**
 * A dataset is divided into partitions.  There is one writer per partition
 * and partitions are internally sharded as well by rowID.
 * Partitions keeps state about the shards, each of which should own a non-overlapping range of rowIDs.
 *
 * Each partition and shard stores columns of data in chunks of rows.  The chunks
 * are kept in multiples of chunkSize, and the rowId is the first row number of the chunk.
 * For example, for columns First, Last, and Age, and chunksize of 100, chunks might be
 * stored thus in a shard:
 *   First-0 First-100 First-200  Last-0 Last-100 Last-200  Age-0 Age-100 Age-200
 *
 * Multiple versions coexist within a partition.  Shard definitions do not vary across versions.
 * In fact one common pattern is new versions = new events/rows = new shards.
 *
 * A bigger chunksize results in more efficient space usage, at a cost of higher memory
 * usage, and more expensive individual row replace/deletes.
 *
 * Row-level operations get translated into chunk reads and writes.
 *   appends (row # > lastRowId)      row # on chunk boundary -> new chunk
 *                                    new version -> new chunk
 *                                    otherwise -> replace last chunk
 *   replace (row # < lastRowId)      replace chunk
 *   delete                           replace chunk or delete chunk
 *
 * Rows are addressed by row # or ID.  Custom primary key support will be provided through
 * a PK-to-row-# index.
 *
 * Row #'s never change once a row is appended.
 */
case class Partition(dataset: String,
                     partition: String,
                     shardingStrategy: ShardingStrategy = ShardingStrategy.DefaultStrategy,
                     firstRowId: Seq[Int] = Nil,   // rowIDs must be monotonically increasing
                     versionRange: Seq[(Int, Int)] = Nil,
                     chunkSize: Int = Partition.DefaultChunkSize) {
  def isEmpty: Boolean = firstRowId.isEmpty

  def isValid: Boolean =
    (firstRowId.length == versionRange.length) &&
    chunkSize > 0 &&
    firstRowId.sliding(2).forall {
      case Seq(a, b) => a < b
      case Seq(a)    => true
    }

  /**
   * Returns Some(newPartition) with a valid new shard added, and None if the new shard info is not valid.
   * A valid new shard must have a firstRowId that is greater than the current firstRowId.last
   */
  def addShard(firstRowId: Int, versionRange: (Int, Int)): Option[Partition] = {
    val newPart = this.copy(firstRowId = this.firstRowId :+ firstRowId,
                            versionRange = this.versionRange :+ versionRange)
    if (newPart.isValid) Some(newPart) else None
  }
}

object Partition {
  val DefaultChunkSize = 1000

  /**
   * Set of low-level partition commands to send to a datastore actor for I/O
   */

  /**
   * Creates a new partition in FiloDB.  Updates both partitions and datasets tables.
   * @param partition a Partition, with a name unique within the dataset.  It should be empty.
   * @returns Success, or AlreadyExists, or NotEmpty/NotValid
   */
  case class NewPartition(partition: Partition) extends Command

  /**
   * Attempts to get a lock on the partition for writing.  Uses Cassandra's LWT
   * and if exists functionality.
   * @param dataset the name of the dataset
   * @param name the name of the partition
   * @param owner a unique string identifying the owner
   * @returns AlreadyLocked, or Success
   */
  case class GetPartitionLock(dataset: String, partition: String, owner: String) extends Command

  /**
   * Release the lock on the partition explicitly (stream is done, shutting down, etc.)
   * NOTE: there is a TTL on the partition lock, so this should not be needed every time
   * (and in case of failure might not be called anyways), but releasing faster helps
   * failure recovery and distributing work.
   * @param dataset the name of the dataset
   * @param name the name of the partition
   */
  case class ReleasePartitionLock(dataset: String, partition: String) extends Command

  /**
   * Reads all of the current Partition state information, including existing shards.
   * TODO: maybe when # of shards gets huge, this will be expensive, and we need a version
   * that reads a subset.
   * @param dataset the name of the dataset
   * @param name the name of the partition
   * @returns ThePartition
   */
  case class GetPartition(dataset: String, partition: String) extends Command

  /**
   * Adds a shard to an existing partition.  Checks to see that the new shard results in a valid
   * shard, and also updates using ONLY IF to ensure data on disk is consistent and we are really
   * adding the shard to what we think.
   * @param partition the existing Partition object before the shard was added
   * @param firstRowId the first rowID of the new shard
   * @param versionRange the range of version numbers for the new shard
   * @return Success, NotValid, InconsistentState (hash mismatch - somebody else updated the partition!)
   * Note that if partition/dataset is not found, InconsistentState will be returned.
   */
  case class AddShard(partition: Partition,
                      firstRowId: Int,
                      versionRange: (Int, Int)) extends Command

  /**
   * Updates the partition state information.  Again, in the future might need partial update
   * commands.  NOTE: you CANNOT change the chunksize and shardingStrategy once you start!
   * @param partition the Partition object to write out
   * @returns Success
   */
  case class UpdatePartition(partition: Partition) extends Command

  /**
   * Deletes a partition.  NOTE: need to clarify exact behavior.  Is this permanent?
   * @param dataset the name of the dataset
   * @param name the name of the partition
   * @returns Success
   */
  case class DeletePartition(dataset: String, partition: String) extends Command

  /**
   * Set of responses from partition commands
   */
  case object NotEmpty extends ErrorResponse
  case object NotValid extends ErrorResponse
  case class AlreadyLocked(owner: String) extends Response
  case class ThePartition(partition: Partition) extends Response
}