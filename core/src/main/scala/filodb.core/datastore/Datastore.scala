package filodb.core.datastore

import java.nio.ByteBuffer
import scala.concurrent.{Future, ExecutionContext}

import filodb.core.metadata.{Partition, Shard}
import filodb.core.messages._

object Datastore {
  /**
   * Set of responses from PartitionApi methods
   */
  case object NotEmpty extends ErrorResponse
  case object NotValid extends ErrorResponse
  case class AlreadyLocked(owner: String) extends Response
  case class ThePartition(partition: Partition) extends Response

  /**
   * Set of responses from DataApi methods
   */
  case class Ack(lastSequenceNo: Long) extends Response
  case object ChunkMisaligned extends ErrorResponse
}

/**
 * The Datastore trait is intended to be the higher level API abstraction
 * over the lower level storage specific APIs.  Most folks will want
 * to use this instead of the *Api traits.  It combines the separate *Api traits as well.
 *
 * It includes some verification and metadata logic on top of the *Api interfaces
 * TODO: as well as throttling/backpressure.
 *
 * Each datastore implementation usually implements Datastore by implementing each lower-level
 * sub-API, such as DataApi.
 */
trait Datastore {
  import Datastore._

  def dataApi: DataApi
  def partitionApi: PartitionApi

  import DataApi.ColRowBytes

  /**
   * Creates a new partition in FiloDB.  Updates both partitions and datasets tables.
   * The partition must be empty and valid.
   * @param partition a Partition, with a name unique within the dataset.  It should be empty.
   * @returns Success, or AlreadyExists, or NotEmpty/NotValid
   */
  def newPartition(partition: Partition)
                  (implicit context: ExecutionContext): Future[Response] = {
    if (!partition.isEmpty) return Future(NotEmpty)
    if (!partition.isValid) return Future(NotValid)
    partitionApi.newPartition(partition)
  }

  /**
   * Reads all of the current Partition state information, including existing shards.
   * TODO: maybe when # of shards gets huge, this will be expensive, and we need a version
   * that reads a subset.
   * @param dataset the name of the dataset
   * @param name the name of the partition
   * @returns ThePartition
   */
  def getPartition(dataset: String, partition: String)
                  (implicit context: ExecutionContext): Future[Response] =
    partitionApi.getPartition(dataset, partition)

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
                      version: Int)
                     (implicit context: ExecutionContext): Future[Response] =
    partitionApi.addShardVersion(partition, firstRowId, version)

  /**
   * Inserts one chunk of data from different columns.
   * Checks that the rowIdRange is aligned with the chunkSize.
   * @param shard the Shard to write to
   * @param rowId the starting rowId for the chunk. Must be aligned to chunkSize.
   * @param lastSeqNo the last Long sequence number of the chunk of columns
   * @param columnsBytes the column name and bytes to be written for each column
   * @returns Ack(lastSeqNo), or other ErrorResponse
   */
  def insertOneChunk(shard: Shard,
                     rowId: Long,
                     lastSeqNo: Long,
                     columnsBytes: Map[String, ByteBuffer])
                    (implicit context: ExecutionContext): Future[Response] =
    dataApi.insertOneChunk(shard, rowId, columnsBytes)
        .collect { case Success => Ack(lastSeqNo) }

  /**
   * Streams chunks from one column in, applying the folding function to chunks.
   * @param shard the Shard to read from
   * @param column the name of the column to read from
   * @param rowIdRange an optional range to restrict portion of shard to read from.
   * @param initValue the initial value to be fed to the folding function
   * @param foldFunc a function taking the currently accumulated T, new chunk, and returns a T
   * @returns either a T or ErrorResponse
   */
  def scanOneColumn[T](shard: Shard,
                    column: String,
                    rowIdRange: Option[(Long, Long)] = None)
                   (initValue: T)
                   (foldFunc: (T, ColRowBytes) => T)
                   (implicit context: ExecutionContext): Future[Either[T, ErrorResponse]] =
    dataApi.scanOneColumn(shard, column, rowIdRange)(initValue)(foldFunc)
}