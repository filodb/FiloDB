package filodb.core.datastore

import java.nio.ByteBuffer
import scala.concurrent.{Future, ExecutionContext}

import filodb.core.metadata.{Dataset, Partition, Shard, Column}
import filodb.core.messages._

object Datastore {
  // max # of partitions to retrieve
  val MaxPartitionLimit = 10000

  /**
   * Set of responses from dataset commands
   */
  case class TheDataset(dataset: Dataset) extends Response
  // This is really an exception - deleting a non-empty partition should be a bug
  class NonEmptyDataset(partitions: Set[String]) extends Exception

  /**
   * Set of responses from column commands
   */
  case class IllegalColumnChange(reasons: Seq[String]) extends Response
  case class TheSchema(schema: Column.Schema) extends Response

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
  def datasetApi: DatasetApi
  def partitionApi: PartitionApi
  def columnApi: ColumnApi

  import DataApi.ColRowBytes

  /**
   * ** Dataset API ***
   */

  /**
   * Creates a new dataset with the given name, if it doesn't already exist.
   * @param name Name of the dataset to create
   * @returns Success, or AlreadyExists, or StorageEngineException
   */
  def newDataset(name: String)(implicit context: ExecutionContext): Future[Response] =
    datasetApi.createNewDataset(name)

  /**
   * Retrieves a Dataset object of the given name, including a list of partitions.
   * TODO: implement limits.  # of partitions could get really long.
   * @param name Name of the dataset to retrieve
   * @param limit the max number of partitions to retrieve
   * @param TheDataset
   */
  def getDataset(name: String, limit: Int = MaxPartitionLimit)
                (implicit context: ExecutionContext): Future[Response] =
    datasetApi.getDataset(name)

  /**
   * Attempts to delete a dataset with the given name.  Will fail if the dataset still has
   * partitions inside. You need to delete the partitions first.
   * @param name Name of the dataset to delete.
   * @returns Success, or MetadataException, or StorageEngineException
   */
  def deleteDataset(name: String)(implicit context: ExecutionContext): Future[Response] =
    datasetApi.deleteDataset(name)

  /**
   * ** Column API ***
   */

  /**
   * Creates a new column for a particular dataset and effective version.
   * Can also be used to change the column type by "creating" the same column with changes for a higher
   * version.  Note that changes for a column must have an effective version higher than the last change.
   * See the notes in metadata/Column.scala regarding columns and versioning.
   * @param column the new Column to create.
   * @return Created if succeeds, or AlreadyExists, or IllegalColumnChange
   */
  def newColumn(column: Column)(implicit context: ExecutionContext): Future[Response] = {
    columnApi.getSchema(column.dataset, Int.MaxValue).flatMap {
      case TheSchema(schema) =>
        val invalidReasons = Column.invalidateNewColumn(column.dataset, schema, column)
        if (invalidReasons.nonEmpty) { Future(IllegalColumnChange(invalidReasons)) }
        else                         { columnApi.insertColumn(column) }
      case other: ErrorResponse =>
        Future(other)
    }
  }

  /**
   * Get the schema for a version of a dataset.  This scans all defined columns from the first version
   * on up to figure out the changes. Deleted columns are not returned.
   * @param dataset the name of the dataset to return the schema for
   * @param version the version of the dataset to return the schema for
   * @return a Schema, column name -> Column definition
   */
  def getSchema(dataset: String, version: Int)(implicit context: ExecutionContext): Future[Response] =
    columnApi.getSchema(dataset, version)

  /**
   * Marks a column as deleted.  This is more like "hiding" a column and does not actually delete the data --
   * rather it marks a column as deleted starting at version Y, and reads will no longer return data for that
   * column.
   * Also, this cannot be done to the current / latest version, it must be a new version.
   *
   * TODO: Maybe remove this command.  A delete is = reading previous column definition, and marking
   * isDeleted = true, and calling NewColumn.
   */
  def deleteColumn(dataset: String, version: Int, name: String)
                  (implicit context: ExecutionContext): Future[Response] = ???

  /**
   * ** Partition API ***
   */

  /**
   * Creates a new partition in FiloDB.  Updates both partitions and datasets tables.
   * The partition must be empty and valid.
   * TODO: update the dataset table - where to do this?
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
   * ** Columnar chunk data API ***
   */

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