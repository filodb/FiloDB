package filodb.core.store

import filodb.core.Types.{ChunkID, ColumnId}
import filodb.core.metadata.Dataset
import filodb.core.query.PartitionTimeRangeReader
import filodb.memory.format.{BinaryVector, RowReader, UnsafeUtils, VectorDataReader}


trait FiloPartition {
  def dataset: Dataset
  def partKeyBase: Array[Byte]
  def partKeyOffset: Long
  def partID: Int

  /**
    * Two FiloPartitions are equal if they have the same partition key.  This test is used in various
    * data structures.
    */
  override def hashCode: Int = dataset.partKeySchema.partitionHash(partKeyBase, partKeyOffset)

  override def equals(other: Any): Boolean = other match {
    case UnsafeUtils.ZeroPointer => false
    case f: FiloPartition => dataset.partKeySchema.equals(partKeyBase, partKeyOffset, f.partKeyBase, f.partKeyOffset)
    case _: Any           => false
  }

}

class EmptyPartition(val dataset: Dataset,
                     val partKeyBase: Array[Byte],
                     val partKeyOffset: Long,
                     val partID: Int) extends FiloPartition

/**
 * An abstraction for a single partition of data, from which chunk pointers or rows can be read.
 * Note that chunks MUST be offheap.
 * Optimized for low heap usage since there are millions of these.  Avoid adding any new fields unless the CPU
 * tradeoff calls for it.
 */
trait ReadablePartition extends FiloPartition {

  // Returns string representation of partition key
  def stringPartition: String = dataset.partKeySchema.stringify(partKeyBase, partKeyOffset)

  // Returns binary partition key as a new, copied byte array
  def partKeyBytes: Array[Byte] = dataset.partKeySchema.asByteArray(partKeyBase, partKeyOffset)

  def numChunks: Int

  /**
   * The length or # of data samples of the set of appending or ingesting chunks.  0 if there are no
   * appendable chunks or write buffers.
   */
  def appendingChunkLen: Int

  def shard: Int

  /**
   * Obtains the native offheap pointer for the chunk id for data column columnID
   */
  // Disabled for now. Requires a shared lock on the chunk map.
  //def dataChunkPointer(id: ChunkID, columnID: Int): BinaryVector.BinaryVectorPtr

  /**
   * Obtains the native offheap pointer for the chunk id for column columnID, including for partition key columns
   * @param id the chunkID for the chunk to obtain
   * @param columnID the column ID of the column to obtain the pointer for
   */
  /* Disabled for now. Requires a shared lock on the chunk map.
  final def chunkPointer(id: ChunkID, columnID: Int): BinaryVector.BinaryVectorPtr =
    if (Dataset.isPartitionID(columnID)) {
      // NOTE: we cannot actually query for the vector pointer for a partition key column
      // This makes no sense because partition key columns are not vectorized
      throw new UnsupportedOperationException(s"You cannot get a chunk pointer from part column $columnID")
    } else {
      dataChunkPointer(id, columnID)
    }
  */

  /**
   * Obtains the correct VectorDataReader for the given column and pointer
   */
  final def chunkReader(columnID: Int, vector: BinaryVector.BinaryVectorPtr): VectorDataReader = {
    require(columnID < dataset.numDataColumns)
    dataset.dataReaders(columnID)(vector)
  }

  /**
   * Finds all of the relevant chunks depending on the scan method, returning all the ChunkSetInfos
   * in order of increasing ChunkID.
   */
  def infos(method: ChunkScanMethod): ChunkInfoIterator

  def infos(startTime: Long, endTime: Long): ChunkInfoIterator

  /**
    * Use to check if a ChunkScanMethod for this partition results in any chunks
    */
  def hasChunks(method: ChunkScanMethod): Boolean

  /**
   * Returns true if the partition has the chunk with the given id
   */
  def hasChunksAt(id: ChunkID): Boolean

  /**
   * Returns the earliest timestamp where the FiloPartiton has data
   */
  def earliestTime: Long

  /**
   * Produces an iterator of rows for the given time range and columns.
   * @param startTime starting timestamp, in milliseconds since Epoch
   * @param endTime ending timestamp, in milliseconds since Epoch
   * @param columnIDs the column IDs to query
   */
  final def timeRangeRows(startTime: Long, endTime: Long, columnIDs: Array[ColumnId]): Iterator[RowReader] =
    new PartitionTimeRangeReader(this, startTime, endTime, infos(startTime, endTime), columnIDs)

  final def timeRangeRows(method: ChunkScanMethod, columnIDs: Array[ColumnId]): Iterator[RowReader] =
    new PartitionTimeRangeReader(this, method.startTime, method.endTime, infos(method), columnIDs)

}
