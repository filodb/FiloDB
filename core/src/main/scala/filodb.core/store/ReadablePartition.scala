package filodb.core.store

import filodb.core.Types.{ChunkID, ColumnId}
import filodb.core.metadata.Schema
import filodb.core.query.{PartitionTimeRangeReader, RangeVectorCursor}
import filodb.memory.format.{BinaryVector, MemoryReader, UnsafeUtils, VectorDataReader}


trait FiloPartition {
  def schema: Schema
  def partKeyBase: Array[Byte]
  def partKeyOffset: Long
  def partID: Int

  /**
    * Two FiloPartitions are equal if they have the same partition key.  This test is used in various
    * data structures.
    */
  override def hashCode: Int = schema.partKeySchema.partitionHash(partKeyBase, partKeyOffset)

  override def equals(other: Any): Boolean = other match {
    case UnsafeUtils.ZeroPointer => false
    case f: FiloPartition => schema.partKeySchema.equals(partKeyBase, partKeyOffset, f.partKeyBase, f.partKeyOffset)
    case _: Any           => false
  }

}

class EmptyPartition(val schema: Schema,
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
  def stringPartition: String = schema.partKeySchema.stringify(partKeyBase, partKeyOffset)

  // Returns binary partition key as a new, copied byte array
  def partKeyBytes: Array[Byte] = schema.partKeySchema.asByteArray(partKeyBase, partKeyOffset)

  def hexPartKey: String = schema.partKeySchema.toHexString(partKeyBase, partKeyOffset)

  def numChunks: Int

  /**
   * The length or # of data samples of the set of appending or ingesting chunks.  0 if there are no
   * appendable chunks or write buffers.
   */
  def appendingChunkLen: Int

  def shard: Int

  def minResolutionMs: Int = 1

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
  final def chunkReader(columnID: Int, acc: MemoryReader, vector: BinaryVector.BinaryVectorPtr): VectorDataReader = {
    require(columnID < schema.numDataColumns)
    schema.dataReader(columnID, acc, vector)
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
  final def timeRangeRows(startTime: Long, endTime: Long, columnIDs: Array[ColumnId]): RangeVectorCursor =
    new PartitionTimeRangeReader(this, startTime, endTime, infos(startTime, endTime), columnIDs)

  final def timeRangeRows(method: ChunkScanMethod, columnIDs: Array[ColumnId]): RangeVectorCursor =
    new PartitionTimeRangeReader(this, method.startTime, method.endTime, infos(method), columnIDs)

  final def timeRangeRows(method: ChunkScanMethod,
                          columnIDs: Array[ColumnId],
                          countInfoIterator: CountingChunkInfoIterator): RangeVectorCursor =
    new PartitionTimeRangeReader(this, method.startTime, method.endTime, countInfoIterator, columnIDs)

  /**
   * @param schemaNameToCheck the schema name to check against
   * @param schemaHashToCheck the schema hash to check against
   * @return true if the schema name and hash match or if the schemas are back compatible histograms
   */
  final def doesSchemaMatchOrBackCompatibleHistograms(schemaNameToCheck : String, schemaHashToCheck : Int) : Boolean = {
    if (schema.schemaHash == schemaHashToCheck) { true }
    else {
      val sortedSchemas = Seq(schema.name, schemaNameToCheck).sortBy(_.length)
      val ret = if ((sortedSchemas(0) == "prom-histogram") && (sortedSchemas(1) == "otel-cumulative-histogram")) true
      else if ((sortedSchemas(0) == "delta-histogram") && (sortedSchemas(1) == "otel-delta-histogram")) true
      else false
      ret
    }
  }

  def publishInterval: Option[Long]
}
