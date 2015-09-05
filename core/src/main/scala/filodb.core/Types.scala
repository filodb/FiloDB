package filodb.core

import java.nio.ByteBuffer

/**
 * Temporary home for new FiloDB API definitions, including column store and memtable etc.
 * Perhaps they should be moved into filodb.core.columnstore and filodb.core.reprojector
 *
 * NOT included: the MetadataStore, which contains column, partition, version definitions
 */
object Types {
  // A Chunk is a single columnar chunk for a given table, partition, column
  type Chunk = ByteBuffer
  // TODO: Change ColumnId to an Int.  Would be more efficient, and allow renaming columns.
  type ColumnId = String
  type TableName = String
  type ChunkID = Int    // Each chunk is identified by segmentID and a long timestamp

  type SortOrder = Set[(ColumnId, Boolean)]

  // TODO: support composite partition keys?
  type PartitionKey = String
}

// A range of keys, used for describing ingest rows as well as queries
case class KeyRange[K : SortKeyHelper](dataset: Types.TableName,
                                       partition: Types.PartitionKey,
                                       start: K, end: K) {
  val helper = implicitly[SortKeyHelper[K]]
  def binaryStart: ByteBuffer = helper.toBytes(start)
  def binaryEnd: ByteBuffer = helper.toBytes(end)
}
