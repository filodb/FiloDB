package filodb.core

import java.nio.ByteBuffer

import filodb.core.binaryrecord.BinaryRecord
import filodb.memory.format.ZeroCopyUTF8String

/**
 * Temporary home for new FiloDB API definitions, including column store and memtable etc.
 * Perhaps they should be moved into filodb.core.store and filodb.core.reprojector
 *
 * NOT included: the MetadataStore, which contains column, partition, version definitions
 */
object Types {
  // A Chunk is a single columnar chunk for a given table, partition, column
  type Chunk = ByteBuffer
  type ColumnId = Int
  type ChunkID = Long     // Each chunk is identified by segmentID and a long timestamp

  type PartitionKey = BinaryRecord

  // Hashcodes are cached in a UTF8String, so it is strongly recommended to use a cache of map keys and
  // reuse instances of UTF8MapKey
  type UTF8MapKey = ZeroCopyUTF8String

  type UTF8Map = Map[UTF8MapKey, ZeroCopyUTF8String]

  val emptyUTF8Map = Map.empty[UTF8MapKey, ZeroCopyUTF8String]
}

// database is like Cassandra keyspace, or HiveMetaStore/RDBMS database - a namespace for tables
case class DatasetRef(dataset: String, database: Option[String] = None) {
  override def toString: String =
    database.map { db => s"$db.$dataset" }.getOrElse(dataset)
}

object DatasetRef {
  def fromDotString(str: String): DatasetRef =
    if (str contains ".") {
      val Array(db, dataset) = str.split('.')
      DatasetRef(dataset, Some(db))
    } else {
      DatasetRef(str)
    }
}
