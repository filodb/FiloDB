package filodb.core

import java.nio.ByteBuffer
import scalaxy.loops._
import scodec.bits.ByteVector

import filodb.core.metadata.RichProjection

/**
 * Temporary home for new FiloDB API definitions, including column store and memtable etc.
 * Perhaps they should be moved into filodb.core.store and filodb.core.reprojector
 *
 * NOT included: the MetadataStore, which contains column, partition, version definitions
 */
object Types {
  // A Chunk is a single columnar chunk for a given table, partition, column
  type Chunk = ByteBuffer
  type SegmentId = ByteVector
  // TODO: Change ColumnId to an Int.  Would be more efficient, and allow renaming columns.
  type ColumnId = String
  type ChunkID = Long     // Each chunk is identified by segmentID and a long timestamp

  type BinaryPartition = ByteVector

  // TODO: contribute this Ordering back to ByteVector
  // Assumes unsigned comparison, big endian, meaning that the first byte in a vector
  // is the most significant one.
  // Compares byte by byte, if all bytes equal up to the min length for both, then lengths are compared
  implicit object BigEndianByteVectorOrdering extends Ordering[ByteVector] {
    def compare(x: ByteVector, y: ByteVector): Int = {
      val minLen = Math.min(x.length, y.length)
      for { i <- 0 until minLen optimized } {
        val byteCompare = (x(i) & 0x00ff) compare (y(i) & 0x00ff)
        if (byteCompare != 0) return byteCompare
      }
      x.length compare y.length
    }
  }
}

// database is like Cassandra keyspace, or HiveMetaStore/RDBMS database - a namespace for tables
case class DatasetRef(dataset: String, database: Option[String] = None) {
  override def toString: String =
    database.map { db => s"$db.$dataset" }.getOrElse(dataset)
}

// A range of keys, used for describing ingest rows as well as queries
// Right now this describes a range of segments, not row keys.
case class KeyRange[PK, SK](partition: PK,
                            start: SK, end: SK,
                            endExclusive: Boolean = true) {
  def basedOn(projection: RichProjection): KeyRange[projection.PK, projection.SK] =
    this.asInstanceOf[KeyRange[projection.PK, projection.SK]]
}

case class BinaryKeyRange(partition: Types.BinaryPartition,
                          start: Types.SegmentId, end: Types.SegmentId,
                          endExclusive: Boolean = true)

case class SegmentRange[SK](start: SK, end: SK) {
  def basedOn(projection: RichProjection): SegmentRange[projection.SK] =
    this.asInstanceOf[SegmentRange[projection.SK]]
}

case class BinarySegmentRange(start: Types.SegmentId, end: Types.SegmentId)
