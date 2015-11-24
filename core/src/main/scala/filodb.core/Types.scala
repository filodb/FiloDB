package filodb.core

import scodec.bits.ByteVector

import scalaxy.loops._

/**
 * Temporary home for new FiloDB API definitions, including column store and memtable etc.
 * Perhaps they should be moved into filodb.core.columnstore and filodb.core.reprojector
 *
 * NOT included: the MetadataStore, which contains column, partition, version definitions
 */
object Types {

  // TODO: Change ColumnId to an Int.  Would be more efficient, and allow renaming columns.
  type ColumnId = String
  type TableName = String
  type ChunkId = Int // Each chunk is identified by segmentID and a long timestamp

  type SortOrder = Set[(ColumnId, Boolean)]

  // TODO: contribute this Ordering back to ByteVector
  // Assumes unsigned comparison, big endian, meaning that the first byte in a vector
  // is the most significant one.
  // Compares byte by byte, if all bytes equal up to the min length for both, then lengths are compared
  implicit object BigEndianByteVectorOrdering extends Ordering[ByteVector] {
    def compare(x: ByteVector, y: ByteVector): Int = {
      val minLen = Math.min(x.length, y.length)
      for {i <- 0 until minLen optimized} {
        val byteCompare = (x(i) & 0x00ff) compare (y(i) & 0x00ff)
        if (byteCompare != 0) return byteCompare
      }
      x.length compare y.length
    }
  }


}

