package filodb.core.datastore2

import java.nio.ByteBuffer
import scala.math.Ordering

/**
 * Definitions for primary keys and key ranges.
 * Primary keys must be linearizable so all the data in a partition can be evenly divisible into segments.
 * For more info, see [[doc/sorted_chunk_merge.md]].
 */

/**
 * A typeclass for working with primary keys.
 */
trait PrimaryKeyHelper[K] {
  def ordering: Ordering[K]    // must be comparable

  /**
   * Returns the inclusive start and exclusive end keys for the segment corresponding to a primary key.
   * Must return the same start and end for all keys within [start, end) of a segment.
   */
  def getSegment(key: K): (K, K)

  def toBytes(key: K): ByteBuffer
  def fromBytes(bytes: ByteBuffer): K
}

/**
 * A typeclass for a timestamp based on a Long = milliseconds since Epoch
 */
case class TimestampKeyHelper(intervalMs: Long) extends PrimaryKeyHelper[Long] {
  def ordering: Ordering[Long] = Ordering.Long
  def getSegment(key: Long): (Long, Long) = {
    val segmentNum = key / intervalMs
    (segmentNum * intervalMs, (segmentNum + 1) * intervalMs)
  }
  def toBytes(key: Long): ByteBuffer = ByteBuffer.allocate(java.lang.Long.BYTES).putLong(key)
  def fromBytes(bytes: ByteBuffer): Long = bytes.getLong
}

// A range of keys, used for describing ingest rows as well as queries
case class KeyRange[K : PrimaryKeyHelper](dataset: Types.TableName,
                                          partition: Types.PartitionKey,
                                          start: K, end: K)


