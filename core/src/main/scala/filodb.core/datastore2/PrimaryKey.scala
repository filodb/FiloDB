package filodb.core.datastore2

import scala.math.Ordering

/**
 * Definitions for primary keys and key ranges.
 * Primary keys must be linearizable so all the data in a partition can be evenly divisible into segments.
 * For more info, see [[doc/sorted_chunk_merge.md]].
 */

/**
 * A typeclass for working with primary keys.
 */
trait PrimaryKeyHelper[T] {
  def ordering: Ordering[T]    // must be comparable

  /**
   * Returns the inclusive start and exclusive end keys for the segment corresponding to a primary key.
   * Must return the same start and end for all keys within [start, end) of a segment.
   */
  def getSegment(key: T): (T, T)
}

/**
 * A typeclass for a timestamp based on a Long = milliseconds since Epoch
 */
case class TimestampKeyHelper(intervalMs: Long) extends PrimaryKeyHelper[Long] {
  def ordering = Ordering.Long
  def getSegment(key: Long): (Long, Long) = {
    val segmentNum = key / intervalMs
    (segmentNum * intervalMs, (segmentNum + 1) * intervalMs)
  }
}

// A range of keys, used for describing ingest rows as well as queries
case class KeyRange[K : PrimaryKeyHelper](dataset: Types.TableName,
                                          partition: Types.PartitionKey,
                                          start: K, end: K)


