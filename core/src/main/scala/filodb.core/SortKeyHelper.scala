package filodb.core

import scala.math.Ordering
import scodec.bits.{ByteVector, ByteOrdering}

/**
 * Definitions for sort keys and key ranges.
 * Sort keys must be linearizable so all the data in a partition can be evenly divisible into segments.
 * For more info, see [[doc/sorted_chunk_merge.md]].
 */

/**
 * A typeclass for working with sort keys.
 */
trait SortKeyHelper[K] {
  def ordering: Ordering[K]    // must be comparable

  /**
   * Returns the inclusive start and exclusive end keys for the segment corresponding to a sort key.
   * Must return the same start and end for all keys within [start, end) of a segment.
   */
  def getSegment(key: K): (K, K)

  def toBytes(key: K): ByteVector
  def fromBytes(bytes: ByteVector): K
}

/**
 * Typeclasses for sort keys
 */
case class LongKeyHelper(segmentLen: Long) extends SortKeyHelper[Long] {
  def ordering: Ordering[Long] = Ordering.Long
  def getSegment(key: Long): (Long, Long) = {
    val segmentNum = key / segmentLen
    (segmentNum * segmentLen, (segmentNum + 1) * segmentLen)
  }
  def toBytes(key: Long): ByteVector = ByteVector.fromLong(key, ordering = ByteOrdering.LittleEndian)
  def fromBytes(bytes: ByteVector): Long = bytes.toLong(true, ByteOrdering.LittleEndian)
}

case class IntKeyHelper(segmentLen: Int) extends SortKeyHelper[Int] {
  def ordering: Ordering[Int] = Ordering.Int
  def getSegment(key: Int): (Int, Int) = {
    val segmentNum = key / segmentLen
    (segmentNum * segmentLen, (segmentNum + 1) * segmentLen)
  }
  def toBytes(key: Int): ByteVector = ByteVector.fromInt(key, ordering = ByteOrdering.LittleEndian)
  def fromBytes(bytes: ByteVector): Int = bytes.toInt(true, ByteOrdering.LittleEndian)
}

case class DoubleKeyHelper(segmentLen: Double) extends SortKeyHelper[Double] {
  def ordering: Ordering[Double] = Ordering.Double
  def getSegment(key: Double): (Double, Double) = {
    val segmentNum = Math.floor(key / segmentLen)
    (segmentNum * segmentLen, (segmentNum + 1) * segmentLen)
  }
  def toBytes(key: Double): ByteVector =
    ByteVector.fromLong(java.lang.Double.doubleToLongBits(key), ordering = ByteOrdering.LittleEndian)
  def fromBytes(bytes: ByteVector): Double =
    java.lang.Double.longBitsToDouble(bytes.toLong(true, ByteOrdering.LittleEndian))
}

