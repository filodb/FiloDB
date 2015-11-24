package filodb.core

import org.velvia.filo.RowReader
import org.velvia.filo.RowReader._

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
  type Key = K
  def ordering: Ordering[K]    // must be comparable

  /**
   * Returns the inclusive start and exclusive end keys for the segment corresponding to a sort key.
   * Must return the same start and end for all keys within [start, end) of a segment.
   */
  def getSegment(key: K): (K, K)

  def toBytes(key: K): ByteVector
  def fromBytes(bytes: ByteVector): K

  def getSortKeyFunc(sortColNums: Seq[Int]): RowReader => K
}

object SortKeyHelper {
  val ValidSortClasses = Seq(classOf[Long], classOf[Int], classOf[Double], classOf[String])
}

abstract class SingleSortKeyHelper[K: TypedFieldExtractor] extends SortKeyHelper[K] {
  val extractor = implicitly[TypedFieldExtractor[K]]

  def getSortKeyFunc(sortColNums: Seq[Int]): RowReader => K = {
    require(sortColNums.length == 1)
    extractor.getField(_, sortColNums.head)
  }
}

/**
 * Typeclasses for sort keys
 * NOTE: both the Ordering for ByteVector as well as how bytes are compared in most places is big-endian
 */
case class LongKeyHelper(segmentLen: Long) extends SingleSortKeyHelper[Long] {
  def ordering: Ordering[Long] = Ordering.Long
  def getSegment(key: Long): (Long, Long) = {
    val segmentNum = key / segmentLen
    (segmentNum * segmentLen, (segmentNum + 1) * segmentLen)
  }
  def toBytes(key: Long): ByteVector = ByteVector.fromLong(key, ordering = ByteOrdering.BigEndian)
  def fromBytes(bytes: ByteVector): Long = bytes.toLong(true, ByteOrdering.BigEndian)
}

case class IntKeyHelper(segmentLen: Int) extends SingleSortKeyHelper[Int] {
  def ordering: Ordering[Int] = Ordering.Int
  def getSegment(key: Int): (Int, Int) = {
    val segmentNum = key / segmentLen
    (segmentNum * segmentLen, (segmentNum + 1) * segmentLen)
  }
  def toBytes(key: Int): ByteVector = ByteVector.fromInt(key, ordering = ByteOrdering.BigEndian)
  def fromBytes(bytes: ByteVector): Int = bytes.toInt(true, ByteOrdering.BigEndian)
}

case class DoubleKeyHelper(segmentLen: Double) extends SingleSortKeyHelper[Double] {
  def ordering: Ordering[Double] = Ordering.Double
  def getSegment(key: Double): (Double, Double) = {
    val segmentNum = Math.floor(key / segmentLen)
    (segmentNum * segmentLen, (segmentNum + 1) * segmentLen)
  }
  def toBytes(key: Double): ByteVector =
    ByteVector.fromLong(java.lang.Double.doubleToLongBits(key), ordering = ByteOrdering.BigEndian)
  def fromBytes(bytes: ByteVector): Double =
    java.lang.Double.longBitsToDouble(bytes.toLong(true, ByteOrdering.BigEndian))
}

/**
 * Right now, you have to specify a prefixLen for the string key helper.
 * All string keys with identical prefixes (characters 0 to prefixLen - 1) will then be
 * bucketed into the same segment.
 * Thus one needs to look through your string sort key data and make sure to bucketize
 * correctly.
 *
 * TODO: perhaps combine prefixLen with a range of chars the last char is allowed to vary on
 */
case class StringKeyHelper(prefixLen: Int) extends SingleSortKeyHelper[String] {
  def ordering: Ordering[String] = Ordering.String
  def getSegment(key: String): (String, String) = {
    val start = key.take(prefixLen)
    val end = start.take(start.length - 1) + ((start(start.length - 1) + 1).toChar)
    (start, end)
  }

  private final val UTF8Encoding = "UTF-8"

  def toBytes(key: String): ByteVector = ByteVector(key.take(prefixLen).getBytes(UTF8Encoding))
  def fromBytes(bytes: ByteVector): String = new String(bytes.toArray, UTF8Encoding)
}

