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
  def ordering: Ordering[K]        // must be comparable
  def minBinaryValue: ByteVector   // A minimum binary value for this type of key
  val minValue: K                  // the minimum value for this type of key

  def toBytes(key: K): ByteVector
  def fromBytes(bytes: ByteVector): K

  def getSortKeyFunc(sortColNums: Seq[Int]): RowReader => K

  /**
   * Intended for the start and end values for KeyRange / SegmentInfo / SegmentMeta etc.
   * If key is None, then the minBinaryValue is encoded.
   */
  def toSegmentBinary(key: Option[K]): ByteVector = key.map(toBytes).getOrElse(minBinaryValue)
  def fromSegmentBinary(bytes: ByteVector): Option[K] =
    if (bytes == minBinaryValue) None else Some(fromBytes(bytes))
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

  def minBinaryValue: ByteVector = toBytes(minValue)
}

/**
 * Typeclasses for sort keys
 * NOTE: both the Ordering for ByteVector as well as how bytes are compared in most places is big-endian
 */
case object LongKeyHelper extends SingleSortKeyHelper[Long] {
  def ordering: Ordering[Long] = Ordering.Long
  val minValue = Long.MinValue

  def toBytes(key: Long): ByteVector = ByteVector.fromLong(key + Long.MinValue,
                                                           ordering = ByteOrdering.BigEndian)
  def fromBytes(bytes: ByteVector): Long = bytes.toLong(true, ByteOrdering.BigEndian) - Long.MinValue
}

case object IntKeyHelper extends SingleSortKeyHelper[Int] {
  def ordering: Ordering[Int] = Ordering.Int
  val minValue = Int.MinValue

  def toBytes(key: Int): ByteVector = ByteVector.fromInt(key + Int.MinValue,
                                                         ordering = ByteOrdering.BigEndian)
  def fromBytes(bytes: ByteVector): Int = bytes.toInt(true, ByteOrdering.BigEndian) - Int.MinValue
}

case object DoubleKeyHelper extends SingleSortKeyHelper[Double] {
  def ordering: Ordering[Double] = Ordering.Double
  val minValue = Double.MinValue

  def toBytes(key: Double): ByteVector =
    ByteVector.fromLong(java.lang.Double.doubleToLongBits(key), ordering = ByteOrdering.BigEndian)
  def fromBytes(bytes: ByteVector): Double =
    java.lang.Double.longBitsToDouble(bytes.toLong(true, ByteOrdering.BigEndian))
}

case object StringKeyHelper extends SingleSortKeyHelper[String] {
  def ordering: Ordering[String] = Ordering.String
  val minValue = ""

  def toBytes(key: String): ByteVector = ByteVector(key.getBytes("UTF-8"))
  def fromBytes(bytes: ByteVector): String = new String(bytes.toArray, "UTF-8")
}

