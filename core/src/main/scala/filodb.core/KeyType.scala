package filodb.core

import org.velvia.filo.RowReader
import org.velvia.filo.RowReader._
import scodec.bits.{ByteOrdering, ByteVector}

import scala.math.Ordering

/**
 * Definitions for sort keys and key ranges.
 * Keys must be Comparable and Serializable.
 */

trait KeyType[K] {

  def ordering: Ordering[K] // must be comparable

  def toBytes(key: K): ByteVector

  def fromBytes(bytes: ByteVector): K

  def getKeyFunc(sortColNums: Seq[Int]): RowReader => K

}

object KeyType {
  val ValidSortClasses = Seq(classOf[Long], classOf[Int], classOf[Double], classOf[String])
}

abstract class SingleKeyType[K: TypedFieldExtractor] extends KeyType[K] {
  val extractor = implicitly[TypedFieldExtractor[K]]

  def getKeyFunc(sortColNums: Seq[Int]): RowReader => K = {
    require(sortColNums.length == 1)
    extractor.getField(_, sortColNums.head)
  }

}

/**
 * Typeclasses for sort keys
 * NOTE: both the Ordering for ByteVector as well as how bytes are compared in most places is big-endian
 */
case class LongKeyType(segmentLen: Long) extends SingleKeyType[Long] {
  def ordering: Ordering[Long] = Ordering.Long

  def toBytes(key: Long): ByteVector = ByteVector.fromLong(key, ordering = ByteOrdering.BigEndian)

  def fromBytes(bytes: ByteVector): Long = bytes.toLong(true, ByteOrdering.BigEndian)
}

case class IntKeyType(segmentLen: Int) extends SingleKeyType[Int] {
  def ordering: Ordering[Int] = Ordering.Int

  def toBytes(key: Int): ByteVector = ByteVector.fromInt(key, ordering = ByteOrdering.BigEndian)

  def fromBytes(bytes: ByteVector): Int = bytes.toInt(true, ByteOrdering.BigEndian)
}

case class DoubleKeyType(segmentLen: Double) extends SingleKeyType[Double] {
  def ordering: Ordering[Double] = Ordering.Double

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
case class StringKeyType(prefixLen: Int) extends SingleKeyType[String] {
  def ordering: Ordering[String] = Ordering.String

  def toBytes(key: String): ByteVector = ByteVector(key.take(prefixLen).getBytes("UTF-8"))

  def fromBytes(bytes: ByteVector): String = new String(bytes.toArray, "UTF-8")
}

