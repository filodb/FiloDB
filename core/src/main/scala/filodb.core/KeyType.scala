package filodb.core

import org.velvia.filo.RowReader
import org.velvia.filo.RowReader._
import scodec.bits.{ByteOrdering, ByteVector}

import scala.math.Ordering

/**
 * Definitions for sort keys and key ranges.
 * Keys must be Comparable and Serializable.
 */

trait KeyType {
  type T

  def ordering: Ordering[T] // must be comparable

  def toBytes(key: T): ByteVector

  def fromBytes(bytes: ByteVector): T

  def getKeyFunc(sortColNums: Seq[Int]): RowReader => T

}

object KeyType {
  val ValidSortClasses = Seq(classOf[Long], classOf[Int], classOf[Double], classOf[String])
}

abstract class SingleKeyType extends KeyType {

  def getKeyFunc(sortColNums: Seq[Int]): RowReader => T = {
    require(sortColNums.length == 1)
    extractor.getField(_, sortColNums.head)
  }

  def extractor: TypedFieldExtractor[T]

}

case class CompositeOrdering(atomTypes: Seq[SingleKeyType]) extends Ordering[Seq[_]] {
  override def compare(x: Seq[_], y: Seq[_]): Int = {
    import scala.math.Ordered.orderingToOrdered
    if (x.length == y.length && x.length == atomTypes.length) {
      (0 to x.length).foreach { i =>
        val keyType = atomTypes(i)
        implicit val ordering = keyType.ordering
        val xi = x(i).asInstanceOf[keyType.T]
        val yi = y(i).asInstanceOf[keyType.T]
        val res = xi compare yi
        if (res != 0) res
      }
    }
    throw new IllegalArgumentException("Comparing wrong composite types")
  }

}

case class CompositeKeyType(atomTypes: Seq[SingleKeyType]) extends KeyType {

  type T = Seq[_]

  override def ordering: scala.Ordering[Seq[_]] = CompositeOrdering(atomTypes)

  override def toBytes(key: Seq[_]): ByteVector = (0 to atomTypes.length).map { i =>
    val atomType = atomTypes(i)
    val bytes = atomType.toBytes(key(i).asInstanceOf[atomType.T])
    ByteVector(bytes.length.toByte).++(bytes)
  }.reduce[ByteVector] { case (a, b) => a ++ b }

  override def fromBytes(bytes: ByteVector): Seq[_] = {
    val elements = scala.collection.mutable.ListBuffer.empty[Any]
    var currentOffset = 0
    (0 to atomTypes.length).foreach { i =>
      val atomType = atomTypes(i)
      val length: Int = bytes.get(currentOffset).toInt
      currentOffset = currentOffset + 1
      val atomBytes = bytes.slice(currentOffset, currentOffset + length)
      currentOffset = currentOffset + length
      elements += atomType.fromBytes(atomBytes)
    }
    elements.toSeq
  }

  override def getKeyFunc(sortColNums: Seq[Int]): (RowReader) => Seq[_] = {
    def toSeq(rowReader: RowReader): Seq[_] = atomTypes.
      zipWithIndex.map { case (t, i) =>
      t.extractor.getField(rowReader, sortColNums(i))
    }
    toSeq
  }
}

/**
 * Typeclasses for sort keys
 * NOTE: both the Ordering for ByteVector as well as how bytes are compared in most places is big-endian
 */
case class LongKeyType() extends SingleKeyType {
  type T = Long

  def ordering: Ordering[Long] = Ordering.Long

  def toBytes(key: Long): ByteVector = ByteVector.fromLong(key, ordering = ByteOrdering.BigEndian)

  def fromBytes(bytes: ByteVector): Long = bytes.toLong(true, ByteOrdering.BigEndian)

  override def extractor: TypedFieldExtractor[Long] = LongFieldExtractor
}

case class IntKeyType() extends SingleKeyType {
  type T = Int

  def ordering: Ordering[Int] = Ordering.Int

  def toBytes(key: Int): ByteVector = ByteVector.fromInt(key, ordering = ByteOrdering.BigEndian)

  def fromBytes(bytes: ByteVector): Int = bytes.toInt(true, ByteOrdering.BigEndian)

  override def extractor: TypedFieldExtractor[Int] = IntFieldExtractor
}

case class DoubleKeyType() extends SingleKeyType {
  type T = Double

  def ordering: Ordering[Double] = Ordering.Double

  def toBytes(key: Double): ByteVector =
    ByteVector.fromLong(java.lang.Double.doubleToLongBits(key), ordering = ByteOrdering.BigEndian)

  def fromBytes(bytes: ByteVector): Double =
    java.lang.Double.longBitsToDouble(bytes.toLong(true, ByteOrdering.BigEndian))

  override def extractor: TypedFieldExtractor[Double] = DoubleFieldExtractor
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
case class StringKeyType(prefixLen: Int) extends SingleKeyType {
  type T = String

  def ordering: Ordering[String] = Ordering.String

  def toBytes(key: String): ByteVector = ByteVector(key.take(prefixLen).getBytes("UTF-8"))

  def fromBytes(bytes: ByteVector): String = new String(bytes.toArray, "UTF-8")

  override def extractor: TypedFieldExtractor[String] = StringFieldExtractor
}

