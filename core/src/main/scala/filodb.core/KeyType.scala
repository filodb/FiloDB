package filodb.core

import org.velvia.filo.RowReader
import org.velvia.filo.RowReader._
import scodec.bits.{ByteOrdering, ByteVector}

import scala.math.Ordering

/**
 * A generic typeclass for dealing with keys (partition, sort, segment) of various types.
 * Including comparison, serialization, extraction from rows.
 */
trait KeyType {
  type T

  def ordering: Ordering[T] // must be comparable

  /**
   * Serializes the key to bytes. Preferably, the bytes should themselves be comparable
   * byte by byte as unsigned bytes, like how Cassandra's blob type is comparable.
   */
  def toBytes(key: T): ByteVector

  def fromBytes(bytes: ByteVector): T

  // Extracts the type T from a RowReader
  def getKeyFunc(columnNumbers: Seq[Int]): RowReader => T

  def size(key: T): Int
}

object KeyType {
  val ValidSortClasses = Seq(classOf[Long], classOf[Int], classOf[Double], classOf[String])
}

abstract class SingleKeyType extends KeyType {
  def getKeyFunc(columnNumbers: Seq[Int]): RowReader => T = {
    require(columnNumbers.length == 1)
    extractor.getField(_, columnNumbers.head)
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

/**
 * A generic composite key type which serializes by putting a byte length prefix
 * in front of each element.
 * TODO(velvia): handle components which are more than 256 bytes long...
 * TODO(velvia): Think of a way to serialize such that keys are somewhat binary comparable
 */
case class CompositeKeyType(atomTypes: Seq[SingleKeyType]) extends KeyType {
  type T = Seq[_]

  override def ordering: scala.Ordering[Seq[_]] = CompositeOrdering(atomTypes)

  override def toBytes(key: Seq[_]): ByteVector = {
    (0 until atomTypes.length).map { i =>
      val atomType = atomTypes(i)
      val bytes = atomType.toBytes(key(i).asInstanceOf[atomType.T])
      ByteVector(bytes.length.toByte) ++ bytes
    }.reduce[ByteVector] { case (a, b) => a ++ b }
  }

  override def fromBytes(bytes: ByteVector): Seq[_] = {
    var currentOffset = 0
    atomTypes.map { atomType =>
      val length: Int = bytes.get(currentOffset).toInt
      currentOffset = currentOffset + 1
      val atomBytes = bytes.slice(currentOffset, currentOffset + length)
      currentOffset = currentOffset + length
      atomType.fromBytes(atomBytes)
    }
  }

  override def getKeyFunc(sortColNums: Seq[Int]): (RowReader) => Seq[_] = {
    def toSeq(rowReader: RowReader): Seq[_] = atomTypes.
      zip(sortColNums).map { case (t, colNum) =>
      t.extractor.getField(rowReader, colNum)
    }
    toSeq
  }

  override def size(keys: Seq[_]): Int = atomTypes.zip(keys).map {
    case (t, k) => t.size(k.asInstanceOf[t.T])
  }.sum
}

/**
 * Typeclasses for keys
 * NOTE: both the Ordering for ByteVector as well as how bytes are compared in most places is big-endian
 * TODO: make byte comparison unsigned
 */
case object LongKeyType extends SingleKeyType {
  type T = Long

  def ordering: Ordering[Long] = Ordering.Long

  def toBytes(key: Long): ByteVector = ByteVector.fromLong(key, ordering = ByteOrdering.BigEndian)

  def fromBytes(bytes: ByteVector): Long = bytes.toLong(signed = true, ByteOrdering.BigEndian)

  override def extractor: TypedFieldExtractor[Long] = LongFieldExtractor

  override def size(key: Long): Int = 8
}

case object IntKeyType extends SingleKeyType {
  type T = Int

  def ordering: Ordering[Int] = Ordering.Int

  def toBytes(key: Int): ByteVector = ByteVector.fromInt(key, ordering = ByteOrdering.BigEndian)

  def fromBytes(bytes: ByteVector): Int = bytes.toInt(signed = true, ByteOrdering.BigEndian)

  override def extractor: TypedFieldExtractor[Int] = IntFieldExtractor

  override def size(key: Int): Int = 4
}

case object DoubleKeyType extends SingleKeyType {
  type T = Double

  def ordering: Ordering[Double] = Ordering.Double

  def toBytes(key: Double): ByteVector =
    ByteVector.fromLong(java.lang.Double.doubleToLongBits(key), ordering = ByteOrdering.BigEndian)

  def fromBytes(bytes: ByteVector): Double =
    java.lang.Double.longBitsToDouble(bytes.toLong(signed = true, ByteOrdering.BigEndian))

  override def extractor: TypedFieldExtractor[Double] = DoubleFieldExtractor

  override def size(key: Double): Int = 8
}

case object StringKeyType extends SingleKeyType {
  type T = String

  def ordering: Ordering[String] = Ordering.String

  def toBytes(key: String): ByteVector = ByteVector(key.getBytes("UTF-8"))

  def fromBytes(bytes: ByteVector): String = new String(bytes.toArray, "UTF-8")

  override def extractor: TypedFieldExtractor[String] = StringFieldExtractor

  override def size(key: String): Int = key.getBytes("UTF-8").length
}

