package filodb.core

import org.velvia.filo.RowReader
import org.velvia.filo.RowReader._
import scala.language.postfixOps
import scala.util.{Failure, Try}
import scalaxy.loops._
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

  /**
   * Extracts the type T from a RowReader.
   * @params columnNumbers an array of column numbers to extract from.  Sorry, must be
   *                       an array for speed.
   * @throws a NullKeyValue exception if nulls are found.  Nulls cannot be injected into
   *         keys.
   */
  def getKeyFunc(columnNumbers: Array[Int]): RowReader => T

  def size(key: T): Int
}

object KeyType {
  val ClazzToKeyType = Map[Class[_], SingleKeyType](
    classOf[Long]   -> LongKeyType,
    classOf[Int]    -> IntKeyType,
    classOf[Double] -> DoubleKeyType,
    classOf[String] -> StringKeyType
  )

  def getKeyType(clazz: Class[_]): Try[SingleKeyType] =
    ClazzToKeyType.get(clazz)
                  .map(kt => util.Success(kt))
                  .getOrElse(Failure(UnsupportedKeyType(clazz)))
}

import scala.language.existentials
case class NullKeyValue(colIndex: Int) extends Exception(s"Null partition value for col index $colIndex")
case class UnsupportedKeyType(clazz: Class[_]) extends Exception(s"Type $clazz is not supported for keys")

abstract class SingleKeyType extends KeyType {
  def getKeyFunc(columnNumbers: Array[Int]): RowReader => T = {
    require(columnNumbers.size == 1)
    val columnNum = columnNumbers(0)
    (r: RowReader) => {
      if (r.notNull(columnNum)) {
        extractor.getField(r, columnNum)
      } else {
        throw NullKeyValue(columnNum)
      }
    }
  }

  def extractor: TypedFieldExtractor[T]
}

case class CompositeOrdering(atomTypes: Seq[SingleKeyType]) extends Ordering[Seq[_]] {
  override def compare(x: Seq[_], y: Seq[_]): Int = {
    if (x.length == y.length && x.length == atomTypes.length) {
      for { i <- 0 until x.length optimized } {
        val keyType = atomTypes(i)
        val res = keyType.ordering.compare(x(i).asInstanceOf[keyType.T],
                                           y(i).asInstanceOf[keyType.T])
        if (res != 0) return res
      }
      return 0
    }
    throw new IllegalArgumentException("Comparing wrong composite types")
  }
}

/**
 * A generic composite key type which serializes by putting a byte length prefix
 * in front of each element.
 * TODO(velvia): handle components which are more than 256 bytes long...
 * TODO(velvia): Think of a way to serialize such that keys are somewhat binary comparable
 * TODO(velvia): Optimize performance.  Use Metal vector types, custom generated classes,
 *               anything except terribly inefficient Seq[_]
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

  override def getKeyFunc(columnNumbers: Array[Int]): (RowReader) => Seq[_] = {
    require(columnNumbers.size == atomTypes.length)

    def getValue(rowReader: RowReader, keyType: SingleKeyType, columnNo: Int): Any =
      if (rowReader.notNull(columnNo)) {
        keyType.extractor.getField(rowReader, columnNo)
      } else {
        throw NullKeyValue(columnNo)
      }

    def toSeq(rowReader: RowReader): Seq[_] = {
      (0 until columnNumbers.size).map { i =>
        getValue(rowReader, atomTypes(i), columnNumbers(i))
      }
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
trait LongKeyTypeLike extends SingleKeyType {
  type T = Long

  def ordering: Ordering[Long] = Ordering.Long

  def toBytes(key: Long): ByteVector = ByteVector.fromLong(key, ordering = ByteOrdering.BigEndian)

  def fromBytes(bytes: ByteVector): Long = bytes.toLong(signed = true, ByteOrdering.BigEndian)

  override def extractor: TypedFieldExtractor[Long] = LongFieldExtractor

  override def size(key: Long): Int = 8
}

case object LongKeyType extends LongKeyTypeLike

trait IntKeyTypeLike extends SingleKeyType {
  type T = Int

  def ordering: Ordering[Int] = Ordering.Int

  def toBytes(key: Int): ByteVector = ByteVector.fromInt(key, ordering = ByteOrdering.BigEndian)

  def fromBytes(bytes: ByteVector): Int = bytes.toInt(signed = true, ByteOrdering.BigEndian)

  override def extractor: TypedFieldExtractor[Int] = IntFieldExtractor

  override def size(key: Int): Int = 4
}

case object IntKeyType extends IntKeyTypeLike

trait DoubleKeyTypeLike extends SingleKeyType {
  type T = Double

  def ordering: Ordering[Double] = Ordering.Double

  def toBytes(key: Double): ByteVector =
    ByteVector.fromLong(java.lang.Double.doubleToLongBits(key), ordering = ByteOrdering.BigEndian)

  def fromBytes(bytes: ByteVector): Double =
    java.lang.Double.longBitsToDouble(bytes.toLong(signed = true, ByteOrdering.BigEndian))

  override def extractor: TypedFieldExtractor[Double] = DoubleFieldExtractor

  override def size(key: Double): Int = 8
}

case object DoubleKeyType extends DoubleKeyTypeLike

trait StringKeyTypeLike extends SingleKeyType {
  type T = String

  def ordering: Ordering[String] = Ordering.String

  def toBytes(key: String): ByteVector = ByteVector(key.getBytes("UTF-8"))

  def fromBytes(bytes: ByteVector): String = new String(bytes.toArray, "UTF-8")

  override def extractor: TypedFieldExtractor[String] = StringFieldExtractor

  override def size(key: String): Int = key.getBytes("UTF-8").length
}

case object StringKeyType extends StringKeyTypeLike

