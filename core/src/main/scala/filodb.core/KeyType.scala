package filodb.core

import java.sql.Timestamp
import org.joda.time.DateTime
import org.velvia.filo.RowReader
import org.velvia.filo.RowReader._
import scala.language.postfixOps
import scalaxy.loops._
import scodec.bits.{ByteOrdering, ByteVector}

import scala.math.Ordering

/**
 * A generic typeclass for dealing with keys (partition, sort, segment) of various types.
 * Including comparison, serialization, extraction from rows, extraction from strings.
 * In general, every column type has a KeyType, since any column could be used for any keys,
 * except for segment keys which have some special requirements.
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

  def fromString(str: String): T

  /**
   * Extracts the type T from a RowReader.
   * @params columnNumbers an array of column numbers to extract from.  Sorry, must be
   *                       an array for speed.
   * @throws a NullKeyValue exception if nulls are found.  Nulls cannot be injected into
   *         keys.
   */
  def getKeyFunc(columnNumbers: Array[Int]): RowReader => T

  def isSegmentType: Boolean = false

  def size(key: T): Int
}

import SingleKeyTypes._

case class NullKeyValue(colIndex: Int) extends Exception(s"Null partition value for col index $colIndex")

abstract class SingleKeyTypeBase[K : Ordering : TypedFieldExtractor] extends KeyType {
  type T = K

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

  def ordering: Ordering[T] = implicitly[Ordering[K]]

  def extractor: TypedFieldExtractor[T] = implicitly[TypedFieldExtractor[K]]
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

  def ordering: scala.Ordering[Seq[_]] = CompositeOrdering(atomTypes)

  def toBytes(key: Seq[_]): ByteVector = {
    (0 until atomTypes.length).map { i =>
      val atomType = atomTypes(i)
      val bytes = atomType.toBytes(key(i).asInstanceOf[atomType.T])
      ByteVector(bytes.length.toByte) ++ bytes
    }.reduce[ByteVector] { case (a, b) => a ++ b }
  }

  def fromBytes(bytes: ByteVector): Seq[_] = {
    var currentOffset = 0
    atomTypes.map { atomType =>
      val length: Int = bytes.get(currentOffset).toInt
      currentOffset = currentOffset + 1
      val atomBytes = bytes.slice(currentOffset, currentOffset + length)
      currentOffset = currentOffset + length
      atomType.fromBytes(atomBytes)
    }
  }

  def fromString(str: String): T = {
    val components = str.split(",")
    (0 until atomTypes.length).map { i =>
      atomTypes(i).fromString(components(i))
    }
  }

  override def getKeyFunc(columnNumbers: Array[Int]): (RowReader) => Seq[_] = {
    require(columnNumbers.size == atomTypes.length)

    val keyFuncs: Seq[RowReader => Any] = atomTypes.zip(columnNumbers).map { case (atomType, colNo) =>
      atomType.getKeyFunc(Array(colNo)).asInstanceOf[RowReader => Any]
    }

    def toSeq(rowReader: RowReader): Seq[_] = keyFuncs.map(_(rowReader))

    toSeq
  }

  override def size(keys: Seq[_]): Int = atomTypes.zip(keys).map {
    case (t, k) => t.size(k.asInstanceOf[t.T])
  }.sum
}


/**
 * Typeclasses for single keys
 * NOTE: both the Ordering for ByteVector as well as how bytes are compared in most places is big-endian
 * TODO: make byte comparison unsigned
 */
object SingleKeyTypes {
  trait LongKeyTypeLike extends SingleKeyTypeBase[Long] {
    def toBytes(key: Long): ByteVector = ByteVector.fromLong(key, ordering = ByteOrdering.BigEndian)
    def fromBytes(bytes: ByteVector): Long = bytes.toLong(signed = true, ByteOrdering.BigEndian)

    def fromString(str: String): Long = str.toLong

    override def isSegmentType: Boolean = true
    override def size(key: Long): Int = 8
  }

  implicit case object LongKeyType extends LongKeyTypeLike

  trait IntKeyTypeLike extends SingleKeyTypeBase[Int] {
    def toBytes(key: Int): ByteVector = ByteVector.fromInt(key, ordering = ByteOrdering.BigEndian)
    def fromBytes(bytes: ByteVector): Int = bytes.toInt(signed = true, ByteOrdering.BigEndian)

    def fromString(str: String): Int = str.toInt

    override def isSegmentType: Boolean = true
    override def size(key: Int): Int = 4
  }

  implicit case object IntKeyType extends IntKeyTypeLike

  trait DoubleKeyTypeLike extends SingleKeyTypeBase[Double] {
    def toBytes(key: Double): ByteVector =
      ByteVector.fromLong(java.lang.Double.doubleToLongBits(key), ordering = ByteOrdering.BigEndian)

    def fromBytes(bytes: ByteVector): Double =
      java.lang.Double.longBitsToDouble(bytes.toLong(signed = true, ByteOrdering.BigEndian))

    def fromString(str: String): Double = str.toDouble

    override def size(key: Double): Int = 8
  }

  implicit case object DoubleKeyType extends DoubleKeyTypeLike

  trait StringKeyTypeLike extends SingleKeyTypeBase[String] {
    def toBytes(key: String): ByteVector = ByteVector(key.getBytes("UTF-8"))
    def fromBytes(bytes: ByteVector): String = new String(bytes.toArray, "UTF-8")
    def fromString(str: String): String = str
    override def isSegmentType: Boolean = true
    override def size(key: String): Int = key.getBytes("UTF-8").length
  }

  implicit case object StringKeyType extends StringKeyTypeLike

  implicit case object BooleanKeyType extends SingleKeyTypeBase[Boolean] {
    def toBytes(key: Boolean): ByteVector = ByteVector(if(key) -1.toByte else 0.toByte)
    def fromBytes(bytes: ByteVector): Boolean = bytes(0) != 0
    def fromString(str: String): Boolean = str.toBoolean
    def size(key: Boolean): Int = 1
  }

  implicit val timestampOrdering = Ordering.by((t: Timestamp) => t.getTime)

  implicit case object TimestampKeyType extends SingleKeyTypeBase[Timestamp] {
    def toBytes(key: Timestamp): ByteVector = LongKeyType.toBytes(key.getTime)
    def fromBytes(bytes: ByteVector): Timestamp = new Timestamp(LongKeyType.fromBytes(bytes))
    // Assume that most Timestamp string args are ISO8601-style date time strings.  Fallback to long ms.
    override def fromString(str: String): Timestamp = {
      try {
        new Timestamp(DateTime.parse(str).getMillis)
      } catch {
        case e: java.lang.IllegalArgumentException => new Timestamp(str.toLong)
      }
    }
    override def isSegmentType: Boolean = true
    def size(key: Timestamp): Int = 8
  }
}
