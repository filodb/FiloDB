package filodb.core

import java.sql.Timestamp
import org.joda.time.DateTime
import org.velvia.filo.RowReader
import org.velvia.filo.RowReader._
import scala.language.postfixOps
import scala.math.Ordering
import scalaxy.loops._
import scodec.bits.{ByteOrdering, ByteVector}

import filodb.core.util.StrideSerialiser

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

}

import SingleKeyTypes._

case class NullKeyValue(colIndex: Int) extends Exception(s"Null key value for col index $colIndex")

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

  // Default implementation assumes we take size bytes and calls the fromBytes method
  def parseBytes(remainder: ByteVector): (T, ByteVector) = {
    val chunk = remainder.take(size)
    (fromBytes(chunk), remainder.drop(size))
  }

  def size: Int

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
      atomType.toBytes(key(i).asInstanceOf[atomType.T])
    }.reduce[ByteVector] { case (a, b) => a ++ b }
  }

  def fromBytes(bytes: ByteVector): Seq[_] = {
    var currentBytes = bytes
    atomTypes.map { atomType =>
      val (atom, nextBytes) = atomType.parseBytes(currentBytes)
      currentBytes = nextBytes
      atom
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
}


/**
 * Typeclasses for single keys
 * NOTE: both the Ordering for ByteVector as well as how bytes are compared in most places is big-endian
 * TODO: make byte comparison unsigned
 */
object SingleKeyTypes {
  val Int32HighBit = 0x80000000
  val Long64HighBit = (1L << 63)

  trait LongKeyTypeLike extends SingleKeyTypeBase[Long] {
    def toBytes(key: Long): ByteVector =
      ByteVector.fromLong(key ^ Long64HighBit, ordering = ByteOrdering.BigEndian)
    def fromBytes(bytes: ByteVector): Long =
      bytes.toLong(signed = true, ByteOrdering.BigEndian) ^ Long64HighBit

    def fromString(str: String): Long = str.toLong

    override def isSegmentType: Boolean = true
    val size = 8
  }

  implicit case object LongKeyType extends LongKeyTypeLike

  trait IntKeyTypeLike extends SingleKeyTypeBase[Int] {
    def toBytes(key: Int): ByteVector =
      ByteVector.fromInt(key ^ Int32HighBit, ordering = ByteOrdering.BigEndian)
    def fromBytes(bytes: ByteVector): Int =
      bytes.toInt(signed = true, ByteOrdering.BigEndian) ^ Int32HighBit

    def fromString(str: String): Int = str.toInt

    override def isSegmentType: Boolean = true
    val size = 4
  }

  implicit case object IntKeyType extends IntKeyTypeLike

  trait DoubleKeyTypeLike extends SingleKeyTypeBase[Double] {
    def toBytes(key: Double): ByteVector =
      ByteVector.fromLong(java.lang.Double.doubleToLongBits(key), ordering = ByteOrdering.BigEndian)

    def fromBytes(bytes: ByteVector): Double =
      java.lang.Double.longBitsToDouble(bytes.toLong(signed = true, ByteOrdering.BigEndian))

    def fromString(str: String): Double = str.toDouble
    override def isSegmentType: Boolean = true
    val size = 8
  }

  implicit case object DoubleKeyType extends DoubleKeyTypeLike

  val stringSerde = StrideSerialiser.instance

  trait StringKeyTypeLike extends SingleKeyTypeBase[String] {
    def toBytes(key: String): ByteVector = ByteVector(stringSerde.toBytes(key))
    override def parseBytes(remainder: ByteVector): (String, ByteVector) = {
      val inputBuf = remainder.toByteBuffer
      val inputStr = stringSerde.fromBytes(inputBuf)
      (inputStr, remainder.drop(inputBuf.position))
    }
    def fromBytes(bytes: ByteVector): String = ""
    def fromString(str: String): String = str
    override def isSegmentType: Boolean = true
    val size = 0
  }

  implicit case object StringKeyType extends StringKeyTypeLike

  implicit case object BooleanKeyType extends SingleKeyTypeBase[Boolean] {
    def toBytes(key: Boolean): ByteVector = ByteVector(if(key) -1.toByte else 0.toByte)
    def fromBytes(bytes: ByteVector): Boolean = bytes(0) != 0
    def fromString(str: String): Boolean = str.toBoolean
    val size = 1
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
    val size = 8
  }
}
