package filodb.core

import java.sql.Timestamp

import scala.math.Ordering

import org.joda.time.DateTime
import spire.syntax.cfor._

import filodb.memory.format.{vectors => bv, RowReader, ZeroCopyUTF8String}
import filodb.memory.format.RowReader._

/**
 * A generic typeclass for dealing with keys (partition, sort, segment) of various types.
 * Including comparison, serialization, extraction from rows, extraction from strings.
 * In general, every column type has a KeyType, since any column could be used for any keys,
 * except for segment keys which have some special requirements.
 */
trait KeyType {
  type T

  def ordering: Ordering[T] // must be comparable

  def rowReaderOrdering: Ordering[RowReader]

  def fromString(str: String): T

  /**
   * Extracts the type T from a RowReader.
   * @param columnNumbers an array of column numbers to extract from.  Sorry, must be an array for speed.
   * @throws NullKeyValue exception if nulls are found.  Nulls cannot be injected into
   *         keys.
   */
  def getKeyFunc(columnNumbers: Array[Int]): RowReader => T

}

case class NullKeyValue(colIndex: Int) extends Exception(s"Null key value for col index $colIndex")

abstract class SingleKeyTypeBase[K : Ordering : TypedFieldExtractor] extends KeyType {
  type T = K

  def getKeyFunc(columnNumbers: Array[Int]): RowReader => T = {
    require(columnNumbers.size == 1)
    val columnNum = columnNumbers(0)
    (r: RowReader) => {
      extractor.getFieldOrDefault(r, columnNum)
    }
  }

  val ordering: Ordering[T] = implicitly[Ordering[K]]
  val rowReaderOrdering = new Ordering[RowReader] {
    def compare(a: RowReader, b: RowReader): Int = extractor.compare(a, b, 0)
  }

  def extractor: TypedFieldExtractor[T] = implicitly[TypedFieldExtractor[K]]
}

case class CompositeOrdering(atomTypes: Seq[SingleKeyType]) extends Ordering[Seq[_]] {
  override def compare(x: Seq[_], y: Seq[_]): Int = {
    if (x.length == y.length && x.length == atomTypes.length) {
      cforRange { 0 until x.length } { i =>
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

case class CompositeReaderOrdering(atomTypes: Seq[SingleKeyType]) extends Ordering[RowReader] {
  private final val extractors = atomTypes.map(_.extractor).toArray
  private final val numAtoms = atomTypes.length
  def compare(a: RowReader, b: RowReader): Int = {
    cforRange { 0 until numAtoms } { i =>
      val res = extractors(i).compare(a, b, i)
      if (res != 0) return res
    }
    return 0
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

  val ordering = CompositeOrdering(atomTypes)
  val rowReaderOrdering: Ordering[RowReader] = CompositeReaderOrdering(atomTypes)

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
    def fromString(str: String): Long = str.toLong
  }

  implicit case object LongKeyType extends LongKeyTypeLike

  trait IntKeyTypeLike extends SingleKeyTypeBase[Int] {
    def fromString(str: String): Int = str.toInt
  }

  implicit case object IntKeyType extends IntKeyTypeLike

  trait DoubleKeyTypeLike extends SingleKeyTypeBase[Double] {
    def fromString(str: String): Double = str.toDouble
  }

  implicit case object DoubleKeyType extends DoubleKeyTypeLike

  trait StringKeyTypeLike extends SingleKeyTypeBase[ZeroCopyUTF8String] {
    def fromString(str: String): ZeroCopyUTF8String = ZeroCopyUTF8String(str)
  }

  implicit case object StringKeyType extends StringKeyTypeLike

  implicit case object BooleanKeyType extends SingleKeyTypeBase[Boolean] {
    def fromString(str: String): Boolean = str.toBoolean
  }

  import Types._
  implicit val utf8MapExtractor = ObjectFieldExtractor(emptyUTF8Map)
  // Kind of bogus ordering - but is there really a way of ordering maps?
  implicit val utf8MapOrdering = Ordering.by((m: UTF8Map) => m.size)

  implicit case object UTF8MapKeyType extends SingleKeyTypeBase[UTF8Map] {
    def fromString(str: String): UTF8Map = ???
  }

  implicit val timestampOrdering = Ordering.by((t: Timestamp) => t.getTime)

  implicit case object TimestampKeyType extends SingleKeyTypeBase[Timestamp] {
    // Assume that most Timestamp string args are ISO8601-style date time strings.  Fallback to long ms.
    override def fromString(str: String): Timestamp = {
      try {
        new Timestamp(DateTime.parse(str).getMillis)
      } catch {
        case e: java.lang.IllegalArgumentException => new Timestamp(str.toLong)
      }
    }
  }

  // Order histograms by top bucket value
  implicit val histOrdering = Ordering.by((h: bv.Histogram) => h.bucketValue(h.numBuckets - 1))
  implicit case object HistogramKeyType extends SingleKeyTypeBase[bv.Histogram] {
    def fromString(str: String): bv.Histogram = ???
  }
}
