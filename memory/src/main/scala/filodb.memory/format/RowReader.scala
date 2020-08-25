package filodb.memory.format

import java.nio.ByteBuffer
import java.sql.Timestamp

import scala.reflect.ClassTag

import org.agrona.DirectBuffer
import org.agrona.concurrent.UnsafeBuffer
import org.joda.time.DateTime
import spire.syntax.cfor._

import filodb.memory.format.vectors.Histogram

/**
 * A generic trait for reading typed values out of a row of data.
 * Used for both reading out of Filo vectors as well as for RowToVectorBuilder,
 * which means it can be used to compose heterogeneous Filo vectors together.
 */
// scalastyle:off
trait RowReader {
  def notNull(columnNo: Int): Boolean
  def getBoolean(columnNo: Int): Boolean
  def getInt(columnNo: Int): Int
  def getLong(columnNo: Int): Long
  def getDouble(columnNo: Int): Double
  def getFloat(columnNo: Int): Float
  def getString(columnNo: Int): String
  def getAny(columnNo: Int): Any

  def getBlobBase(columnNo: Int): Any
  def getBlobOffset(columnNo: Int): Long
  def getBlobNumBytes(columnNo: Int): Int // Total number of bytes for the blob

  // By default this is not implemented as histograms can be parsed from multiple serialized forms or actual objects
  def getHistogram(columnNo: Int): Histogram = ???

  /**
   * Retrieves a view into the blob at column columnNo without duplicating contents.
   * Smart implementations could reuse the same UnsafeBuffer to avoid allocations.
   * This default implementation simply allocates a new one.
   */
  def blobAsBuffer(columnNo: Int): DirectBuffer = {
    val buf = new UnsafeBuffer(Array.empty[Byte])
    UnsafeUtils.wrapDirectBuf(getBlobBase(columnNo), getBlobOffset(columnNo), getBlobNumBytes(columnNo), buf)
    buf
  }

  final def getBuffer(columnNo: Int): ByteBuffer = {
    val length = getBlobNumBytes(columnNo)
    getBlobBase(columnNo) match {
      case UnsafeUtils.ZeroPointer =>   // offheap
        UnsafeUtils.asDirectBuffer(getBlobOffset(columnNo), length)
      case array: Array[Byte] =>
        ByteBuffer.wrap(array, (getBlobOffset(columnNo) - UnsafeUtils.arayOffset).toInt, length)
    }
  }

//  def getUtf8MediumOffset(columnNo: Int): Long

  // Please override final def if your RowReader has a faster implementation
  def filoUTF8String(columnNo: Int): ZeroCopyUTF8String = getAny(columnNo) match {
    case s: String =>
      Option(s).map(ZeroCopyUTF8String.apply).getOrElse(ZeroCopyUTF8String.empty)
    case z: ZeroCopyUTF8String => z
    case null => ZeroCopyUTF8String.NA
  }

  /**
   * This method serves two purposes.
   * For RowReaders that need to parse from some input source, such as CSV,
   * the ClassTag gives a way for per-type parsing for non-primitive types.
   * For RowReaders for fast reading paths, such as Spark, the default
   * implementation serves as a fast way to read from objects.
   */
  def as[T: ClassTag](columnNo: Int): T = getAny(columnNo).asInstanceOf[T]
}

import filodb.memory.format.RowReader._

// A RowReader that knows how to hashcode and compare its individual elements.  Extractors must
// correspond to the schema.   This could allow partition keys to be wrapped directly around raw ingest
// elements without converting to BinaryRecord first
trait SchemaRowReader extends RowReader {
  def extractors: Array[TypedFieldExtractor[_]]

  // NOTE: This is an EXTREMELY HOT code path, needs to be super optimized.  No standard Scala collection
  // or slow functional code here.
  override def hashCode: Int = {
    var hash = 0
    cforRange { 0 until extractors.size } { i =>
      hash ^= extractors(i).getField(this, i).hashCode
    }
    hash
  }

  override def equals(other: Any): Boolean = other match {
    case reader: RowReader =>
      cforRange { 0 until extractors.size } { i =>
        if (extractors(i).compare(this, reader, i) != 0) return false
      }
      true
    case other: Any =>
      false
  }
}

/**
 * An example of a RowReader that can read from Scala tuples containing Option[_]
 */
final case class TupleRowReader(tuple: Product) extends RowReader {
  def notNull(columnNo: Int): Boolean =
    tuple.productElement(columnNo).asInstanceOf[Option[Any]].nonEmpty

  def getBoolean(columnNo: Int): Boolean = tuple.productElement(columnNo) match {
    case Some(x: Boolean) => x
    case None => false
  }

  def getInt(columnNo: Int): Int = tuple.productElement(columnNo) match {
    case Some(x: Int) => x
    case None => 0
  }

  def getLong(columnNo: Int): Long = tuple.productElement(columnNo) match {
    case Some(x: Long) => x
    case Some(x: Timestamp) => x.getTime
    case None => 0L
  }

  def getDouble(columnNo: Int): Double = tuple.productElement(columnNo) match {
    case Some(x: Double) => x
    case None => 0.0
  }

  def getFloat(columnNo: Int): Float = tuple.productElement(columnNo) match {
    case Some(x: Float) => x
    case None => 0.0F
  }

  def getString(columnNo: Int): String = tuple.productElement(columnNo) match {
    case Some(x: String) => x
    case None => null
  }

  def getAny(columnNo: Int): Any =
    tuple.productElement(columnNo).asInstanceOf[Option[Any]].getOrElse(null)

  override def getBlobBase(columnNo: Int): Any = ???
  override def getBlobOffset(columnNo: Int): Long = ???
  override def getBlobNumBytes(columnNo: Int): Int = ???
}

/**
 * A RowReader for working with OpenCSV or anything else that emits string[]
 */
final case class ArrayStringRowReader(strings: Array[String]) extends RowReader {
  //scalastyle:off
  def notNull(columnNo: Int): Boolean = strings(columnNo) != null && strings(columnNo) != ""
  //scalastyle:on
  def getBoolean(columnNo: Int): Boolean = strings(columnNo).toBoolean
  def getInt(columnNo: Int): Int = strings(columnNo).toInt
  def getLong(columnNo: Int): Long = try {
    strings(columnNo).toLong
  } catch {
    case ex: NumberFormatException => DateTime.parse(strings(columnNo)).getMillis
  }
  def getDouble(columnNo: Int): Double = strings(columnNo).toDouble
  def getFloat(columnNo: Int): Float = strings(columnNo).toFloat
  def getString(columnNo: Int): String = strings(columnNo)
  def getAny(columnNo: Int): Any = strings(columnNo)

  override def as[T: ClassTag](columnNo: Int): T = {
    implicitly[ClassTag[T]].runtimeClass.asInstanceOf[T]
  }

  override def toString: String = s"ArrayStringRR(${strings.mkString(", ")})"

  override def getBlobBase(columnNo: Int): Any = ???
  override def getBlobOffset(columnNo: Int): Long = ???
  override def getBlobNumBytes(columnNo: Int): Int = ???
//  override def getUtf8MediumOffset(columnNo: Int): Long = ???
}
// scalastyle:off
/**
 * A RowReader that changes the column numbers around of an original RowReader.  It could be used to
 * present a subset of the original columns, for example.
 * @param columnRoutes an array of original column numbers for the column in question.  For example:
 *                     Array(0, 2, 5) means an getInt(1) would map to a getInt(2) for the original RowReader
 */
//noinspection ScalaStyle
trait RoutingReader extends RowReader {
  def origReader: RowReader
  def columnRoutes: Array[Int]

  final def notNull(columnNo: Int): Boolean    = origReader.notNull(columnRoutes(columnNo))
  final def getBoolean(columnNo: Int): Boolean = origReader.getBoolean(columnRoutes(columnNo))
  final def getInt(columnNo: Int): Int         = origReader.getInt(columnRoutes(columnNo))
  final def getLong(columnNo: Int): Long       = origReader.getLong(columnRoutes(columnNo))
  final def getDouble(columnNo: Int): Double   = origReader.getDouble(columnRoutes(columnNo))
  final def getFloat(columnNo: Int): Float     = origReader.getFloat(columnRoutes(columnNo))
  final def getString(columnNo: Int): String   = origReader.getString(columnRoutes(columnNo))
  final def getAny(columnNo: Int): Any         = origReader.getAny(columnRoutes(columnNo))
  final def getBlobBase(columnNo: Int): Any = ???
  final def getBlobOffset(columnNo: Int): Long = ???
  final def getBlobNumBytes(columnNo: Int): Int = ???

  override def equals(other: Any): Boolean = other match {
    case RoutingRowReader(orig, _) => orig.equals(origReader)
    case r: RowReader              => r.equals(origReader)
    case other: Any                => false
  }
}

final case class RoutingRowReader(origReader: RowReader, columnRoutes: Array[Int]) extends RoutingReader

// A RoutingRowReader which is also a SchemaRowReader
final case class SchemaRoutingRowReader(origReader: RowReader,
                                        columnRoutes: Array[Int],
                                        extractors: Array[TypedFieldExtractor[_]])
extends RoutingReader with SchemaRowReader {
  override def toString: String = s"SchemaRoutingRR($origReader, ${columnRoutes.toList})"
}

final case class SingleValueRowReader(value: Any) extends RowReader {
  def notNull(columnNo: Int): Boolean = Option(value).isDefined
  def getBoolean(columnNo: Int): Boolean = value.asInstanceOf[Boolean]
  def getInt(columnNo: Int): Int = value.asInstanceOf[Int]
  def getLong(columnNo: Int): Long = value.asInstanceOf[Long]
  def getDouble(columnNo: Int): Double = value.asInstanceOf[Double]
  def getFloat(columnNo: Int): Float = value.asInstanceOf[Float]
  def getString(columnNo: Int): String = value.asInstanceOf[String]
  override def getHistogram(columnNo: Int): Histogram = value.asInstanceOf[Histogram]
  def getAny(columnNo: Int): Any = value
  def getBlobBase(columnNo: Int): Any = value
  def getBlobOffset(columnNo: Int): Long = 0
  def getBlobNumBytes(columnNo: Int): Int = value.asInstanceOf[Array[Byte]].length
}

final case class SeqRowReader(sequence: Seq[Any]) extends RowReader {
  def notNull(columnNo: Int): Boolean = true
  def getBoolean(columnNo: Int): Boolean = sequence(columnNo).asInstanceOf[Boolean]
  def getInt(columnNo: Int): Int = sequence(columnNo).asInstanceOf[Int]
  def getLong(columnNo: Int): Long = sequence(columnNo).asInstanceOf[Long]
  def getDouble(columnNo: Int): Double = sequence(columnNo).asInstanceOf[Double]
  def getFloat(columnNo: Int): Float = sequence(columnNo).asInstanceOf[Float]
  def getString(columnNo: Int): String = sequence(columnNo).asInstanceOf[String]
  override def getHistogram(columnNo: Int): Histogram = sequence(columnNo).asInstanceOf[Histogram]
  def getAny(columnNo: Int): Any = sequence(columnNo)
  def getBlobBase(columnNo: Int): Any = ???
  def getBlobOffset(columnNo: Int): Long = ???
  def getBlobNumBytes(columnNo: Int): Int = ???
}

final case class UTF8MapIteratorRowReader(records: Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]]) extends Iterator[RowReader] {
  var currVal: Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = _

  private val rowReader = new RowReader {
    def notNull(columnNo: Int): Boolean = true
    def getBoolean(columnNo: Int): Boolean = ???
    def getInt(columnNo: Int): Int = ???
    def getLong(columnNo: Int): Long = ???
    def getDouble(columnNo: Int): Double = ???
    def getFloat(columnNo: Int): Float = ???
    def getString(columnNo: Int): String = currVal.toString
    def getAny(columnNo: Int): Any = currVal

    def getBlobBase(columnNo: Int): Any = ???
    def getBlobOffset(columnNo: Int): Long = ???
    def getBlobNumBytes(columnNo: Int): Int = ???
  }

  override def hasNext: Boolean = records.hasNext

  override def next(): RowReader = {
    currVal = records.next()
    rowReader
  }
}

final case class SchemaSeqRowReader(sequence: Seq[Any],
                                    extractors: Array[TypedFieldExtractor[_]]) extends SchemaRowReader {
  def notNull(columnNo: Int): Boolean = true
  def getBoolean(columnNo: Int): Boolean = sequence(columnNo).asInstanceOf[Boolean]
  def getInt(columnNo: Int): Int = sequence(columnNo).asInstanceOf[Int]
  def getLong(columnNo: Int): Long = sequence(columnNo).asInstanceOf[Long]
  def getDouble(columnNo: Int): Double = sequence(columnNo).asInstanceOf[Double]
  def getFloat(columnNo: Int): Float = sequence(columnNo).asInstanceOf[Float]
  def getString(columnNo: Int): String = sequence(columnNo).asInstanceOf[String]
  override def getHistogram(columnNo: Int): Histogram = sequence(columnNo).asInstanceOf[Histogram]
  def getAny(columnNo: Int): Any = sequence(columnNo)
  def getBlobBase(columnNo: Int): Any = sequence(columnNo).asInstanceOf[Array[Byte]]
  def getBlobOffset(columnNo: Int): Long = 0
  def getBlobNumBytes(columnNo: Int): Int = sequence(columnNo).asInstanceOf[Array[Byte]].length
}

object RowReader {
  import DefaultValues._

  // Type class for extracting a field of a specific type .. and comparing a field from two RowReaders
  trait TypedFieldExtractor[@specialized F] {
    def getField(reader: RowReader, columnNo: Int): F
    def getFieldOrDefault(reader: RowReader, columnNo: Int): F = getField(reader, columnNo)
    def compare(reader: RowReader, other: RowReader, columnNo: Int): Int
  }

  // A generic FieldExtractor for objects
  case class ObjectFieldExtractor[T: ClassTag](default: T) extends TypedFieldExtractor[T] {
    final def getField(reader: RowReader, columnNo: Int): T = reader.as[T](columnNo)
    final override def getFieldOrDefault(reader: RowReader, columnNo: Int): T =
      if (reader.notNull(columnNo)) getField(reader, columnNo) else default
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      if (getFieldOrDefault(reader, columnNo) == getFieldOrDefault(other, columnNo)) 0 else 1
  }

  class WrappedExtractor[@specialized T, F: TypedFieldExtractor](func: F => T)
    extends TypedFieldExtractor[T] {
    val orig = implicitly[TypedFieldExtractor[F]]
    def getField(reader: RowReader, columnNo: Int): T = func(orig.getField(reader, columnNo))
    def compare(reader: RowReader, other: RowReader, col: Int): Int = orig.compare(reader, other, col)
  }

  implicit object BooleanFieldExtractor extends TypedFieldExtractor[Boolean] {
    final def getField(reader: RowReader, columnNo: Int): Boolean = reader.getBoolean(columnNo)
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      java.lang.Boolean.compare(getFieldOrDefault(reader, columnNo), getFieldOrDefault(other, columnNo))
  }

  implicit object LongFieldExtractor extends TypedFieldExtractor[Long] {
    final def getField(reader: RowReader, columnNo: Int): Long = reader.getLong(columnNo)
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      java.lang.Long.compare(getFieldOrDefault(reader, columnNo), getFieldOrDefault(other, columnNo))
  }

  implicit object IntFieldExtractor extends TypedFieldExtractor[Int] {
    final def getField(reader: RowReader, columnNo: Int): Int = reader.getInt(columnNo)
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      java.lang.Integer.compare(getFieldOrDefault(reader, columnNo), getFieldOrDefault(other, columnNo))
  }

  implicit object DoubleFieldExtractor extends TypedFieldExtractor[Double] {
    final def getField(reader: RowReader, columnNo: Int): Double = reader.getDouble(columnNo)
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      java.lang.Double.compare(getFieldOrDefault(reader, columnNo), getFieldOrDefault(other, columnNo))
  }

  implicit object FloatFieldExtractor extends TypedFieldExtractor[Float] {
    final def getField(reader: RowReader, columnNo: Int): Float = reader.getFloat(columnNo)
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      java.lang.Float.compare(getFieldOrDefault(reader, columnNo), getFieldOrDefault(other, columnNo))
  }

  implicit object StringFieldExtractor extends TypedFieldExtractor[String] {
    final def getField(reader: RowReader, columnNo: Int): String = reader.getString(columnNo)
    override final def getFieldOrDefault(reader: RowReader, columnNo: Int): String = {
      val str = reader.getString(columnNo)
      if (str == null) DefaultString else str
    }
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      getFieldOrDefault(reader, columnNo).compareTo(getFieldOrDefault(other, columnNo))
  }

  implicit object UTF8StringFieldExtractor extends TypedFieldExtractor[ZeroCopyUTF8String] {
    final def getField(reader: RowReader, columnNo: Int): ZeroCopyUTF8String =
      reader.filoUTF8String(columnNo)
    // TODO: do UTF8 comparison so we can avoid having to deserialize
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      getFieldOrDefault(reader, columnNo).compareTo(getFieldOrDefault(other, columnNo))
  }

  implicit object DateTimeFieldExtractor extends TypedFieldExtractor[DateTime] {
    final def getField(reader: RowReader, columnNo: Int): DateTime = reader.as[DateTime](columnNo)
    override final def getFieldOrDefault(reader: RowReader, columnNo: Int): DateTime = {
      val dt = reader.as[DateTime](columnNo)
      if (dt == null) DefaultDateTime else dt
    }
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      getFieldOrDefault(reader, columnNo).compareTo(getFieldOrDefault(other, columnNo))
  }

  implicit object TimestampFieldExtractor extends TypedFieldExtractor[Timestamp] {
    final def getField(reader: RowReader, columnNo: Int): Timestamp = reader.as[Timestamp](columnNo)
    override final def getFieldOrDefault(reader: RowReader, columnNo: Int): Timestamp = {
      val ts = reader.as[Timestamp](columnNo)
      if (ts == null) DefaultTimestamp else ts
    }
    // TODO: compare the Long, instead of deserializing and comparing Timestamp object
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      getFieldOrDefault(reader, columnNo).compareTo(getFieldOrDefault(other, columnNo))
  }

  implicit object HistogramExtractor extends TypedFieldExtractor[Histogram] {
    final def getField(reader: RowReader, columnNo: Int): Histogram = reader.getHistogram(columnNo)
    final def compare(reader: RowReader, other: RowReader, columnNo: Int): Int =
      getFieldOrDefault(reader, columnNo).compare(getFieldOrDefault(other, columnNo))
  }
}
