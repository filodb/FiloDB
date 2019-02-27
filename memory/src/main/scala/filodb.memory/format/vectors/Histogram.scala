package filodb.memory.format.vectors

import java.nio.ByteOrder.LITTLE_ENDIAN

import org.agrona.{DirectBuffer, MutableDirectBuffer}
import scalaxy.loops._

import filodb.memory.format._

/**
 * A trait to represent bucket-based histograms as well as derived statistics such as sums or rates of
 * increasing histograms.
 * The schema is based on Prometheus histograms.  Each bucket is designed to contain all observations less than
 * or equal to the bucketTop ("upper bound"), thus it is cumulative with increasing bucket number.
 * Furthermore the LAST bucket should contain the total number of observations overall.
 */
trait Histogram extends Ordered[Histogram] {
  def numBuckets: Int

  /**
   * Gets the bucket definition for number no.  Observations for values <= this bucket, so it represents
   * an upper limit.
   */
  def bucketTop(no: Int): Double

  /**
   * Gets the counter or number of observations for a particular bucket, or derived value of a bucket.
   */
  def bucketValue(no: Int): Double

  /**
   * Returns an MutableDirectBuffer pointing to a serialized BinaryHistogram representation of this histogram.
   * @param buf if Some(buf) supplied, then that buf is either written into or re-used to wrap where the serialized
   *            representation is.  The supplied buffer must be large enough to hold serialized histogram.
   *            if None is passed, then the thread-local buffer may be re-used, in which case be careful as that
   *            buffer will be mutated with the next call to serialize() within the same thread.
   */
  def serialize(intoBuf: Option[MutableDirectBuffer] = None): MutableDirectBuffer

  /**
   * Finds the first bucket number with a value greater than or equal to the given "rank"
   * @return the bucket number, or numBuckets if not found
   */
  final def firstBucketGTE(rank: Double): Int = {
    var bucketNo = 0
    while (bucketValue(bucketNo) < rank) bucketNo += 1
    bucketNo
  }

  final def topBucketValue: Double = bucketValue(numBuckets - 1)

  /**
   * Calculates histogram quantile based on bucket values using Prometheus scheme (increasing/LE)
   * TODO: monotonicity check, which will be in a separate function.
   */
  final def quantile(q: Double): Double = {
    val result = if (q < 0) Double.NegativeInfinity
    else if (q > 1) Double.PositiveInfinity
    else if (numBuckets < 2) Double.NaN
    else {
      // find rank for the quantile using total number of occurrences (which is the last bucket value)
      var rank = q * topBucketValue
      // using rank, find the le bucket which would have the identified rank
      val b = firstBucketGTE(rank)

      // now calculate quantile
      // If the rank is at the top return the last bucket (though shouldn't we interpolate here too?)
      if (b == numBuckets-1) return bucketTop(numBuckets-2)
      else if (b == 0 && bucketTop(0) <= 0) return bucketTop(0)
      else {
        // interpolate quantile within le bucket
        var (bucketStart, bucketEnd, count) = (0d, bucketTop(b), bucketValue(b))
        if (b > 0) {
          bucketStart = bucketTop(b-1)
          count -= bucketValue(b-1)
          rank -= bucketValue(b-1)
        }
        bucketStart + (bucketEnd-bucketStart)*(rank/count)
      }
    }
    result
  }

  /**
   * Compares two Histograms for equality.
   * If the # buckets or bucket bounds are not equal, just compare the top bucket value.
   * If they are equal, compare counts from top on down.
   */
  def compare(other: Histogram): Int = {
    if (numBuckets != other.numBuckets) return topBucketValue compare other.topBucketValue
    for { b <- 0 until numBuckets optimized } {
      if (bucketTop(b) != other.bucketTop(b)) return topBucketValue compare other.topBucketValue
    }
    for { b <- (numBuckets - 1) to 0 by -1 optimized } {
      val countComp = bucketValue(b) compare other.bucketValue(b)
      if (countComp != 0) return countComp
    }
    return 0
  }

  override def equals(other: Any): Boolean = other match {
    case h: Histogram => compare(h) == 0
    case other: Any   => false
  }

  override def toString: String =
    (0 until numBuckets).map { b => s"${bucketTop(b)}=${bucketValue(b)}" }.mkString("{", ", ", "}")

  override def hashCode: Int = {
    var hash = 7.0
    for { b <- 0 until numBuckets optimized } {
      hash = (31 * bucketTop(b) + hash) * 31 + bucketValue(b)
    }
    java.lang.Double.doubleToLongBits(hash).toInt
  }
}

object Histogram {
  val empty = new Histogram {
    final def numBuckets: Int = 0
    final def bucketTop(no: Int): Double = ???
    final def bucketValue(no: Int): Double = ???
    final def serialize(intoBuf: Option[MutableDirectBuffer] = None): MutableDirectBuffer = {
      val buf = intoBuf.getOrElse(BinaryHistogram.histBuf)
      BinaryHistogram.writeNonIncreasing(HistogramBuckets.emptyBuckets, Array[Long](), buf)
      buf
    }
  }
}

trait HistogramWithBuckets extends Histogram {
  def buckets: HistogramBuckets
  final def numBuckets: Int = buckets.numBuckets
  final def bucketTop(no: Int): Double = buckets.bucketTop(no)
}

final case class LongHistogram(buckets: HistogramBuckets, values: Array[Long]) extends HistogramWithBuckets {
  final def bucketValue(no: Int): Double = values(no).toDouble
  final def serialize(intoBuf: Option[MutableDirectBuffer] = None): MutableDirectBuffer = {
    val buf = intoBuf.getOrElse(BinaryHistogram.histBuf)
    buckets match {
      case g: GeometricBuckets => BinaryHistogram.writeDelta(g, values, buf)
    }
    buf
  }
}

object LongHistogram {
  def fromPacked(bucketDef: HistogramBuckets, packedValues: DirectBuffer): Option[LongHistogram] = {
    val values = new Array[Long](bucketDef.numBuckets)
    val res = NibblePack.unpackToSink(packedValues, NibblePack.DeltaSink(values), bucketDef.numBuckets)
    if (res == NibblePack.Ok) Some(LongHistogram(bucketDef, values)) else None
  }
}

/**
 * A histogram class that can be used for aggregation and to represent intermediate values
 */
final case class MutableHistogram(buckets: HistogramBuckets, values: Array[Double]) extends HistogramWithBuckets {
  final def bucketValue(no: Int): Double = values(no)
  final def serialize(intoBuf: Option[MutableDirectBuffer] = None): MutableDirectBuffer = {
    val buf = intoBuf.getOrElse(BinaryHistogram.histBuf)
    buckets match {
      case g: GeometricBuckets => BinaryHistogram.writeDelta(g, values.map(_.toLong), buf)
    }
    buf
  }

  /**
   * Copies this histogram as a new copy so it can be used for aggregation or mutation. Allocates new storage.
   */
  final def copy: Histogram = MutableHistogram(buckets, values.clone)

  /**
   * Adds the values from another MutableHistogram having the same bucket schema.  If it does not, then
   * an exception is thrown -- for now.  Modifies itself.
   */
  final def add(other: HistogramWithBuckets): Unit =
    if (buckets == other.buckets) {
      for { b <- 0 until numBuckets optimized } {
        values(b) += other.bucketValue(b)
      }
    } else {
      throw new UnsupportedOperationException(s"Cannot add other with buckets ${other.buckets} to myself $buckets")
    }
}

object MutableHistogram {
  def empty(buckets: HistogramBuckets): MutableHistogram =
    MutableHistogram(buckets, new Array[Double](buckets.numBuckets))
}

/**
 * A scheme for buckets in a histogram.  Since these are Prometheus-style histograms,
 * each bucket definition consists of occurrences of numbers which are less than or equal to the bucketTop
 * or definition of each bucket.
 */
sealed trait HistogramBuckets {
  def numBuckets: Int

  /**
   * Gets the bucket definition for number no.  Observations for values <= this bucket, so it represents
   * an upper limit.
   */
  def bucketTop(no: Int): Double

  /**
   * Materializes all bucket tops into an array.  WARNING: Allocation.
   */
  final def allBucketTops: Array[Double] = {
    val tops = new Array[Double](numBuckets)
    for { b <- 0 until numBuckets optimized } {
      tops(b) = bucketTop(b)
    }
    tops
  }

  override def toString: String = allBucketTops.mkString("buckets[", ", ", "]")
}

object HistogramBuckets {
  val OffsetNumBuckets = 0
  val OffsetBucketDetails = 2

  import BinaryHistogram._

  /**
   * Creates the right HistogramBuckets from a binary definition.  NOTE: length field not included here
   * The first two bytes of any binary bucket schema definition are the number of buckets.
   * @param buffer a DirectBuffer with index 0 pointing to the u16/Short length bytes
   */
  def apply(buffer: DirectBuffer, formatCode: Byte): HistogramBuckets = formatCode match {
    case HistFormat_Geometric_Delta  => geometric(buffer.byteArray, buffer.addressOffset + 2, false)
    case HistFormat_Geometric1_Delta => geometric(buffer.byteArray, buffer.addressOffset + 2, true)
    case _                           => emptyBuckets
  }

  // NOTE: must point to u16/Short length prefix bytes
  def apply(bucketsDef: Ptr.U8, formatCode: Byte): HistogramBuckets = formatCode match {
    case HistFormat_Geometric_Delta  => geometric(UnsafeUtils.ZeroArray, bucketsDef.add(2).addr, false)
    case HistFormat_Geometric1_Delta => geometric(UnsafeUtils.ZeroArray, bucketsDef.add(2).addr, true)
    case _                           => emptyBuckets
  }

  // Create geometric buckets definition
  def geometric(bucketsDefBase: Array[Byte], bucketsDefOffset: Long, minusOne: Boolean): HistogramBuckets =
    GeometricBuckets(UnsafeUtils.getDouble(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails),
                     UnsafeUtils.getDouble(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails + 8),
                     UnsafeUtils.getShort(bucketsDefBase, bucketsDefOffset + OffsetNumBuckets).toInt,
                     minusOne)

  // A bucket definition for the bits of a long, ie from 2^0 to 2^63
  // le's = [1, 3, 7, 15, 31, ....]
  val binaryBuckets64 = GeometricBuckets(2.0d, 2.0d, 64, minusOne = true)

  val emptyBuckets = GeometricBuckets(2.0d, 2.0d, 0)
}

/**
 * A geometric series bucketing scheme, where each successive bucket is a multiple of a previous one.
 * That means each bucket contains values <= (firstBucket) * (multipler) ^ (bucketNo)
 * where bucketNo starts at 0 and goes till (numBuckets - 1)
 * @param minusOne if true, subtract 1 from the bucket top.  Used for exclusive "not including"
 */
final case class GeometricBuckets(firstBucket: Double,
                                  multiplier: Double,
                                  numBuckets: Int,
                                  minusOne: Boolean = false) extends HistogramBuckets {
  private val adjustment = if (minusOne) -1 else 0
  final def bucketTop(no: Int): Double = (firstBucket * Math.pow(multiplier, no)) + adjustment

  import HistogramBuckets._
  /**
   * Serializes this bucket definition to a mutable buffer, including writing length bytes
   * @param buf the buffer to write to
   * @param pos the position within buffer to write to
   * @return the final position
   */
  final def serialize(buf: MutableDirectBuffer, pos: Int): Int = {
    require(numBuckets < 65536, s"Too many buckets: $numBuckets")
    val numBucketsPos = pos + 2
    buf.putShort(pos, 2 + 8 + 8)
    buf.putShort(numBucketsPos, numBuckets.toShort, LITTLE_ENDIAN)
    buf.putDouble(numBucketsPos + OffsetBucketDetails, firstBucket, LITTLE_ENDIAN)
    buf.putDouble(numBucketsPos + OffsetBucketDetails + 8, multiplier, LITTLE_ENDIAN)
    pos + 2 + 2 + 8 + 8
  }
}
