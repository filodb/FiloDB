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
  val empty = MutableHistogram(HistogramBuckets.emptyBuckets, Array.empty)
}

trait HistogramWithBuckets extends Histogram {
  def buckets: HistogramBuckets
  final def numBuckets: Int = buckets.numBuckets
  final def bucketTop(no: Int): Double = buckets.bucketTop(no)
  final def valueArray: Array[Double] = {
    val values = new Array[Double](numBuckets)
    for { b <- 0 until numBuckets optimized } {
      values(b) = bucketValue(b)
    }
    values
  }
}

final case class LongHistogram(buckets: HistogramBuckets, values: Array[Long]) extends HistogramWithBuckets {
  final def bucketValue(no: Int): Double = values(no).toDouble
  final def serialize(intoBuf: Option[MutableDirectBuffer] = None): MutableDirectBuffer = {
    val buf = intoBuf.getOrElse(BinaryHistogram.histBuf)
    BinaryHistogram.writeDelta(buckets, values, buf)
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
    BinaryHistogram.writeDelta(buckets, values.map(_.toLong), buf)
    buf
  }

  /**
   * Copies this histogram as a new copy so it can be used for aggregation or mutation. Allocates new storage.
   */
  final def copy: MutableHistogram = MutableHistogram(buckets, values.clone)

  /**
   * Adds the values from another Histogram.
   * If the other histogram has the same bucket scheme, then the values are just added per bucket.
   * If the scheme is different, then an approximation is used so that the resulting histogram has
   * an approximate sum of the individual distributions, with the original scheme.  Modifies itself.
   */
  final def addNoCorrection(other: HistogramWithBuckets): Unit =
    if (buckets == other.buckets) {
      // If it was NaN before, reset to 0 to sum another hist
      if (values(0).isNaN) java.util.Arrays.fill(values, 0.0)
      for { b <- 0 until numBuckets optimized } {
        values(b) += other.bucketValue(b)
      }
    } else {
      throw new UnsupportedOperationException(s"Cannot add histogram of scheme ${other.buckets} to $buckets")
      // TODO: In the future, support adding buckets of different scheme.  Below is an example
      // NOTE: there are two issues here: below add picks the existing bucket scheme (not commutative)
      //       and the newer different buckets are lost (one may want more granularity)
      // var ourBucketNo = 0
      // for { b <- 0 until other.numBuckets optimized } {
      //   // Find our first bucket greater than or equal to their bucket
      //   while (ourBucketNo < numBuckets && bucketTop(ourBucketNo) < other.bucketTop(b)) ourBucketNo += 1
      //   if (ourBucketNo < numBuckets) {
      //     values(ourBucketNo) += other.bucketValue(b)
      //   }
      // }
    }

  /**
   * Adds the values from another Histogram, making a monotonic correction to ensure correctness
   */
  final def add(other: HistogramWithBuckets): Unit = {
    addNoCorrection(other)
    makeMonotonic()
  }

  /**
    * Fixes any issue with monotonicity of supplied bucket values.
    * Bucket values should monotonically increase. It may not be the case
    * if the bucket values are not atomically obtained from the same scrape,
    * or if bucket le values change over time (esp from aggregation) causing NaN on missing buckets.
    */
  final def makeMonotonic(): Unit = {
    var max = 0d
    for { b <- 0 until values.size optimized } {
      // When bucket no longer used NaN will be seen. Non-increasing values can be seen when
      // newer buckets are introduced and not all instances are updated with that bucket.
      if (values(b) < max || values(b).isNaN) values(b) = max // assign previous max
      else if (values(b) > max) max = values(b) // update max
    }
  }
}

object MutableHistogram {
  def empty(buckets: HistogramBuckets): MutableHistogram =
    MutableHistogram(buckets, Array.fill(buckets.numBuckets)(Double.NaN))

  def apply(h: Histogram): MutableHistogram = h match {
    case hb: HistogramWithBuckets => MutableHistogram(hb.buckets, hb.valueArray)
    case other: Histogram         => ???
  }
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

  /**
   * Serializes this bucket definition to a mutable buffer, including writing length bytes
   * @param buf the buffer to write to
   * @param pos the position within buffer to write to
   * @return the final position
   */
  def serialize(buf: MutableDirectBuffer, pos: Int): Int
}

object HistogramBuckets {
  val OffsetNumBuckets = 0
  val OffsetBucketDetails = 2

  val MAX_BUCKETS = 8192

  import BinaryHistogram._

  /**
   * Creates the right HistogramBuckets from a binary definition.  NOTE: length field not included here
   * The first two bytes of any binary bucket schema definition are the number of buckets.
   * @param buffer a DirectBuffer with index 0 pointing to the u16/Short length bytes
   */
  def apply(buffer: DirectBuffer, formatCode: Byte): HistogramBuckets = formatCode match {
    case HistFormat_Geometric_Delta  => geometric(buffer.byteArray, buffer.addressOffset + 2, false)
    case HistFormat_Geometric1_Delta => geometric(buffer.byteArray, buffer.addressOffset + 2, true)
    case HistFormat_Custom_Delta     => custom(buffer.byteArray, buffer.addressOffset)
    case _                           => emptyBuckets
  }

  // NOTE: must point to u16/Short length prefix bytes
  def apply(bucketsDef: Ptr.U8, formatCode: Byte): HistogramBuckets = formatCode match {
    case HistFormat_Geometric_Delta  => geometric(UnsafeUtils.ZeroArray, bucketsDef.add(2).addr, false)
    case HistFormat_Geometric1_Delta => geometric(UnsafeUtils.ZeroArray, bucketsDef.add(2).addr, true)
    case HistFormat_Custom_Delta     => custom(UnsafeUtils.ZeroArray, bucketsDef.addr)
    case _                           => emptyBuckets
  }

  // Create geometric buckets definition
  def geometric(bucketsDefBase: Array[Byte], bucketsDefOffset: Long, minusOne: Boolean): HistogramBuckets =
    GeometricBuckets(UnsafeUtils.getDouble(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails),
                     UnsafeUtils.getDouble(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails + 8),
                     UnsafeUtils.getShort(bucketsDefBase, bucketsDefOffset + OffsetNumBuckets).toInt,
                     minusOne)

  /**
   * Creates a CustomBuckets definition.
   * @param bucketsDefOffset must point to the 2-byte length prefix of the bucket definition
   */
  def custom(bucketsDefBase: Array[Byte], bucketsDefOffset: Long): CustomBuckets = {
    val numBuckets = UnsafeUtils.getShort(bucketsDefBase, bucketsDefOffset + 2) & 0x0ffff
    val les = new Array[Double](numBuckets)
    UnsafeUtils.wrapDirectBuf(bucketsDefBase,
                              bucketsDefOffset + 4,
                              (UnsafeUtils.getShort(bucketsDefBase, bucketsDefOffset) & 0xffff) - 2,
                              valuesBuf)
    require(NibblePack.unpackDoubleXOR(valuesBuf, les) == NibblePack.Ok)
    CustomBuckets(les)
  }

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

  final def serialize(buf: MutableDirectBuffer, pos: Int): Int = {
    require(numBuckets < 65536, s"Too many buckets: $numBuckets")
    val numBucketsPos = pos + 2
    buf.putShort(pos, (2 + 8 + 8).toShort)
    buf.putShort(numBucketsPos, numBuckets.toShort, LITTLE_ENDIAN)
    buf.putDouble(numBucketsPos + OffsetBucketDetails, firstBucket, LITTLE_ENDIAN)
    buf.putDouble(numBucketsPos + OffsetBucketDetails + 8, multiplier, LITTLE_ENDIAN)
    pos + 2 + 2 + 8 + 8
  }
}

/**
 * A bucketing scheme with custom bucket/LE values.
 *
 * Binary/serialized: short:numBytes, short:numBuckets, then NibblePacked XOR-compressed bucket/LE defs
 */
final case class CustomBuckets(les: Array[Double]) extends HistogramBuckets {
  require(les.size < HistogramBuckets.MAX_BUCKETS)
  def numBuckets: Int = les.size
  def bucketTop(no: Int): Double = les(no)
  final def serialize(buf: MutableDirectBuffer, pos: Int): Int = {
    buf.putShort(pos + 2, les.size.toShort)
    val finalPos = NibblePack.packDoubles(les, buf, pos + 4)
    require((finalPos - pos) <= 65535, s"Packing of ${les.size} buckets takes too much space!")
    buf.putShort(pos, (finalPos - pos - 2).toShort)
    finalPos
  }

  override def equals(other: Any): Boolean = other match {
    case CustomBuckets(otherLes) => les.toSeq == otherLes.toSeq
    case other: Any              => false
  }

  override def hashCode: Int = les.hashCode
}
