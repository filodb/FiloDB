package filodb.memory.format.vectors

import org.agrona.concurrent.UnsafeBuffer
import scalaxy.loops._

import filodb.memory.BinaryRegion
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
   * Returns an UnsafeBuffer pointing to a serialized BinaryHistogram representation of this histogram.
   * @param buf if Some(buf) supplied, then that buf is either written into or re-used to wrap where the serialized
   *            representation is.  The supplied buffer must be large enough to hold serialized histogram.
   *            if None is passed, then the thread-local buffer may be re-used, in which case be careful as that
   *            buffer will be mutated with the next call to serialize() within the same thread.
   */
  def serialize(intoBuf: Option[UnsafeBuffer] = None): UnsafeBuffer

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
    final def serialize(intoBuf: Option[UnsafeBuffer] = None): UnsafeBuffer = {
      val buf = intoBuf.getOrElse(BinaryHistogram.histBuf)
      BinaryHistogram.writeNonIncreasing(HistogramBuckets.emptyBuckets, Array[Long](), buf)
      buf
    }
  }
}

/**
 * A histogram class that can be used for aggregation and to represent intermediate values
 */
final case class MutableHistogram(buckets: HistogramBuckets, values: Array[Double]) extends Histogram {
  final def numBuckets: Int = buckets.numBuckets
  final def bucketTop(no: Int): Double = buckets.bucketTop(no)
  final def bucketValue(no: Int): Double = values(no)
  final def serialize(intoBuf: Option[UnsafeBuffer] = None): UnsafeBuffer = {
    val buf = intoBuf.getOrElse(BinaryHistogram.histBuf)
    BinaryHistogram.writeNonIncreasing(buckets, values.map(_.toLong), buf)
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
  final def add(other: MutableHistogram): Unit =
    if (buckets == other.buckets) {
      for { b <- 0 until numBuckets optimized } {
        values(b) += other.values(b)
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
   * Serializes this bucket scheme to a byte array
   */
  def toByteArray: Array[Byte]

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

  /**
   * Creates the right HistogramBuckets from a binary definition.  NOTE: length field not included here
   * The first two bytes of any binary bucket schema definition are the number of buckets.
   * The third byte is the bucket scheme ID.
   */
  def apply(binaryBucketsDef: Array[Byte]): HistogramBuckets =
    apply(binaryBucketsDef, UnsafeUtils.arayOffset, binaryBucketsDef.size)

  def apply(bucketsDef: BinaryRegion.NativePointer, numBytes: Int): HistogramBuckets =
    apply(UnsafeUtils.ZeroPointer.asInstanceOf[Array[Byte]], bucketsDef, numBytes)

  // Create geometric buckets definition
  def geometric(bucketsDefBase: Array[Byte], bucketsDefOffset: Long): HistogramBuckets =
    GeometricBuckets(UnsafeUtils.getDouble(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails),
                     UnsafeUtils.getDouble(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails + 8),
                     UnsafeUtils.getShort(bucketsDefBase, bucketsDefOffset + OffsetNumBuckets).toInt)

  def geometric_1(bucketsDefBase: Array[Byte], bucketsDefOffset: Long): HistogramBuckets =
    GeometricBuckets_1(UnsafeUtils.getDouble(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails),
                       UnsafeUtils.getDouble(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails + 8),
                       UnsafeUtils.getShort(bucketsDefBase, bucketsDefOffset + OffsetNumBuckets).toInt)

  /**
   * Creates a binary bucket definition for a geometric series of histogram buckets.
   * That means each bucket contains values <= (firstBucket) * (multipler) ^ (bucketNo)
   * where bucketNo starts at 0 and goes till (numBuckets - 1)
   * @param firstBucket initial bucket value
   * @param multiplier the geometric multiplier between buckets
   * @param numBuckets the total number of buckets
   */
  final def geometricBucketDef(firstBucket: Double,
                               multiplier: Double,
                               numBuckets: Int): Array[Byte] = {
    require(numBuckets < 65536, s"Too many buckets: $numBuckets")
    val bytes = new Array[Byte](18)
    UnsafeUtils.setShort(bytes, UnsafeUtils.arayOffset + OffsetNumBuckets, numBuckets.toShort)
    UnsafeUtils.setDouble(bytes, UnsafeUtils.arayOffset + OffsetBucketDetails, firstBucket)
    UnsafeUtils.setDouble(bytes, UnsafeUtils.arayOffset + OffsetBucketDetails + 8, multiplier)
    bytes
  }

  private val bucketBuf = new ThreadLocal[(HistogramBuckets, Array[Byte])]

  // Caches last binary bucket definition per thread and retrieves from cache for faster access
  def cachedBucketBytes(buckets: HistogramBuckets): Array[Byte] = bucketBuf.get match {
    case UnsafeUtils.ZeroPointer =>
      val bucketBytes = buckets.toByteArray
      bucketBuf.set((buckets, bucketBytes))
      bucketBytes
    case (bucketDef, bytes) if bucketDef != buckets =>
      val bucketBytes = buckets.toByteArray
      bucketBuf.set((buckets, bucketBytes))
      bucketBytes
    case (bucketDef, bytes) => bytes
  }

  // A bucket definition for the bits of a long, ie from 2^0 to 2^63
  // le's = [1, 3, 7, 15, 31, ....]
  val binaryBuckets64 = GeometricBuckets_1(2.0d, 2.0d, 64)
  val binaryBuckets64Bytes = binaryBuckets64.toByteArray

  val emptyBuckets = GeometricBuckets(2.0d, 2.0d, 0)
}

/**
 * A geometric series bucketing scheme, where each successive bucket is a multiple of a previous one.
 */
final case class GeometricBuckets(firstBucket: Double, multiplier: Double, numBuckets: Int) extends HistogramBuckets {
  final def bucketTop(no: Int): Double = firstBucket * Math.pow(multiplier, no)
  def toByteArray: Array[Byte] = HistogramBuckets.geometricBucketDef(firstBucket, multiplier, numBuckets)
}

/**
 * A variation of a geometric series scheme where the bucket values are 1 less than the geoemtric series.
 */
final case class GeometricBuckets_1(firstBucket: Double, multiplier: Double, numBuckets: Int) extends HistogramBuckets {
  final def bucketTop(no: Int): Double = (firstBucket * Math.pow(multiplier, no)) - 1
  def toByteArray: Array[Byte] = HistogramBuckets.geometricBucketDef(firstBucket, multiplier, numBuckets)
}
