package filodb.memory.format.vectors

import scalaxy.loops._

import filodb.memory.BinaryRegion
import filodb.memory.format._

/**
 * A trait to represent bucket-based histograms as well as derived statistics such as sums or rates of
 * increasing histograms.
 * The schema is based on Prometheus histograms.  Each bucket is designed to contain all observations less than
 * or equal to the bucketTop, thus it is cumulative with increasing bucket number.
 * Furthermore the LAST bucket should contain the total number of observations overall.
 */
trait Histogram {
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
   * Finds the first bucket number with a value greater than or equal to the given "rank"
   * @return the bucket number, or numBuckets if not found
   */
  final def firstBucketGTE(rank: Double): Int = {
    var bucketNo = 0
    while (bucketValue(bucketNo) < rank) bucketNo += 1
    bucketNo
  }

  /**
   * Calculates histogram quantile based on bucket values using Prometheus scheme (increasing/LE)
   */
  final def quantile(q: Double): Double = {
    val result = if (q < 0) Double.NegativeInfinity
    else if (q > 1) Double.PositiveInfinity
    else if (numBuckets < 2) Double.NaN
    else {
      // find rank for the quantile using total number of occurrences (which is the last bucket value)
      var rank = q * bucketValue(numBuckets - 1)
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
}

/**
 * A histogram class that can be used for aggregation and to represent intermediate values
 */
final case class MutableHistogram(buckets: HistogramBuckets, values: Array[Double]) extends Histogram {
  final def numBuckets: Int = buckets.numBuckets
  final def bucketTop(no: Int): Double = buckets.bucketTop(no)
  final def bucketValue(no: Int): Double = values(no)
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
}

object HistogramBuckets {
  val BucketTypeGeometric = 0x01.toByte
  val BucketTypeGeometric_1 = 0x02.toByte
  val BucketTypeExplicit  = 0x03.toByte

  val OffsetBucketType = 2
  val OffsetNumBuckets = 0
  val OffsetBucketDetails = 3

  /**
   * Creates the right HistogramBuckets from a binary definition.  NOTE: length field not included here
   * The first two bytes of any binary bucket schema definition are the number of buckets.
   * The third byte is the bucket scheme ID.
   */
  def apply(binaryBucketsDef: Array[Byte]): HistogramBuckets =
    apply(binaryBucketsDef, UnsafeUtils.arayOffset, binaryBucketsDef.size)

  def apply(bucketsDef: BinaryRegion.NativePointer, numBytes: Int): HistogramBuckets =
    apply(UnsafeUtils.ZeroPointer.asInstanceOf[Array[Byte]], bucketsDef, numBytes)

  def apply(bucketsDefBase: Array[Byte], bucketsDefOffset: Long, numBytes: Int): HistogramBuckets = {
    UnsafeUtils.getByte(bucketsDefBase, bucketsDefOffset + OffsetBucketType) match {
      case BucketTypeGeometric   =>
        GeometricBuckets(UnsafeUtils.getDouble(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails),
                         UnsafeUtils.getDouble(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails + 8),
                         UnsafeUtils.getShort(bucketsDefBase, bucketsDefOffset + OffsetNumBuckets).toInt)
      case BucketTypeGeometric_1 =>
        GeometricBuckets_1(UnsafeUtils.getDouble(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails),
                           UnsafeUtils.getDouble(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails + 8),
                           UnsafeUtils.getShort(bucketsDefBase, bucketsDefOffset + OffsetNumBuckets).toInt)
      case _ =>
        // Probably due to empty histogram vector, just return some default
        binaryBuckets64
    }
  }

  /**
   * Creates a binary bucket definition for a geometric series of histogram buckets.
   * That means each bucket contains values <= (firstBucket) * (multipler) ^ (bucketNo)
   * where bucketNo starts at 0 and goes till (numBuckets - 1)
   * @param firstBucket initial bucket value
   * @param multiplier the geometric multiplier between buckets
   * @param numBuckets the total number of buckets
   * @param minusOne if true, 1 is subtracted from each geometric bucket from the formula above
   */
  final def geometricBucketDef(firstBucket: Double,
                               multiplier: Double,
                               numBuckets: Int,
                               minusOne: Boolean = false): Array[Byte] = {
    require(numBuckets < 65536, s"Too many buckets: $numBuckets")
    val bytes = new Array[Byte](19)
    bytes(OffsetBucketType) = if (minusOne) BucketTypeGeometric_1 else BucketTypeGeometric
    UnsafeUtils.setShort(bytes, UnsafeUtils.arayOffset + OffsetNumBuckets, numBuckets.toShort)
    UnsafeUtils.setDouble(bytes, UnsafeUtils.arayOffset + OffsetBucketDetails, firstBucket)
    UnsafeUtils.setDouble(bytes, UnsafeUtils.arayOffset + OffsetBucketDetails + 8, multiplier)
    bytes
  }

  // A bucket definition for the bits of a long, ie from 2^0 to 2^63
  // le's = [1, 3, 7, 15, 31, ....]
  val binaryBuckets64 = GeometricBuckets_1(2.0d, 2.0d, 64)
  val binaryBuckets64Bytes = binaryBuckets64.toByteArray
}

/**
 * A geometric series bucketing scheme, where each successive bucket is a multiple of a previous one.
 */
final case class GeometricBuckets(firstBucket: Double, multiplier: Double, numBuckets: Int) extends HistogramBuckets {
  final def bucketTop(no: Int): Double = firstBucket * Math.pow(multiplier, no)
  def toByteArray: Array[Byte] = HistogramBuckets.geometricBucketDef(firstBucket, multiplier, numBuckets, false)
}

/**
 * A variation of a geometric series scheme where the bucket values are 1 less than the geoemtric series.
 */
final case class GeometricBuckets_1(firstBucket: Double, multiplier: Double, numBuckets: Int) extends HistogramBuckets {
  final def bucketTop(no: Int): Double = (firstBucket * Math.pow(multiplier, no)) - 1
  def toByteArray: Array[Byte] = HistogramBuckets.geometricBucketDef(firstBucket, multiplier, numBuckets, true)
}
