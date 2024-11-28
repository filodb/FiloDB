package filodb.memory.format.vectors

import java.nio.ByteOrder.LITTLE_ENDIAN

import org.agrona.{DirectBuffer, MutableDirectBuffer}
import spire.syntax.cfor._

import filodb.memory.format._


/**
 * A trait to represent bucket-based histograms as well as derived statistics such as sums or rates of
 * increasing histograms.
 * The schema is based on Prometheus histograms.  Each bucket is designed to contain all observations less than
 * or equal to the bucketTop ("upper bound"), thus it is cumulative with increasing bucket number.
 * Furthermore the LAST bucket should contain the total number of observations overall.
 */
//scalastyle:off file.size.limit
trait Histogram extends Ordered[Histogram] {
  def numBuckets: Int

  /**
   * An empty histogram is one with no buckets
   */
  def isEmpty: Boolean = numBuckets == 0

  /**
   * Gets the bucket definition for number no.  Observations for values <= this bucket, so it represents
   * an upper limit.
   */
  def bucketTop(no: Int): Double

  /**
   * Gets the counter or number of observations for a particular bucket, or derived value of a bucket.
   */
  def bucketValue(no: Int): Double

  def hasExponentialBuckets: Boolean

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

  final def topBucketValue: Double =
    if (numBuckets <= 0) Double.NaN else bucketValue(numBuckets - 1)

  /**
   * Calculates histogram quantile based on bucket values using Prometheus scheme (increasing/LE)
   */
  def quantile(q: Double,
               min: Double = 0, // negative observations not supported yet
               max: Double = Double.PositiveInfinity): Double = {
    val result = if (q < 0) Double.NegativeInfinity
    else if (q > 1) Double.PositiveInfinity
    else if (numBuckets < 2 || topBucketValue <= 0) Double.NaN
    else {
      // find rank for the quantile using total number of occurrences (which is the last bucket value)
      var rank = q * topBucketValue
      // using rank, find the le bucket which would have the identified rank
      val bucket = firstBucketGTE(rank)

      // current bucket lower and upper bound; negative observations not supported yet - to be done later
      var bucketStart = if (bucket == 0) 0 else bucketTop(bucket-1)
      var bucketEnd = bucketTop(bucket)
      // if min and max are in this bucket, adjust the bucket start and end
      if (min > bucketStart && min <= bucketEnd) bucketStart = min
      if (max > bucketStart && max <= bucketEnd) bucketEnd = max

      // now calculate quantile.  If bucket is last one and last bucket is +Inf then return second-to-last bucket top
      // as we cannot interpolate to +Inf.
      if (bucket == numBuckets-1 && bucketTop(numBuckets - 1).isPosInfinity) {
        return bucketTop(numBuckets-2)
      } else if (bucket == 0 && bucketTop(0) <= 0) {
        return bucketTop(0) // zero or negative bucket
      } else {

        // interpolate quantile within boundaries of "bucket"
        val count = if (bucket == 0) bucketValue(bucket) else bucketValue(bucket) - bucketValue(bucket-1)
        rank -= (if (bucket == 0) 0 else bucketValue(bucket-1))
        val fraction = rank/count
        if (!hasExponentialBuckets || bucketStart == 0) {
          bucketStart + (bucketEnd-bucketStart) * fraction
        } else {
          val logBucketEnd = log2(bucketEnd)
          val logBucketStart = log2(bucketStart)
          val logRank = logBucketStart + (logBucketEnd - logBucketStart) * fraction
          Math.pow(2, logRank)
        }
      }
    }
    result
  }

  private def log2(v: Double) = Math.log(v) / Math.log(2)

  /**
   * Adapted from histogram_fraction in Prometheus codebase, but modified to handle
   * the fact that bucket values are cumulative. Also, if min and max are provided,
   * then interpolation accuracy is improved.
   */
  //scalastyle:off cyclomatic.complexity
  //scalastyle:off method.length
  def histogramFraction(lower: Double, upper: Double,
                        min: Double = Double.NegativeInfinity,
                        max: Double = Double.PositiveInfinity): Double = {
    require(lower >= 0 && upper >= 0, s"lower & upper params should be >= 0: lower=$lower, upper=$upper")
    if (numBuckets == 0 || lower.isNaN || upper.isNaN || topBucketValue == 0) {
      return Double.NaN
    }
    val count = topBucketValue

    if (lower == 0 && upper == 0) {
      return bucketValue(0) / count
    }
    if (lower >= upper) {
      return 0.0
    }

    var lowerRank = 0.0
    var upperRank = 0.0
    var lowerSet = false
    var upperSet = false
    val it = (0 until numBuckets).iterator

    while (it.hasNext && (!lowerSet || !upperSet)) {
      val b = it.next()
      val zeroBucket = (b == 0)
      val bucketUpper = bucketTop(b)
      val bucketLower = if (zeroBucket) 0.0 else bucketTop(b - 1)
      val bucketVal = bucketValue(b)
      val prevBucketVal = if (zeroBucket) 0.0 else bucketValue(b - 1)

      // Define interpolation functions
      def interpolateLinearly(v: Double): Double = {
        val low = Math.max(bucketLower, min)
        val high = Math.min(bucketUpper, max)
        val fraction = (v - low) / (high - low)
        prevBucketVal + (bucketVal - prevBucketVal) * fraction
      }

      def interpolateExponentially(v: Double) = {
        val low = Math.max(bucketLower, min)
        val high = Math.min(bucketUpper, max)
        val logLower = log2(Math.abs(low))
        val logUpper = log2(Math.abs(high))
        val logV = log2(Math.abs(v))
        val fraction = if (v > 0) (logV - logLower) / (logUpper - logLower)
                       else 1 - ((logV - logUpper) / (logLower - logUpper))
        prevBucketVal + (bucketVal - prevBucketVal) * fraction
      }

      if (!lowerSet && bucketLower == lower) {
        // We have hit the lower value at the lower bucket boundary.
        lowerRank = prevBucketVal
        lowerSet = true
      }
      if (!upperSet && bucketUpper == upper) {
        // We have hit the upper value at the lower bucket boundary.
        upperRank = bucketVal
        upperSet = true
      }

      if (!lowerSet && bucketLower < lower && bucketUpper > lower) {
        // The lower value is in this bucket
        lowerRank = if (!hasExponentialBuckets || zeroBucket) interpolateLinearly(lower)
                    else interpolateExponentially(lower)
        lowerSet = true
      }
      if (!upperSet && bucketLower < upper && bucketUpper > upper) {
        // The upper value is in this bucket
        upperRank = if (!hasExponentialBuckets || zeroBucket) interpolateLinearly(upper)
                    else interpolateExponentially(upper)
        upperSet = true
      }
    }

    if (!lowerSet || lowerRank > count) lowerRank = count
    if (!upperSet || upperRank > count) upperRank = count

    (upperRank - lowerRank) / count
  }

  /**
   * Compares two Histograms for equality.
   * If the # buckets or bucket bounds are not equal, just compare the top bucket value.
   * If they are equal, compare counts from top on down.
   */
  def compare(other: Histogram): Int = {
    if (numBuckets != other.numBuckets) return topBucketValue compare other.topBucketValue
    cforRange { 0 until numBuckets } { b =>
      if (bucketTop(b) != other.bucketTop(b)) return topBucketValue compare other.topBucketValue
    }
    cforRange { (numBuckets - 1) to 0 by -1 } {b =>
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
    cforRange { 0 until numBuckets } { b =>
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
    cforRange { 0 until numBuckets } { b =>
      values(b) = bucketValue(b)
    }
    values
  }
  def hasExponentialBuckets: Boolean = buckets.isInstanceOf[Base2ExpHistogramBuckets]
}

object HistogramWithBuckets {
  // Can be used for an initial "empty" or "null" Histogram.  No buckets - can't aggregate or do anything
  val empty = LongHistogram(HistogramBuckets.emptyBuckets, Array[Long]())
}

final case class LongHistogram(var buckets: HistogramBuckets, var values: Array[Long]) extends HistogramWithBuckets {
  final def bucketValue(no: Int): Double = values(no).toDouble
  final def serialize(intoBuf: Option[MutableDirectBuffer] = None): MutableDirectBuffer = {
    val buf = intoBuf.getOrElse(BinaryHistogram.histBuf)
    BinaryHistogram.writeDelta(buckets, values, buf)
    buf
  }

  /**
   * Adds the buckets from other into this LongHistogram
   */
  final def add(other: LongHistogram): Unit = {
    if (other.buckets != buckets) {
      throw new IllegalArgumentException(
           s"Cannot add histograms with different bucket configurations. " +
             s"Expected: ${buckets}, Found: ${other.buckets}"
      )
    }
    // TODO if otel histogram, the need to add values in a different way
    // see if we can refactor since MutableHistogram also has this logic
    cforRange { 0 until numBuckets } { b =>
      values(b) += other.values(b)
    }
  }

  final def populateFrom(other: LongHistogram): Unit = {
    require(other.buckets == buckets)
    System.arraycopy(other.values, 0, values, 0, values.size)
  }

  final def copy: LongHistogram = {
    val newHist = LongHistogram.empty(buckets)
    newHist.populateFrom(this)
    newHist
  }
}

object LongHistogram {
  def fromPacked(bucketDef: HistogramBuckets, packedValues: DirectBuffer): Option[LongHistogram] = {
    val values = new Array[Long](bucketDef.numBuckets)
    val res = NibblePack.unpackToSink(packedValues, NibblePack.DeltaSink(values), bucketDef.numBuckets)
    if (res == NibblePack.Ok) Some(LongHistogram(bucketDef, values)) else None
  }

  def empty(bucketDef: HistogramBuckets): LongHistogram =
    LongHistogram(bucketDef, new Array[Long](bucketDef.numBuckets))
}

/**
 * A histogram class that can be used for aggregation and to represent intermediate values
 */
final case class MutableHistogram(var buckets: HistogramBuckets,
                                  var values: Array[Double]) extends HistogramWithBuckets {
  require(buckets.numBuckets == values.size, s"Invalid number of values: ${values.size} != ${buckets.numBuckets}")

  final def bucketValue(no: Int): Double = values(no)
  final def serialize(intoBuf: Option[MutableDirectBuffer] = None): MutableDirectBuffer = {
    val buf = intoBuf.getOrElse(BinaryHistogram.histBuf)
    buckets match {
      case g: GeometricBuckets if g.minusOne => BinaryHistogram.writeDelta(g, values.map(_.toLong), buf)
      case g: Base2ExpHistogramBuckets => BinaryHistogram.writeDelta(g, values.map(_.toLong), buf)
      case _ => BinaryHistogram.writeDoubles(buckets, values, buf)
    }
    buf
  }

  /**
   * Copies this histogram as a new copy so it can be used for aggregation or mutation. Allocates new storage.
   */
  final def copy: MutableHistogram = MutableHistogram(buckets, values.clone)

  /**
   * Copies the values of this histogram from another histogram.  Other histogram must have same bucket scheme.
   */
  final def populateFrom(other: HistogramWithBuckets): Unit = {
    require(other.buckets == buckets)
    other match {
      case m: MutableHistogram =>
        System.arraycopy(m.values, 0, values, 0, values.size)
      case l: LongHistogram    =>
        cforRange { 0 until values.size } { n =>
          values(n) = l.values(n).toDouble
        }
    }
  }

  /**
   * Adds the values from another Histogram.
   * If the other histogram has the same bucket scheme, then the values are just added per bucket.
   * If the scheme is different, it assigns NaN to values.
   * @param other Histogram to be added
   * @return true when input histogram has same schema and false when schema is different
   */
  // scalastyle:off method.length
  final def addNoCorrection(other: HistogramWithBuckets): Boolean = {
    // Allow addition when type of bucket is different
    if (buckets.similarForMath(other.buckets)) {
      // If it was NaN before, reset to 0 to sum another hist
      if (java.lang.Double.isNaN(values(0))) java.util.Arrays.fill(values, 0.0)
      cforRange { 0 until numBuckets } { b =>
        values(b) += other.bucketValue(b)
      }
      true
    } else if (buckets.isInstanceOf[Base2ExpHistogramBuckets] &&
               other.buckets.isInstanceOf[Base2ExpHistogramBuckets]) {
      // If it was NaN before, reset to 0 to sum another hist
      if (java.lang.Double.isNaN(values(0))) java.util.Arrays.fill(values, 0.0)
      val ourBuckets = buckets.asInstanceOf[Base2ExpHistogramBuckets]
      val otherBuckets = other.buckets.asInstanceOf[Base2ExpHistogramBuckets]
      // if our buckets is subset of other buckets, then we can add the values
      if (ourBuckets.canAccommodate(otherBuckets)) {
        ourBuckets.addValues(values, otherBuckets, other)
        // since we are making the exp histogram buckets cumulative during
        // ingestion, we can assume cumulative bucket values
        false
      } else {
        val newBuckets = ourBuckets.add(otherBuckets) // create new buckets that can accommodate both
        val newValues = new Array[Double](newBuckets.numBuckets) // new values array
        newBuckets.addValues(newValues, ourBuckets, this)
        newBuckets.addValues(newValues, otherBuckets, other)
        buckets = newBuckets
        values = newValues
        // since we are making the exp histogram buckets cumulative during
        // ingestion, we can assume cumulative bucket values
        false
      }
    }
    else {
      cforRange { 0 until numBuckets } { b =>
        values(b) = Double.NaN
      }
      false
      // TODO: In the future, support adding buckets of different scheme.  Below is an example
      // NOTE: there are two issues here: below add picks the existing bucket scheme (not commutative)
      //       and the newer different buckets are lost (one may want more granularity)
      // var ourBucketNo = 0
      // cforRange { 0 until other.numBuckets } { b =>
      //   // Find our first bucket greater than or equal to their bucket
      //   while (ourBucketNo < numBuckets && bucketTop(ourBucketNo) < other.bucketTop(b)) ourBucketNo += 1
      //   if (ourBucketNo < numBuckets) {
      //     values(ourBucketNo) += other.bucketValue(b)
      //   }
      // }
    }
  }

  /**
   * Adds the values from another Histogram, making a monotonic correction to ensure correctness
   */
  final def add(other: HistogramWithBuckets): Unit = {
    // Call makeMonotonic only when buckets have same schema as result is NaN otherwise and monotonic correction
    // isn't needed
    if (addNoCorrection(other)) makeMonotonic()
  }

  /**
    * Fixes any issue with monotonicity of supplied bucket values.
    * Bucket values should monotonically increase. It may not be the case
    * if the bucket values are not atomically obtained from the same scrape,
    * or if bucket le values change over time (esp from aggregation) causing NaN on missing buckets.
    */
  final def makeMonotonic(): Unit = {
    var max = 0d
    cforRange { 0 until values.size } { b =>
      // When bucket no longer used NaN will be seen. Non-increasing values can be seen when
      // newer buckets are introduced and not all instances are updated with that bucket.
      if (values(b) < max || java.lang.Double.isNaN(values(b))) values(b) = max // assign previous max
      else if (values(b) > max) max = values(b) // update max
    }
  }
}

object MutableHistogram {
  def empty(buckets: HistogramBuckets): MutableHistogram =
    MutableHistogram(buckets, Array.fill(buckets.numBuckets)(Double.NaN))

  def fromPacked(bucketDef: HistogramBuckets, packedValues: DirectBuffer): Option[MutableHistogram] = {
    val values = new Array[Double](bucketDef.numBuckets)
    val res = NibblePack.unpackDoubleXOR(packedValues, values)
    if (res == NibblePack.Ok) Some(MutableHistogram(bucketDef, values)) else None
  }

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
    cforRange { 0 until numBuckets } { b =>
      tops(b) = bucketTop(b)
    }
    tops
  }

  final def bucketSet: debox.Set[Double] = {
    val tops = debox.Set.empty[Double]
    cforRange { 0 until numBuckets } { b =>
      tops += bucketTop(b)
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

  /**
    Returns true when buckets are similar for mathematical operations like addition
   */
  def similarForMath(other: HistogramBuckets): Boolean = this == other
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
    case HistFormat_OtelExp_Delta    => otelExp(buffer.byteArray, buffer.addressOffset + 2)
    case HistFormat_Custom_Delta     => custom(buffer.byteArray, buffer.addressOffset)
    case _                           => emptyBuckets
  }

  // NOTE: must point to u16/Short length prefix bytes
  def apply(acc: MemoryReader, bucketsDef: Ptr.U8, formatCode: Byte): HistogramBuckets = formatCode match {
    case HistFormat_Geometric_Delta  => geometric(acc.base, acc.baseOffset + bucketsDef.add(2).addr, false)
    case HistFormat_Geometric1_Delta => geometric(acc.base, acc.baseOffset + bucketsDef.add(2).addr, true)
    case HistFormat_OtelExp_Delta    => otelExp(acc.base, acc.baseOffset + bucketsDef.add(2).addr)
    case HistFormat_Custom_Delta     => custom(acc.base, acc.baseOffset + bucketsDef.addr)
    case _                           => emptyBuckets
  }

  // Create geometric buckets definition
  // FIXME Use MemoryAccessor methods here instead of unsafe access.
  def geometric(bucketsDefBase: Array[Byte], bucketsDefOffset: Long, minusOne: Boolean): HistogramBuckets =
    GeometricBuckets(UnsafeUtils.getDouble(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails),
                     UnsafeUtils.getDouble(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails + 8),
                     UnsafeUtils.getShort(bucketsDefBase, bucketsDefOffset + OffsetNumBuckets).toInt,
                     minusOne)

  def otelExp(bucketsDefBase: Array[Byte], bucketsDefOffset: Long): HistogramBuckets = {
    val scale = UnsafeUtils.getShort(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails)
    val startPosBucket = UnsafeUtils.getShort(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails + 2)
    val numPosBuckets = UnsafeUtils.getShort(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails + 4)
    // uncomment when negative buckets are supported
    // val startNegBucket = UnsafeUtils.getShort(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails + 6)
    // val numNegBuckets = UnsafeUtils.getShort(bucketsDefBase, bucketsDefOffset + OffsetBucketDetails + 8)
    Base2ExpHistogramBuckets(scale, startPosBucket, numPosBuckets)
  }

  /**
   * Creates a CustomBuckets definition.
   * @param bucketsDefOffset must point to the 2-byte length prefix of the bucket definition
   */
  def custom(bucketsDefBase: Any, bucketsDefOffset: Long): CustomBuckets = {
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

  override def similarForMath(other: HistogramBuckets): Boolean = {
    other match {
      case c: CustomBuckets => c.allBucketTops.sameElements(other.allBucketTops)
      case _                => this == other
    }
  }
}

object Base2ExpHistogramBuckets {
  // TODO: make maxBuckets default configurable; not straightforward to get handle to global config from here
  // see PR for benchmark test results based on which maxBuckets was fixed. Dont increase without analysis.
  val maxBuckets = 180
  val maxAbsScale = 100
  val maxAbsBucketIndex = 500
}

/**
 * Open Telemetry Base2 Exponential Histogram Bucket Scheme.
 * See https://github.com/open-telemetry/opentelemetry-proto/blob/7312bdf63218acf27fe96430b7231de37fd091f2/
 *             opentelemetry/proto/metrics/v1/metrics.proto#L500
 * for the definition.
 *
 * Easier explanation here: https://dyladan.me/histograms/2023/05/04/exponential-histograms/
 *
 * The bucket scheme is based on the formula:
 * `bucket(index) = base ^ (index + 1)`
 * where index is the bucket index, and `base = 2 ^ 2 ^ -scale`
 *
 * Negative observations are not supported yet. But serialization format is forward compatible.
 *
 * @param scale OTel metrics ExponentialHistogramDataPoint proto scale value
 * @param startIndexPositiveBuckets offset for positive buckets from the same proto
 * @param numPositiveBuckets length of the positive buckets array from the same proto
 */
final case class Base2ExpHistogramBuckets(scale: Int,
                                          startIndexPositiveBuckets: Int,
                                          numPositiveBuckets: Int
                                        ) extends HistogramBuckets {
  import Base2ExpHistogramBuckets._
  require(numPositiveBuckets <= maxBuckets && numPositiveBuckets >= 0,
    s"Invalid buckets: numPositiveBuckets=$numPositiveBuckets  maxBuckets=${maxBuckets}")
  // limit bucket index and scale values since the corresponding bucketTop values can get very large or very small and
  // may tend to very very large or small values above double precision. Note we are also serializing
  // these as shorts in the binary format, so they need to be within limits of short
  require(startIndexPositiveBuckets > -maxAbsBucketIndex && startIndexPositiveBuckets < maxAbsBucketIndex,
          s"Invalid startIndexPositiveBuckets $startIndexPositiveBuckets should be between " +
            s"${-maxAbsBucketIndex} and ${maxAbsBucketIndex}")
  require(scale > -maxAbsScale && scale < maxAbsScale,
    s"Invalid scale $scale should be between ${-maxAbsScale} and ${maxAbsScale}")

  val base: Double = Math.pow(2, Math.pow(2, -scale))
  val startBucketTop: Double = bucketTop(1)
  val endBucketTop: Double = bucketTop(numBuckets - 1)

  override def numBuckets: Int = numPositiveBuckets + 1 // add one for zero count

  final def bucketTop(no: Int): Double = {
    if (no ==0) 0.0 else {
      // From OTel metrics proto docs:
      // The histogram bucket identified by `index`, a signed integer,
      // contains values that are greater than (base^index) and
      // less than or equal to (base^(index+1)).
      val index = startIndexPositiveBuckets + no - 1
      Math.pow(base, index + 1)
    }
  }

  final def serialize(buf: MutableDirectBuffer, pos: Int): Int = {

    // Negative observations are not supported yet, so startIndexNegativeBuckets and numNegativeBuckets are always 0.
    // They are here since the proto has it (but the client SDKs do not), and we want to be able to support it
    // when the spec changes
    val startIndexNegativeBuckets = 0
    val numNegativeBuckets = 0
    require(numBuckets < Short.MaxValue, s"numBucket overflow: $numBuckets")
    require(startIndexPositiveBuckets < Short.MaxValue,
      s"startIndexPositiveBuckets overflow: $startIndexPositiveBuckets")
    require(scale < Short.MaxValue, s"scale overflow: $scale")
    require(numPositiveBuckets < Short.MaxValue, s"numPositiveBuckets overflow $numPositiveBuckets")
    // per BinHistogram format, bucket def len comes first always
    buf.putShort(pos, (2 + 2 + 4 + 4).toShort)
    // numBuckets comes next always
    val numBucketsPos = pos + 2
    buf.putShort(numBucketsPos, numBuckets.toShort, LITTLE_ENDIAN)
    // now bucket format specific data
    val bucketSchemeFieldsPos = pos + 4
    buf.putShort(bucketSchemeFieldsPos, scale.toShort, LITTLE_ENDIAN)
    buf.putShort(bucketSchemeFieldsPos + 2, startIndexPositiveBuckets.toShort, LITTLE_ENDIAN)
    buf.putShort(bucketSchemeFieldsPos + 4, numPositiveBuckets.toShort, LITTLE_ENDIAN)
    buf.putShort(bucketSchemeFieldsPos + 6, startIndexNegativeBuckets.toShort, LITTLE_ENDIAN)
    buf.putShort(bucketSchemeFieldsPos + 8, numNegativeBuckets.toShort, LITTLE_ENDIAN)
    pos + 14
  }

  override def toString: String = {
    s"OTelExpHistogramBuckets(scale=$scale, startIndexPositiveBuckets=$startIndexPositiveBuckets, " +
      s"numPositiveBuckets=$numPositiveBuckets) ${super.toString}"
  }
  override def similarForMath(other: HistogramBuckets): Boolean = {
    other match {
      case c: Base2ExpHistogramBuckets => this.scale == c.scale &&
                                          this.startIndexPositiveBuckets == c.startIndexPositiveBuckets &&
                                          this.numPositiveBuckets == c.numPositiveBuckets
      case _                           => false
    }
  }

  def canAccommodate(other: Base2ExpHistogramBuckets): Boolean = {
    // Can we do better? There can be double's == problems here
    endBucketTop >= other.endBucketTop && startBucketTop <= other.startBucketTop
  }

  def add(o: Base2ExpHistogramBuckets,
          maxBuckets: Int = Base2ExpHistogramBuckets.maxBuckets): Base2ExpHistogramBuckets = {
    if (canAccommodate(o)) {
      this
    } else {
      // calculate new bucket scheme that can accommodate both bucket ranges
      val minBucketTopNeeded = Math.min(startBucketTop, o.startBucketTop)
      val maxBucketTopNeeded = Math.max(endBucketTop, o.endBucketTop)
      var newScale = Math.min(scale, o.scale)
      var newBase = Math.max(base, o.base)
      // minus one below since there is  "+1" in `bucket(index) = base ^ (index + 1)`
      var newBucketIndexEnd = Math.ceil(Math.log(maxBucketTopNeeded) / Math.log(newBase)).toInt - 1 // exclusive
      var newBucketIndexStart = Math.floor(Math.log(minBucketTopNeeded) / Math.log(newBase)).toInt - 1 // inclusive
      // Even if the two schemes are of same scale, they can have non-overlapping bucket ranges.
      // The new bucket scheme should have at most maxBuckets, so keep reducing scale until within limits.
      while (newBucketIndexEnd - newBucketIndexStart + 1 > maxBuckets) {
        newScale -= 1
        newBase = Math.pow(2, Math.pow(2, -newScale))
        newBucketIndexEnd = Math.ceil(Math.log(maxBucketTopNeeded) / Math.log(newBase)).toInt - 1
        newBucketIndexStart = Math.floor(Math.log(minBucketTopNeeded) / Math.log(newBase)).toInt - 1
      }
      Base2ExpHistogramBuckets(newScale, newBucketIndexStart, newBucketIndexEnd - newBucketIndexStart + 1)
    }
  }

  /**
   * Converts an OTel exponential index to array index (aka bucket no).
   * For example if startIndexPositiveBuckets = -5 and numPositiveBuckets = 10, then
   * -5 will return 1, -4 will return 2, 0 will return 6, 4 will return 10.
   * Know that 0 array index is reserved for zero bucket.
   */
  def bucketIndexToArrayIndex(index: Int): Int = index - startIndexPositiveBuckets + 1

  /**
   * Add the otherValues using otherBuckets scheme to ourValues. This method assumes (a) ourValues uses "this"
   * bucket scheme and (a) this bucket scheme can accommodate otherBuckets.
   */
  def addValues(ourValues: Array[Double],
                otherBuckets: Base2ExpHistogramBuckets,
                otherHistogram: HistogramWithBuckets): Unit = {
    // TODO consider removing the require once code is stable
    require(ourValues.length == numBuckets)
    require(otherHistogram.numBuckets == otherBuckets.numBuckets)
    require(canAccommodate(otherBuckets))
    val scaleIncrease = otherBuckets.scale - scale
    // for each ourValues, find the bucket index in otherValues and add the value
    val fac = Math.pow(2 , scaleIncrease).toInt
    ourValues(0) += otherHistogram.bucketValue(0) // add the zero bucket
    // For this algorithm we take advantage of the pattern that there is a relationship between scale
    // and the bucketIndexPlus1. When scale increases by scaleIncrease,
    // then the otherBucketIndexPlus1 for the same bucket in the other scheme
    // is 2^scaleIncrease (which is fac) times otherBucketIndexPlus1.
    cforRange { startIndexPositiveBuckets until startIndexPositiveBuckets + numPositiveBuckets } { ourBucketIndex =>
      val ourBucketIndexPlus1 = ourBucketIndex + 1
      val otherBucketIndexPlus1 = ourBucketIndexPlus1 * fac
      val ourArrayIndex = bucketIndexToArrayIndex(ourBucketIndexPlus1 - 1)
      // ourArrayIndex is guaranteed to be within bounds since we are iterating our bucket range

      val otherArrayIndex = otherBuckets.bucketIndexToArrayIndex(otherBucketIndexPlus1 - 1)
      // there is a possibility that otherArrayIndex is out of bounds, so we need to check

      if (otherArrayIndex > 0 && otherArrayIndex < otherBuckets.numBuckets) { // add if within bounds

        // Example 1: add every 2nd bucket from other to our bucket
        // other scale = 2 . . . . . . . . . . .
        //                 ↓   ↓   ↓   ↓   ↓   ↓
        // our  scale =  1 .   .   .   .   .   .
        //
        // Example 2: add every 4th bucket from other to our bucket
        // other scale = 3 . . . . . . . . . . . . .
        //                 ↓       ↓       ↓       ↓
        // our  scale =  1 .       .       .       .

        // TODO consider removing the require once code is stable
        require(ourArrayIndex != 0, "double counting zero bucket")
        require( Math.abs(bucketTop(ourArrayIndex) - otherBuckets.bucketTop(otherArrayIndex)) <= 0.000001,
          s"Bucket tops of $ourArrayIndex and $otherArrayIndex don't match:" +
            s" ${bucketTop(ourArrayIndex)} != ${otherBuckets.bucketTop(otherArrayIndex)}")
        ourValues(ourArrayIndex) += otherHistogram.bucketValue(otherArrayIndex)
      } else if (otherArrayIndex >= otherBuckets.numBuckets) {
        // if otherArrayIndex higher than upper bound, add to our last bucket since bucket counts are cumulative

        // Example 3: add every 4th bucket from other to our
        // other scale = 4 . . . . . . . . . . . . X    add last bucket X here
        //                 ↓       ↓       ↓       ↓       ↓       ↓
        // our  scale =  2 .       .       .       .       .       .

        // TODO consider removing the require once code is stable
        require(ourArrayIndex != 0, "double counting zero bucket")
        ourValues(ourArrayIndex) += otherHistogram.bucketValue(otherBuckets.numBuckets - 1)
      }
      // if otherArrayIndex is less than lower bound, the counts are already included and we don't need
      // to add since the counts are cumulative
    }
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

  override def similarForMath(other: HistogramBuckets): Boolean = {
    other match {
      case g: GeometricBuckets  => g.allBucketTops.sameElements(other.allBucketTops)
      case _                    => this == other
    }
  }
}
