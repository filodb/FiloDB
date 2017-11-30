package filodb.core.query

import scala.language.postfixOps
import scala.reflect.ClassTag

import scalaxy.loops._

import filodb.memory.format.{BinaryVector, FiloVector}

/**
 * A time-series filtering + aggregation function - agg(x) group by time WHERE time > start AND time < end
 * Basically dividing a time window into N even buckets or with M time delta per bucket, and performing
 * aggregations in each bucket
 * It uses two vectors - one must be a Long vector containing timestamps since UNIX Epoch, and the other
 * is the vector to aggregate.  The timestamps will be used to filter out values not in the time window.
 *
 * @param timeVectPos the position or index of the timestamp column within the projection/ChunkSetReader
 * @param valueVectPos the position or index of the value column to aggregate
 * @param startTime the minimum, inclusive starting timestamp of the window
 * @param endTime   the exclusive ending timestamp... meaning timestamps must be smaller than this
 * @param numBuckets the number of buckets to divide the time window into
 *
 * ## Performance Optimizations
 * TODO: If the vector is sorted, then the bounds checking / filtering can be simplified and sped up
 * TODO: Take advantage of DeltaDelta vectors and estimate filtering
 * TODO: It's faster to run multiple aggregations together
 */
abstract class TimeGroupingBaseAggregator[T]
  (timeVectPos: Int, valueVectPos: Int, startTime: Long, endTime: Long, numBuckets: Int)
extends ChunkAggregator {
  require(numBuckets > 0, s"The number of buckets must be positive")
  protected final val bucketWidth = (endTime - startTime) / numBuckets

  val positions = Array(timeVectPos, valueVectPos)

  final def add(orig: A, reader: ChunkSetReader): A = {
    val numRows = reader.info.numRows
    reader.vectors(0) match {
      case tv: BinaryVector[Long] @unchecked if !tv.maybeNAs =>
        reader.vectors(1) match {
          case vv: BinaryVector[T] if !vv.maybeNAs => aggNoNAs(orig, tv, vv, numRows)
          case v: FiloVector[T] =>                    aggWithNAs(orig, tv, v, numRows)
        }
      case tv: FiloVector[Long] @unchecked =>
        aggWithNAs(orig, tv, reader.vectors(1).asInstanceOf[FiloVector[T]], numRows)
    }
    orig
  }

  def aggNoNAs(agg: A, tv: BinaryVector[Long], vv: BinaryVector[T], numRows: Int): Unit
  def aggWithNAs(agg: A, tv: FiloVector[Long], vv: FiloVector[T], numRows: Int): Unit
}

abstract class TimeGroupingAggregator[@specialized(Int, Long, Double) T: ClassTag]
  (timeVectPos: Int, valueVectPos: Int, startTime: Long, endTime: Long, numBuckets: Int)
extends TimeGroupingBaseAggregator[T](timeVectPos, valueVectPos, startTime, endTime, numBuckets) {
  type A = ArrayAggregate[T]

  // It is really important that this tight inner loop be in a separate method with the BinaryVector[T]
  // in the param signature.  This forces scalac to generate specialized methods so that the specialized
  // instead of boxed/j.l.Object method of aggregateOne is called.
  final def aggNoNAs(agg: ArrayAggregate[T], tv: BinaryVector[Long], vv: BinaryVector[T], numRows: Int): Unit =
    for { i <- 0 until numRows optimized } {
      val ts = tv(i)
      if (ts >= startTime && ts < endTime) {
        aggregateOne(agg.result, vv(i), ((ts - startTime) / bucketWidth).toInt)
      }
    }

  final def aggWithNAs(agg: ArrayAggregate[T], tv: FiloVector[Long], vv: FiloVector[T], numRows: Int): Unit =
    for { i <- 0 until numRows optimized } {
      if (tv.isAvailable(i) && vv.isAvailable(i)) {
        val ts = tv(i)
        if (ts >= startTime && ts <= endTime) {
          aggregateOne(agg.result, vv(i), ((ts - startTime) / bucketWidth).toInt)
        }
      }
    }

  def aggregateOne(buckets: Array[T], value: T, bucketNo: Int): Unit
}

class TimeGroupingMinDoubleAgg(timeVectPos: Int,
                               valueVectPos: Int,
                               startTime: Long,
                               endTime: Long,
                               numBuckets: Int) extends
TimeGroupingAggregator[Double](timeVectPos, valueVectPos, startTime, endTime, numBuckets) {
  def emptyAggregate: A = new ArrayAggregate(numBuckets, Double.MaxValue)

  final def aggregateOne(buckets: Array[Double], value: Double, bucketNo: Int): Unit =
    buckets(bucketNo) = Math.min(buckets(bucketNo), value)

  final def combine(first: ArrayAggregate[Double], second: ArrayAggregate[Double]): ArrayAggregate[Double] = {
    for { i <- 0 until numBuckets optimized } { first.result(i) = Math.min(first.result(i), second.result(i)) }
    first
  }
}

class TimeGroupingMaxDoubleAgg(timeVectPos: Int,
                               valueVectPos: Int,
                               startTime: Long,
                               endTime: Long,
                               numBuckets: Int) extends
TimeGroupingAggregator[Double](timeVectPos, valueVectPos, startTime, endTime, numBuckets) {
  def emptyAggregate: A = new ArrayAggregate(numBuckets, Double.MinValue)

  final def aggregateOne(buckets: Array[Double], value: Double, bucketNo: Int): Unit =
    buckets(bucketNo) = Math.max(buckets(bucketNo), value)

  final def combine(first: ArrayAggregate[Double], second: ArrayAggregate[Double]): ArrayAggregate[Double] = {
    for { i <- 0 until numBuckets optimized } { first.result(i) = Math.max(first.result(i), second.result(i)) }
    first
  }
}

class AverageDoubleArrayAgg(size: Int) extends Aggregate[Double] {
  val sums = new Array[Double](size)
  val counts = new Array[Int](size)
  def result: Array[Double] = (0 until size).map(i => sums(i)/counts(i)).toArray

  def merge(other: AverageDoubleArrayAgg): Unit = {
    for { i <- 0 until size optimized } {
      sums(i) += other.sums(i)
      counts(i) += other.counts(i)
    }
  }
}

class TimeGroupingAvgDoubleAgg(timeVectPos: Int,
                               valueVectPos: Int,
                               startTime: Long,
                               endTime: Long,
                               numBuckets: Int) extends
TimeGroupingBaseAggregator[Double](timeVectPos, valueVectPos, startTime, endTime, numBuckets) {
  type A = AverageDoubleArrayAgg
  def emptyAggregate: A = new AverageDoubleArrayAgg(numBuckets)

  final def aggNoNAs(agg: AverageDoubleArrayAgg,
                     tv: BinaryVector[Long],
                     vv: BinaryVector[Double],
                     numRows: Int): Unit = {
    val sums = agg.sums
    val counts = agg.counts
    for { i <- 0 until numRows optimized } {
      val ts = tv(i)
      if (ts >= startTime && ts < endTime) {
        val bucket = ((ts - startTime) / bucketWidth).toInt
        sums(bucket) += vv(i)
        counts(bucket) += 1
      }
    }
  }

  final def aggWithNAs(agg: AverageDoubleArrayAgg,
                       tv: FiloVector[Long],
                       vv: FiloVector[Double],
                       numRows: Int): Unit = {
    val sums = agg.sums
    val counts = agg.counts
    for { i <- 0 until numRows optimized } {
      if (tv.isAvailable(i) && vv.isAvailable(i)) {
        val ts = tv(i)
        if (ts >= startTime && ts <= endTime) {
          val bucket = ((ts - startTime) / bucketWidth).toInt
          sums(bucket) += vv(i)
          counts(bucket) += 1
        }
      }
    }
  }

  final def combine(first: AverageDoubleArrayAgg, second: AverageDoubleArrayAgg): AverageDoubleArrayAgg = {
    first.merge(second)
    first
  }
}
