package filodb.core.query

import org.velvia.filo.{FiloVector, BinaryVector}
import scala.language.postfixOps
import scala.reflect.ClassTag
import scalaxy.loops._

import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.RichProjection
import filodb.core.store.{ChunkScanMethod, RowKeyChunkScan}

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
abstract class TimeGroupingAggregate[@specialized(Int, Long, Double) T, R: ClassTag]
  (timeVectPos: Int, valueVectPos: Int, startTime: Long, endTime: Long, numBuckets: Int)
extends Aggregate[R] {
  require(numBuckets > 0, s"The number of buckets must be positive")
  private final val bucketWidth = (endTime - startTime) / numBuckets

  final def add(reader: ChunkSetReader): Aggregate[R] = {
    val numRows = reader.info.numRows
    reader.vectors(timeVectPos) match {
      case tv: BinaryVector[Long] @unchecked if !tv.maybeNAs =>
        reader.vectors(valueVectPos) match {
          case vv: BinaryVector[T] if !vv.maybeNAs => aggNoNAs(tv, vv, numRows)
          case v: FiloVector[T] =>                    aggWithNAs(tv, v, numRows)
        }
      case tv: FiloVector[Long] @unchecked =>
        aggWithNAs(tv, reader.vectors(valueVectPos).asInstanceOf[FiloVector[T]], numRows)
    }
    this
  }

  override def chunkScan(projection: RichProjection): Option[ChunkScanMethod] =
    Some(RowKeyChunkScan(BinaryRecord(projection, Seq(startTime)), BinaryRecord(projection, Seq(endTime))))

  // It is really important that this tight inner loop be in a separate method with the BinaryVector[T]
  // in the param signature.  This forces scalac to generate specialized methods so that the specialized
  // instead of boxed/j.l.Object method of aggregateOne is called.
  final def aggNoNAs(tv: BinaryVector[Long], vv: BinaryVector[T], numRows: Int): Unit =
    for { i <- 0 until numRows optimized } {
      val ts = tv(i)
      if (ts >= startTime && ts < endTime) aggregateOne(vv(i), ((ts - startTime) / bucketWidth).toInt)
    }

  final def aggWithNAs(tv: FiloVector[Long], vv: FiloVector[T], numRows: Int): Unit =
    for { i <- 0 until numRows optimized } {
      if (tv.isAvailable(i) && vv.isAvailable(i)) {
        val ts = tv(i)
        if (ts >= startTime && ts <= endTime) aggregateOne(vv(i), ((ts - startTime) / bucketWidth).toInt)
      }
    }

  def aggregateOne(value: T, bucketNo: Int): Unit
}

class TimeGroupingMinDoubleAgg(timeVectPos: Int,
                               valueVectPos: Int,
                               startTime: Long,
                               endTime: Long,
                               numBuckets: Int) extends
TimeGroupingAggregate[Double, Double](timeVectPos, valueVectPos, startTime, endTime, numBuckets) {
  private final val buckets = Array.fill(numBuckets)(Double.MaxValue)
  def result: Array[Double] = buckets

  final def aggregateOne(value: Double, bucketNo: Int): Unit =
    buckets(bucketNo) = Math.min(buckets(bucketNo), value)
}

class TimeGroupingMaxDoubleAgg(timeVectPos: Int,
                               valueVectPos: Int,
                               startTime: Long,
                               endTime: Long,
                               numBuckets: Int) extends
TimeGroupingAggregate[Double, Double](timeVectPos, valueVectPos, startTime, endTime, numBuckets) {
  private final val buckets = Array.fill(numBuckets)(Double.MinValue)
  def result: Array[Double] = buckets

  final def aggregateOne(value: Double, bucketNo: Int): Unit =
    buckets(bucketNo) = Math.max(buckets(bucketNo), value)
}

class TimeGroupingAvgDoubleAgg(timeVectPos: Int,
                               valueVectPos: Int,
                               startTime: Long,
                               endTime: Long,
                               numBuckets: Int) extends
TimeGroupingAggregate[Double, Double](timeVectPos, valueVectPos, startTime, endTime, numBuckets) {
  private final val sums = new Array[Double](numBuckets)
  private final val counts = new Array[Int](numBuckets)
  def result: Array[Double] = (0 until numBuckets).map(i => sums(i)/counts(i)).toArray

  final def aggregateOne(value: Double, bucketNo: Int): Unit = {
    sums(bucketNo) += value
    counts(bucketNo) += 1
  }
}
