package filodb.query.exec

import monix.reactive.Observable

import filodb.core.query._
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.query.QueryConfig

object HistogramQuantileMapper {
  import ZeroCopyUTF8String._
  val le = "le".utf8
}

/**
  * Calculates histogram le for one or more histograms whose bucket range vectors are passed
  * into the apply function.
  *
  * @param funcParams
  */
case class HistogramQuantileMapper(funcParams: Seq[Any]) extends RangeVectorTransformer {

  import HistogramQuantileMapper._
  require(funcParams.size == 1,
    "histogram_quantile function needs a single le argument")

  private val quantile = funcParams.head.asInstanceOf[Number].doubleValue()

  /**
    * Represents a prometheus histogram bucket for quantile calculation purposes.
    * @param le the less-than boundary for histogram bucket
    * @param count number of occurrences
    */
  case class Bucket(val le: Double, var count: Double)

  /**
    * Groups incoming range vectors by histogram, calculates le for each histogram
    * using the buckets supplied for it.
    *
    * Important note: The source range vectors for each bucket should NOT be the counter values,
    * but should be the rate of increase. The histogram_quantile function should always
    * be preceded by a rate function or a sum-of-rate function.
    */
  override def apply(source: Observable[RangeVector],
                     queryConfig: QueryConfig, limit: Int,
                     sourceSchema: ResultSchema): Observable[RangeVector] = {
    val res = source.toListL.map { rvs =>
      val histograms = groupRangeVectorsByHistogram(rvs)
      val quantileResults = histograms.map { histBuckets =>
        val sortedBucketRvs = histBuckets._2.toArray.map { bucket =>
          val leStr = bucket.key.labelValues(le).toString
          val leDouble = if (leStr == "+Inf") Double.PositiveInfinity else leStr.toDouble
          leDouble -> bucket
        }.sortBy(_._1)

        // apply counter correction on the buckets
        val samples = sortedBucketRvs.map(rv => new BufferableCounterCorrectionIterator(rv._2.rows))
        val buckets = sortedBucketRvs.map {b => Bucket(b._1, 0d)}
        val quantileResult = new Iterator[RowReader] {
          val row = new TransientRow()
          override def hasNext: Boolean = samples.forall(_.hasNext)
          override def next(): RowReader = {
            for { i <- 0 until samples.length } {
              val nxt = samples(i).next()
              buckets(i).count = nxt.getDouble(1)
              row.timestamp = nxt.getLong(0)
            }
            row.value = histogramQuantile(quantile, buckets)
            row
          }
        }
        IteratorBackedRangeVector(histBuckets._1, quantileResult)
      }
      Observable.fromIterable(quantileResults)
    }
    Observable.fromTask(res).flatten
  }

  /**
    * Given a bunch of range vectors, this function groups them after ignoring the le tag.
    * Function returns a map of the histogram range vector key to all its buckets.
    * This is essentially a helper function used to group relevant buckets together to calculate quantiles.
    */
  private def groupRangeVectorsByHistogram(rvs: Seq[RangeVector]): Map[CustomRangeVectorKey, Seq[RangeVector]] = {
    rvs.groupBy { rv =>
      val resultKey = rv.key.labelValues - le
      CustomRangeVectorKey(resultKey)
    }
  }

  /**
    * Calculates histogram le using the bucket values.
    * Similar to prometheus implementation for consistent results.
    */
  private def histogramQuantile(q: Double, buckets: Seq[Bucket]): Double = {
    if (q < 0) Double.NegativeInfinity
    else if (q > 1) Double.PositiveInfinity
    else if (buckets.length < 2) Double.NaN
    else {
      if (!buckets.last.le.isPosInfinity) return Double.NaN
      else {
        ensureMonotonic(buckets)
        // find rank for the quantile using total number of occurrences
        var rank = q * buckets.last.count
        // using rank, find the le bucket which would have the requested quantile
        val b = buckets.indexWhere(_.count >= rank)

        // calculate quantile
        if (b == buckets.length-1) return buckets(buckets.length-2).le
        else if (b == 0 && buckets.head.le <= 0) return buckets.head.le
        else {
          // interpolate quantile within le bucket
          var (bucketStart, bucketEnd, count) = (0d, buckets(b).le, buckets(b).count)
          if (b > 0) {
            bucketStart = buckets(b-1).le
            count -= buckets(b-1).count
            rank -= buckets(b-1).count
          }
          bucketStart + (bucketEnd-bucketStart)*(rank/count)
        }
      }
    }
  }

  /**
    * Fixes any issue with monotonicity of supplied bucket counts.
    * This could happen if the bucket count values are not atomically obtained,
    * or if bucket le values change over time.
    */
  private def ensureMonotonic(buckets: Seq[Bucket]): Unit = {
    var max = 0d
    buckets.foreach{ b =>
      if (b.count.isNaN) b.count = max
      else if (b.count > max) max = b.count
      else if (b.count < max) b.count = max
    }
  }

  override protected[exec] def args: String = s"le=$quantile"
}
