package filodb.query.exec

import monix.reactive.Observable
import org.agrona.MutableDirectBuffer
import spire.syntax.cfor._

import filodb.core.query._
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.memory.format.vectors.Histogram
import filodb.query.BadQueryException

object HistogramQuantileMapper {
  import ZeroCopyUTF8String._
  val le = "le".utf8
}

/**
  * Calculates histogram quantile for one or more histograms whose bucket range vectors are passed
  * into the apply function.
  *
  * @param funcParams Needs one double quantile argument
  */
final case class HistogramQuantileMapper(funcParams: Seq[FuncArgs]) extends RangeVectorTransformer {

  import HistogramQuantileMapper._
  require(funcParams.size == 1, "histogram_quantile function needs a single quantile argument")
  require(funcParams.head.isInstanceOf[StaticFuncArgs], "Dynamic arg not supported yet")

  private val quantile = funcParams.head.asInstanceOf[StaticFuncArgs].scalar

  /**
    * Represents a prometheus histogram bucket for quantile calculation purposes.
    * @param le the less-than-equals boundary for histogram bucket
    * @param rate number of occurrences for the bucket per second
    */
  case class Bucket(le: Double, var rate: Double) {
    override def toString: String = s"$le->$rate"
  }

  /**
    * Groups incoming bucket range vectors by histogram name. It then calculates quantile for each histogram
    * using the buckets supplied for it. It is assumed that each bucket value contains rate of increase for that
    * bucket.
    *
    * Important Note: The source range vectors for each bucket should NOT be the counter values themselves,
    * but should be the rate of increase for that bucket counter. The histogram_quantile function should always
    * be preceded by a rate function or a sum-of-rate function.
    */
  override def apply(source: Observable[RangeVector],
                     querySession: QuerySession,
                     limit: Int,
                     sourceSchema: ResultSchema,
                     paramResponse: Seq[Observable[ScalarRangeVector]]): Observable[RangeVector] = {

    val res = source.toListL.map { rvs =>

      // first group the buckets by histogram
      val histograms = groupRangeVectorsByHistogram(rvs)

      // calculate quantile for each bucket
      val quantileResults = histograms.map { histBuckets =>

        // sort the bucket range vectors by increasing le tag value
        val sortedBucketRvs = histBuckets._2.toArray.map { bucket =>
          val labelValues = bucket.key.labelValues
          if (!labelValues.contains(le))
            throw new BadQueryException("Cannot calculate histogram quantile" +
              s"because 'le' tag is absent in the time series ${bucket.key.labelValues}")
          val leStr = labelValues(le).toString
          val leDouble = if (leStr == "+Inf") Double.PositiveInfinity else leStr.toDouble
          leDouble -> bucket
        }.sortBy(_._1)

        val samples = sortedBucketRvs.map(_._2.rows)

        // The buckets here will be populated for each instant for quantile calculation
        val buckets = sortedBucketRvs.map { b => Bucket(b._1, 0d) }

        // create the result iterator that lazily produces quantile for each timestamp
        val quantileResult = new RangeVectorCursor {
          val row = new TransientRow()
          override def hasNext: Boolean = samples.forall(_.hasNext)
          override def next(): RowReader = {
            cforRange { 0 until samples.size } { i =>
              val nxt = samples(i).next()
              buckets(i).rate = nxt.getDouble(1)
              row.timestamp = nxt.getLong(0)
            }
            row.value = histogramQuantile(quantile, buckets)
            row
          }
          override def close(): Unit = rvs.foreach(_.rows().close())
        }
        IteratorBackedRangeVector(histBuckets._1, quantileResult, sortedBucketRvs.headOption.flatMap(_._2.outputRange))
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
      val resultKey = rv.key.labelValues - le // remove the le tag from the labels
      CustomRangeVectorKey(resultKey)
    }
  }

  private case class PromRateHistogram(buckets: Array[Bucket]) extends Histogram {
    final def numBuckets: Int = buckets.size
    final def bucketTop(no: Int): Double = buckets(no).le
    final def bucketValue(no: Int): Double = buckets(no).rate
    final def serialize(intoBuf: Option[MutableDirectBuffer] = None): MutableDirectBuffer = ???
  }

  /**
    * Calculates histogram quantile using the bucket values.
    * Similar to prometheus implementation for consistent results.
    */
  private def histogramQuantile(q: Double, buckets: Array[Bucket]): Double = {
    if (!buckets.last.le.isPosInfinity) Double.NaN
    else {
      makeMonotonic(buckets)
      PromRateHistogram(buckets).quantile(q)
    }
  }

  /**
    * Fixes any issue with monotonicity of supplied bucket rates.
    * Rates on increasing le buckets should monotonically increase. It may not be the case
    * if the bucket values are not atomically obtained from the same scrape,
    * or if bucket le values change over time causing NaN on missing buckets.
    */
  private def makeMonotonic(buckets: Array[Bucket]): Unit = {
    var max = 0d
    buckets.foreach { b =>
      // When bucket no longer used NaN will be seen. Non-increasing values can be seen when
      // newer buckets are introduced and not all instances are updated with that bucket.
      if (b.rate < max || b.rate.isNaN) b.rate = max // assign previous max
      else if (b.rate > max) max = b.rate // update max
    }
  }

  override protected[exec] def args: String = s"quantile=$quantile"
}
