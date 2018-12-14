package filodb.query.exec

import monix.reactive.Observable

import filodb.core.query._
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.query.QueryConfig

object HistogramQuantileMapper {
  import ZeroCopyUTF8String._
  val le = "le".utf8
}

case class HistogramQuantileMapper(funcParams: Seq[Any]) extends RangeVectorTransformer {

  import HistogramQuantileMapper._
  require(funcParams.size == 1,
    "histogram_quantile function needs a single quantile argument")

  private val quantile = funcParams.head.asInstanceOf[Number].doubleValue()

  override def apply(source: Observable[RangeVector],
                     queryConfig: QueryConfig, limit: Int,
                     sourceSchema: ResultSchema): Observable[RangeVector] = {
    val res = source.toListL.map { rvs =>
      val resultKey = validateRangeVectorKeys(rvs.map(_.key))
      val sortedRvs = rvs.toArray.map { rv =>
        val leStr = rv.key.labelValues(le).toString
        val leDouble = if (leStr == "+Inf") Double.PositiveInfinity else leStr.toDouble
        leDouble -> rv
      }.sortBy(_._1)

      val samples = sortedRvs.map(_._2.rows)
      val buckets = sortedRvs.map{ b => Array(b._1, 0d)}
      val result = new Iterator[RowReader] {
        val row = new TransientRow()
        override def hasNext: Boolean = samples.forall(_.hasNext)
        override def next(): RowReader = {
          for { i <- 0 until samples.length } {
            val nxt = samples(i).next()
            buckets(i)(1) = nxt.getDouble(1)
            row.timestamp = nxt.getLong(0)
          }
          row.value = histogramQuantile(quantile, buckets)
          row
        }
      }
      IteratorBackedRangeVector(resultKey, result)
    }
    Observable.fromTask(res)
  }

  private def validateRangeVectorKeys(rvKeys: Seq[RangeVectorKey]): CustomRangeVectorKey = {
    // assert all RV Keys are same except for "le" tag
    val resultKey = rvKeys.head.labelValues - le
    require(rvKeys.forall(k => (k.labelValues - le) == resultKey),
      "Source vectors for histogram should be the same except for le tag")
    CustomRangeVectorKey(resultKey)
  }

  private def histogramQuantile(q: Double, buckets: Array[Array[Double]]): Double = {
    if (q < 0) Double.NegativeInfinity
    else if (q > 1) Double.PositiveInfinity
    else if (buckets.length < 2) Double.NaN
    else {
      if (!buckets.last(0).isPosInfinity) return Double.NaN
      else {
        ensureMonotonic(buckets)
        var rank = q * buckets.last(1)
        val b = buckets.indexWhere(_(1) >= rank)
        if (b == buckets.length-1) return buckets(buckets.length-2)(0)
        else if (b == 0 && buckets.head(0) <= 0) return buckets.head(0)
        else {
          var (bucketStart, bucketEnd, count) = (0d, buckets(b)(0), buckets(b)(1))
          if (b > 0) {
            bucketStart = buckets(b-1)(0)
            count -= buckets(b-1)(1)
            rank -= buckets(b-1)(1)
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
  private def ensureMonotonic(buckets: Array[Array[Double]]): Unit = {
    var max = buckets(0)(1)
    buckets.foreach{ b =>
      if (b(1) > max) max = b(1)
      else if (b(1) < max) b(1) = max
    }
  }

  override protected[exec] def args: String = s"quantile=$quantile"
}
