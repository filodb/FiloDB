package filodb.query.exec

import monix.reactive.Observable

import filodb.core.query._
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.query.QueryConfig

case class HistogramQuantileMapper(funcParams: Seq[Any]) extends RangeVectorTransformer {

  require(funcParams.size == 1,
    "histogram_quantile function needs a single quantile argument")

  val quantile = funcParams.head.asInstanceOf[Number].doubleValue()

  override def apply(source: Observable[RangeVector],
                     queryConfig: QueryConfig, limit: Int,
                     sourceSchema: ResultSchema): Observable[RangeVector] = {
    import ZeroCopyUTF8String._
    val res = source.toListL.map { rvs =>
      val resultKey = validateRangeVectorKeys(rvs.map(_.key))
      val sortedRvs = rvs.sortBy(_.key.labelValues("le".utf8).toString)
      val samples = sortedRvs.map(_.rows)
      val buckets = Array.tabulate(sortedRvs.length) { i =>
        val le = sortedRvs(i).key.labelValues("le".utf8).toString.toDouble
        Array(le, 0d)
      }
      val result = new Iterator[RowReader] {
        val row = new TransientRow()
        override def hasNext: Boolean = samples.forall(_.hasNext)
        override def next(): RowReader = {
          for { i <- 0 to samples.length } {
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

  def validateRangeVectorKeys(rvKeys: Seq[RangeVectorKey]): CustomRangeVectorKey = {
    // TODO assert all RV are same except for "le" tag

  }

  def histogramQuantile(q: Double, buckets: Array[Array[Double]]): Double = {
    if (q < 0) Double.NegativeInfinity
    else if (q > 1) Double.PositiveInfinity
    else if (buckets.length < 2) Double.NaN
    else {
      if (buckets.last(0).isPosInfinity) return Double.NaN
      else {
        // TODO ensureMonotonic
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

  override protected[exec] def args: String = s"quantile=$quantile"
}
