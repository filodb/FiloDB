package filodb.query.exec

import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.core.metadata.Column.ColumnType
import filodb.core.query.{ColumnInfo, CustomRangeVectorKey, IteratorBackedRangeVector, ResultSchema}
import filodb.memory.format.ZeroCopyUTF8String
import filodb.query.QueryConfig

class HistogramQuantileMapperSpec extends FunSpec with Matchers with ScalaFutures {

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  import ZeroCopyUTF8String._
  import HistogramQuantileMapper._

  it ("should calculate histogram_quantile correctly") {
    val histKey = Map("dc".utf8->"dc1".utf8, "host".utf8->"host1".utf8, "instance".utf8->"instance1".utf8)
    val numBuckets = 8
    val histKeys = Array.tabulate(numBuckets) { i =>
      if (i < numBuckets -1 ) {
        val leVal = Math.pow(2, i)
        CustomRangeVectorKey(histKey + (le -> leVal.toString.utf8))
      } else {
        CustomRangeVectorKey(histKey + (le -> "+Inf".utf8))
      }
    }

    val histRvs = Array(
      Array( (10, 10), (20, 11), (30, 11), (40, 40) ),
      Array( (10, 15), (20, 16), (30, 16), (40, 45) ),
      Array( (10, 17), (20, 26), (30, 26), (40, 47) ),
      Array( (10, 20), (20, 28), (30, 33), (40, 49) ),
      Array( (10, 25), (20, 30), (30, 33), (40, 57) ),
      Array( (10, 34), (20, 38), (30, 38), (40, 67) ),
      Array( (10, 34), (20, 42), (30, 46), (40, 89) ),
      Array( (10, 35), (20, 45), (30, 46), (40, 89) )
    ).zipWithIndex.map { case (rv, i) =>
      IteratorBackedRangeVector(histKeys(i), rv.map(s => new TransientRow(s._1, s._2.toDouble)).toIterator)
    }

    val hqMapper = HistogramQuantileMapper(Seq(0.5))

    val result = hqMapper.apply(Observable.fromIterable(histRvs),
        queryConfig, 10, new ResultSchema(Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
                                              ColumnInfo("value", ColumnType.DoubleColumn)), 1))
      .toListL.runAsync.futureValue
    result.size shouldEqual 1
    result.head.key.labelValues shouldEqual histKey
    val resultSamples = result.head.rows.map(r=> (r.getLong(0), r.getDouble(1))).toList
    resultSamples shouldEqual Seq((10,4.666666666666667), (20,3.3), (30,3.4), (40,1.9))
  }

  it ("should calculate histogram_quantile correctly after doing counter corrections") {

  }

  it ("should calculate histogram_quantile correctly when buckets change over time") {

  }

}
