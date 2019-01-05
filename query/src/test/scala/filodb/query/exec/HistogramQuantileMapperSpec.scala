package filodb.query.exec

import scala.util.Random

import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.ZeroCopyUTF8String
import filodb.query.QueryConfig

class HistogramQuantileMapperSpec extends FunSpec with Matchers with ScalaFutures {

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  import ZeroCopyUTF8String._
  import HistogramQuantileMapper._

  private def genHistBuckets(histKey: Map[ZeroCopyUTF8String, ZeroCopyUTF8String]): Array[CustomRangeVectorKey] = {
    val numBuckets = 8
    Array.tabulate(numBuckets) { i =>
      if (i < numBuckets -1 ) {
        val leVal = Math.pow(2, i)
        CustomRangeVectorKey(histKey + (le -> leVal.toString.utf8))
      } else {
        CustomRangeVectorKey(histKey + (le -> "+Inf".utf8))
      }
    }
  }

  val histKey1 = Map("dc".utf8->"dc1".utf8, "host".utf8->"host1".utf8, "isntance".utf8->"instance1".utf8)
  val histKey2 = Map("dc".utf8->"dc1".utf8, "host".utf8->"host1".utf8, "isntance".utf8->"instance2".utf8)

  val bucketValues = Array(
    Array( (10, 10), (20, 6), (30, 11), (40, 4) ),
    Array( (10, 15), (20, 16), (30, 16), (40, 5) ),
    Array( (10, 17), (20, 26), (30, 26), (40, 4) ),
    Array( (10, 20), (20, 26), (30, 27), (40, 33) ),
    Array( (10, 25), (20, 36),  (30, 33), (40, 35) ),
    Array( (10, 34), (20, 38), (30, 42),  (40, 67) ),
    Array( (10, 76), (20, 56), (30, 46), (40, 91) ),
    Array( (10, 82), (20, 59), (30, 55),  (40, 121) ))

  val quantile50Result = Seq((10,37.333333333333336), (20,10.8), (30,8.666666666666666), (40,28.75))

  val histBuckets1 = genHistBuckets(histKey1)
  val histBuckets2 = genHistBuckets(histKey2)

  private def calculateAndVerify(
                      q: Double,
                      histRvs: Array[IteratorBackedRangeVector],
                      expectedResult: Seq[(Map[ZeroCopyUTF8String, ZeroCopyUTF8String], Seq[(Int, Double)])]): Unit = {
    val hqMapper = HistogramQuantileMapper(Seq(q))

    val result = hqMapper.apply(Observable.fromIterable(histRvs),
      queryConfig, 10, new ResultSchema(Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
        ColumnInfo("value", ColumnType.DoubleColumn)), 1))
      .toListL.runAsync.futureValue
    for { i <- expectedResult.indices } {
        expectedResult(i)._1 shouldEqual result(i).key.labelValues
        val resultSamples = result(i).rows.map(r => (r.getLong(0), r.getDouble(1))).toList
        resultSamples shouldEqual expectedResult(i)._2
    }
  }

  it ("should calculate histogram_quantile correctly") {
    val histRvs = bucketValues.zipWithIndex.map { case (rv, i) =>
      IteratorBackedRangeVector(histBuckets1(i), rv.map(s => new TransientRow(s._1, s._2.toDouble)).toIterator)
    }

    val expectedResult = Seq(histKey1 -> quantile50Result)
    calculateAndVerify(0.5, histRvs, expectedResult)
  }

  it ("should calculate histogram_quantile correctly for multiple histograms") {
    val histRvs = bucketValues.zipWithIndex.map { case (rv, i) =>
      IteratorBackedRangeVector(histBuckets1(i), rv.map(s => new TransientRow(s._1, s._2.toDouble)).toIterator)
    } ++ bucketValues.zipWithIndex.map { case (rv, i) =>
      IteratorBackedRangeVector(histBuckets2(i), rv.map(s => new TransientRow(s._1, s._2.toDouble)).toIterator)
    }

    val expectedResult = Seq(histKey2 -> quantile50Result, histKey1 -> quantile50Result)
    calculateAndVerify(0.5, histRvs, expectedResult)
  }

  it ("should sort the buckets to calculate histogram_quantile correctly ") {
    val histRvs = bucketValues.zipWithIndex.map { case (rv, i) =>
      IteratorBackedRangeVector(histBuckets1(i), rv.map(s => new TransientRow(s._1, s._2.toDouble)).toIterator)
    }

    val shuffledHistRvs = Random.shuffle(histRvs.toSeq).toArray

    val expectedResult = Seq(histKey1 -> quantile50Result)
    calculateAndVerify(0.5, shuffledHistRvs, expectedResult)
  }

  it ("should calculate histogram_quantile correctly when buckets change over time") {
    val histRvs = Array(
      Array[(Int, Double)]( (10, Double.NaN), (20, Double.NaN), (30, 11), (40, 40) ),
      Array[(Int, Double)]( (10, 15), (20, 16), (30, 16), (40, 45) ),
      Array[(Int, Double)]( (10, 17), (20, 26), (30, 26), (40, 47) ),
      Array[(Int, Double)]( (10, 20), (20, 30), (30, 33), (40, 49) ),
      Array[(Int, Double)]( (10, 25), (20, 30), (30, 33), (40, 57) ),
      Array[(Int, Double)]( (10, Double.NaN), (20, Double.NaN), (30, 38), (40, 67) ),
      Array[(Int, Double)]( (10, 34), (20, 42), (30, 46), (40, Double.NaN) ),
      Array[(Int, Double)]( (10, 35), (20, 45), (30, 46), (40, 89) )
    ).zipWithIndex.map { case (rv, i) =>
      IteratorBackedRangeVector(histBuckets1(i), rv.map(s => new TransientRow(s._1, s._2)).toIterator)
    }

    val expectedResult = Seq(histKey1 -> Seq((10, 4.666666666666667), (20, 3.3), (30, 3.4), (40, 1.9)))
    calculateAndVerify(0.5, histRvs, expectedResult)
  }

}
