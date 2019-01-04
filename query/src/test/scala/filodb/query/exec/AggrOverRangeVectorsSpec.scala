package filodb.query.exec

import scala.annotation.tailrec
import scala.util.Random

import com.tdunning.math.stats.TDigest
import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.query.{AggregationOperator, QueryConfig}

class AggrOverRangeVectorsSpec extends FunSpec with Matchers with ScalaFutures {

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val rand = new Random()
  val error = 0.0000001d

  it ("should work without grouping") {
    val ignoreKey = CustomRangeVectorKey(
      Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))

    val noKey = CustomRangeVectorKey(Map.empty)
    def noGrouping(rv: RangeVector): RangeVectorKey = noKey

    val samples: Array[RangeVector] = Array.fill(100)(new RangeVector {
      val data = Stream.from(0).map { n=>
        new TransientRow(n.toLong, rand.nextDouble())
      }.take(20)
      override def key: RangeVectorKey = ignoreKey
      override def rows: Iterator[RowReader] = data.iterator
    })

    // Sum
    val resultObs = RangeVectorAggregator.mapReduce(AggregationOperator.Sum,
      Nil, false, Observable.fromIterable(samples), noGrouping)
    val result = resultObs.toListL.runAsync.futureValue
    result.size shouldEqual 1
    result(0).key shouldEqual noKey
    val readyToAggr = samples.toList.map(_.rows.toList).transpose
    compareIter(result(0).rows.map(_.getDouble(1)), readyToAggr.map(_.map(_.getDouble(1)).sum).iterator)

    // Min
    val resultObs2 = RangeVectorAggregator.mapReduce(AggregationOperator.Min,
      Nil, false, Observable.fromIterable(samples), noGrouping)
    val result2 = resultObs2.toListL.runAsync.futureValue
    result2.size shouldEqual 1
    result2(0).key shouldEqual noKey
    val readyToAggr2 = samples.toList.map(_.rows.toList).transpose
    compareIter(result2(0).rows.map(_.getDouble(1)), readyToAggr2.map(_.map(_.getDouble(1)).min).iterator)

    // Count
    val resultObs3a = RangeVectorAggregator.mapReduce(AggregationOperator.Count,
      Nil, false, Observable.fromIterable(samples), noGrouping)
    val resultObs3 = RangeVectorAggregator.mapReduce(AggregationOperator.Count,
      Nil, true, resultObs3a, rv=>rv.key)
    val result3 = resultObs3.toListL.runAsync.futureValue
    result3.size shouldEqual 1
    result3(0).key shouldEqual noKey
    val readyToAggr3 = samples.toList.map(_.rows.toList).transpose
    compareIter(result3(0).rows.map(_.getDouble(1)), readyToAggr3.map(_.map(_.getDouble(1)).size.toDouble).iterator)

    // Avg
    val resultObs4a = RangeVectorAggregator.mapReduce(AggregationOperator.Avg,
      Nil, false, Observable.fromIterable(samples), noGrouping)
    val resultObs4 = RangeVectorAggregator.mapReduce(AggregationOperator.Avg,
      Nil, true, resultObs4a, rv=>rv.key)
    val result4 = resultObs4.toListL.runAsync.futureValue
    result4.size shouldEqual 1
    result4(0).key shouldEqual noKey
    val readyToAggr4 = samples.toList.map(_.rows.toList).transpose
    compareIter(result4(0).rows.map(_.getDouble(1)), readyToAggr4.map { v =>
      v.map(_.getDouble(1)).sum / v.map(_.getDouble(1)).size
    }.iterator)

    // BottomK
    val resultObs5a = RangeVectorAggregator.mapReduce(AggregationOperator.BottomK,
      Seq(3.0), false, Observable.fromIterable(samples), noGrouping)
    val resultObs5 = RangeVectorAggregator.mapReduce(AggregationOperator.BottomK,
      Seq(3.0), true, resultObs5a, rv=>rv.key)
    val result5 = resultObs5.toListL.runAsync.futureValue
    result5.size shouldEqual 1
    result5(0).key shouldEqual noKey
    val readyToAggr5 = samples.toList.map(_.rows.toList).transpose
    compareIter2(result5(0).rows.map(r=> Set(r.getDouble(2), r.getDouble(4), r.getDouble(6))),
      readyToAggr5.map { v =>
      v.map(_.getDouble(1)).sorted.take(3).toSet
    }.iterator)

    // TopK
    val resultObs6a = RangeVectorAggregator.mapReduce(AggregationOperator.TopK,
      Seq(3.0), false, Observable.fromIterable(samples), noGrouping)
    val resultObs6 = RangeVectorAggregator.mapReduce(AggregationOperator.TopK,
      Seq(3.0), true, resultObs6a, rv=>rv.key)
    val result6 = resultObs6.toListL.runAsync.futureValue
    result6.size shouldEqual 1
    result6(0).key shouldEqual noKey
    val readyToAggr6 = samples.toList.map(_.rows.toList).transpose
    compareIter2(result6(0).rows.map(r=> Set(r.getDouble(2), r.getDouble(4), r.getDouble(6))),
      readyToAggr6.map { v =>
        v.map(_.getDouble(1)).sorted(Ordering[Double].reverse).take(3).toSet
      }.iterator)

    // Quantile
    val resultObs7a = RangeVectorAggregator.mapReduce(AggregationOperator.Quantile,
      Seq(0.70), false, Observable.fromIterable(samples), noGrouping)
    val resultObs7 = RangeVectorAggregator.mapReduce(AggregationOperator.Quantile,
      Seq(0.70), true, resultObs7a, rv=>rv.key)
    val resultObs7b = RangeVectorAggregator.present(AggregationOperator.Quantile, Seq(0.70), resultObs7, 1000)
    val result7 = resultObs7b.toListL.runAsync.futureValue
    result7.size shouldEqual 1
    result7(0).key shouldEqual noKey
    val readyToAggr7 = samples.toList.map(_.rows.toList).transpose
    compareIter(result7(0).rows.map(_.getDouble(1)), readyToAggr7.map { v =>
      quantile(0.70, v.map(_.getDouble(1)))
    }.iterator)
  }

  private def quantile(q: Double, items: List[Double]): Double = {
    val tdig = TDigest.createArrayDigest(100)
    items.foreach(i => tdig.add(i))
    tdig.quantile(q)
  }

  val ignoreKey = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))

  val noKey = CustomRangeVectorKey(Map.empty)
  def noGrouping(rv: RangeVector): RangeVectorKey = noKey

  it ("should ignore NaN while aggregating") {

    val samples: Array[RangeVector] = Array(
      toRv(Seq((1L, Double.NaN), (2L, 5.6d))),
      toRv(Seq((1L, 4.6d), (2L, 4.4d))),
      toRv(Seq((1L, 2.1d), (2L, 5.4d)))
    )

    // Sum
    val resultObs = RangeVectorAggregator.mapReduce(AggregationOperator.Sum,
      Nil, false, Observable.fromIterable(samples), noGrouping)
    val result = resultObs.toListL.runAsync.futureValue
    result.size shouldEqual 1
    result(0).key shouldEqual noKey
    compareIter(result(0).rows.map(_.getDouble(1)), Seq(6.7d, 15.4d).iterator)

    // Min
    val resultObs2 = RangeVectorAggregator.mapReduce(AggregationOperator.Min,
      Nil, false, Observable.fromIterable(samples), noGrouping)
    val result2 = resultObs2.toListL.runAsync.futureValue
    result2.size shouldEqual 1
    result2(0).key shouldEqual noKey
    compareIter(result2(0).rows.map(_.getDouble(1)), Seq(2.1d, 4.4d).iterator)

    // Count
    val resultObs3a = RangeVectorAggregator.mapReduce(AggregationOperator.Count,
      Nil, false, Observable.fromIterable(samples), noGrouping)
    val resultObs3 = RangeVectorAggregator.mapReduce(AggregationOperator.Count,
      Nil, true, resultObs3a, rv=>rv.key)
    val result3 = resultObs3.toListL.runAsync.futureValue
    result3.size shouldEqual 1
    result3(0).key shouldEqual noKey
    compareIter(result3(0).rows.map(_.getDouble(1)), Seq(2d, 3d).iterator)

    // Avg
    val resultObs4a = RangeVectorAggregator.mapReduce(AggregationOperator.Avg,
      Nil, false, Observable.fromIterable(samples), noGrouping)
    val resultObs4 = RangeVectorAggregator.mapReduce(AggregationOperator.Avg,
      Nil, true, resultObs4a, rv=>rv.key)
    val result4 = resultObs4.toListL.runAsync.futureValue
    result4.size shouldEqual 1
    result4(0).key shouldEqual noKey
    compareIter(result4(0).rows.map(_.getDouble(1)), Seq(3.35d, 5.133333333333333d).iterator)

    // BottomK
    val resultObs5a = RangeVectorAggregator.mapReduce(AggregationOperator.BottomK,
      Seq(2.0), false, Observable.fromIterable(samples), noGrouping)
    val resultObs5 = RangeVectorAggregator.mapReduce(AggregationOperator.BottomK,
      Seq(2.0), true, resultObs5a, rv=>rv.key)
    val result5 = resultObs5.toListL.runAsync.futureValue
    result5.size shouldEqual 1
    result5(0).key shouldEqual noKey
    compareIter2(result5(0).rows.map(r=> Set(r.getDouble(2), r.getDouble(4))),
      Seq(Set(2.1d, 4.6d), Set(4.4, 5.4d)).iterator)

    // TopK
    val resultObs6a = RangeVectorAggregator.mapReduce(AggregationOperator.TopK,
      Seq(2.0), false, Observable.fromIterable(samples), noGrouping)
    val resultObs6 = RangeVectorAggregator.mapReduce(AggregationOperator.TopK,
      Seq(2.0), true, resultObs6a, rv=>rv.key)
    val result6 = resultObs6.toListL.runAsync.futureValue
    result6.size shouldEqual 1
    result6(0).key shouldEqual noKey
    compareIter2(result6(0).rows.map(r=> Set(r.getDouble(2), r.getDouble(4))),
      Seq(Set(4.6d, 2.1d), Set(5.6, 5.4d)).iterator)

    // Quantile
    val resultObs7a = RangeVectorAggregator.mapReduce(AggregationOperator.Quantile,
      Seq(0.5), false, Observable.fromIterable(samples), noGrouping)
    val resultObs7 = RangeVectorAggregator.mapReduce(AggregationOperator.Quantile,
      Seq(0.5), true, resultObs7a, rv=>rv.key)
    val resultObs7b = RangeVectorAggregator.present(AggregationOperator.Quantile, Seq(0.5), resultObs7, 1000)
    val result7 = resultObs7b.toListL.runAsync.futureValue
    result7.size shouldEqual 1
    result7(0).key shouldEqual noKey
    compareIter(result7(0).rows.map(_.getDouble(1)), Seq(3.35d, 5.4d).iterator)
  }

  it ("should be able to serialize to and deserialize t-digest from SerializableRangeVector") {
    val samples: Array[RangeVector] = Array(
      toRv(Seq((1L, Double.NaN), (2L, 5.6d))),
      toRv(Seq((1L, 4.6d), (2L, 4.4d))),
      toRv(Seq((1L, 2.1d), (2L, 5.4d)))
    )

    // Quantile
    val resultObs7a = RangeVectorAggregator.mapReduce(AggregationOperator.Quantile,
      Seq(0.5), false, Observable.fromIterable(samples), noGrouping)
    val resultObs7 = RangeVectorAggregator.mapReduce(AggregationOperator.Quantile,
      Seq(0.5), true, resultObs7a, rv=>rv.key)
    val result7 = resultObs7.toListL.runAsync.futureValue
    result7.size shouldEqual 1

    val recSchema = SerializableRangeVector.toSchema(Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
                                                         ColumnInfo("tdig", ColumnType.StringColumn)))
    val builder = SerializableRangeVector.toBuilder(recSchema)
    val srv = SerializableRangeVector(result7(0), builder, recSchema, 10)

    val resultObs7b = RangeVectorAggregator.present(AggregationOperator.Quantile, Seq(0.5), Observable.now(srv), 1000)
    val finalResult = resultObs7b.toListL.runAsync.futureValue
    compareIter(finalResult(0).rows.map(_.getDouble(1)), Seq(3.35d, 5.4d).iterator)

  }

  private def toRv(samples: Seq[(Long, Double)]): RangeVector = {
    new RangeVector {
      override def key: RangeVectorKey = ignoreKey
      override def rows: Iterator[RowReader] = samples.map(r => new TransientRow(r._1, r._2)).iterator
    }
  }

  it ("average should work with NaN Test case 2 ") {
    val s1 = Seq( (1541190600L, Double.NaN), (1541190660L, Double.NaN), (1541190720L, Double.NaN),
         (1541190780L, Double.NaN), (1541190840L, Double.NaN), (1541190900L, 1.0), (1541190960L, 1.0))
    val s2 = Seq( (1541190600L, 1.0d), (1541190660L,1.0d), (1541190720L,1.0d),
         (1541190780L,1.0d), (1541190840L,1.0d), (1541190900L,1.0d), (1541190960L,1.0d))

    val mapped1 = RangeVectorAggregator.mapReduce(AggregationOperator.Avg,
      Nil, false, Observable.fromIterable(Seq(toRv(s1))), noGrouping)

    val mapped2 = RangeVectorAggregator.mapReduce(AggregationOperator.Avg,
      Nil, false, Observable.fromIterable(Seq(toRv(s2))), noGrouping)

    val resultObs4 = RangeVectorAggregator.mapReduce(AggregationOperator.Avg,
      Nil, true, mapped1 ++ mapped2, rv=>rv.key)
    val result4 = resultObs4.toListL.runAsync.futureValue
    result4.size shouldEqual 1
    result4(0).key shouldEqual noKey
    // prior to this fix, test was returning List(NaN, NaN, NaN, NaN, NaN, 1.0, 1.0)
    result4(0).rows.map(_.getDouble(1)).toList shouldEqual Seq(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
  }

  it("should return NaN when all values are NaN for a timestamp ") {

    val samples: Array[RangeVector] = Array(
      toRv(Seq((1L, Double.NaN), (2L, 5.6d))),
      toRv(Seq((1L, Double.NaN), (2L, 4.4d))),
      toRv(Seq((1L, Double.NaN), (2L, 5.4d)))
    )

    // Sum
    val resultObs = RangeVectorAggregator.mapReduce(AggregationOperator.Sum,
      Nil, false, Observable.fromIterable(samples), noGrouping)
    val result = resultObs.toListL.runAsync.futureValue
    result.size shouldEqual 1
    result(0).key shouldEqual noKey
    compareIter(result(0).rows.map(_.getDouble(1)), Seq(Double.NaN, 15.4d).iterator)

    // Min
    val resultObs2 = RangeVectorAggregator.mapReduce(AggregationOperator.Min,
      Nil, false, Observable.fromIterable(samples), noGrouping)
    val result2 = resultObs2.toListL.runAsync.futureValue
    result2.size shouldEqual 1
    result2(0).key shouldEqual noKey
    compareIter(result2(0).rows.map(_.getDouble(1)), Seq(Double.NaN, 4.4d).iterator)

    // Count
    val resultObs3a = RangeVectorAggregator.mapReduce(AggregationOperator.Count,
      Nil, false, Observable.fromIterable(samples), noGrouping)
    val resultObs3 = RangeVectorAggregator.mapReduce(AggregationOperator.Count,
      Nil, true, resultObs3a, rv => rv.key)
    val result3 = resultObs3.toListL.runAsync.futureValue
    result3.size shouldEqual 1
    result3(0).key shouldEqual noKey
    compareIter(result3(0).rows.map(_.getDouble(1)), Seq(Double.NaN, 3d).iterator)

    // Avg
    val resultObs4a = RangeVectorAggregator.mapReduce(AggregationOperator.Avg,
      Nil, false, Observable.fromIterable(samples), noGrouping)
    val resultObs4 = RangeVectorAggregator.mapReduce(AggregationOperator.Avg,
      Nil, true, resultObs4a, rv => rv.key)
    val result4 = resultObs4.toListL.runAsync.futureValue
    result4.size shouldEqual 1
    result4(0).key shouldEqual noKey
    compareIter(result4(0).rows.map(_.getDouble(1)), Seq(Double.NaN, 5.133333333333333d).iterator)

  }

  @tailrec
  final private def compareIter(it1: Iterator[Double], it2: Iterator[Double]) : Unit = {
    (it1.hasNext, it2.hasNext) match{
      case (true, true) =>
        val v1 = it1.next()
        val v2 = it2.next()
        if (v1.isNaN) v2.isNaN shouldEqual true
        else Math.abs(v1-v2) should be < error
        compareIter(it1, it2)
      case (false, false) => Unit
      case _ => fail("Unequal lengths")
    }
  }

  @tailrec
  final private def compareIter2(it1: Iterator[Set[Double]], it2: Iterator[Set[Double]]) : Unit = {
    (it1.hasNext, it2.hasNext) match{
      case (true, true) =>
        val v1 = it1.next()
        val v2 = it2.next()
        v1 shouldEqual v2
        compareIter2(it1, it2)
      case (false, false) => Unit
      case _ => fail("Unequal lengths")
    }
  }
}
