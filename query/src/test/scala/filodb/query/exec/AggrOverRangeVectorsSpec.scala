package filodb.query.exec

import scala.annotation.tailrec
import scala.util.Random
import com.tdunning.math.stats.TDigest
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures
import filodb.core.{MachineMetricsData => MMD}
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.RowReader
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query.AggregationOperator
import filodb.query.exec.aggregator.RowAggregator
import filodb.query.exec.rangefn.RawDataWindowingSpec

class AggrOverRangeVectorsSpec extends RawDataWindowingSpec with ScalaFutures {
  val rand = new Random()
  val error = 0.0000001d

  val tvSchema = ResultSchema(Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
                                  ColumnInfo("value", ColumnType.DoubleColumn)), 1)
  val histSchema = ResultSchema(MMD.histDataset.schema.infosFromIDs(Seq(0, 3)), 1)
  val histMaxSchema = ResultSchema(MMD.histMaxDS.schema.infosFromIDs(Seq(0, 4, 3)), 1, colIDs = Seq(0, 4, 3))

  it ("should work without grouping") {
    val ignoreKey = CustomRangeVectorKey(
      Map(("ignore").utf8 -> ("ignore").utf8))

    val noKey = CustomRangeVectorKey(Map.empty)
    def noGrouping(rv: RangeVector): RangeVectorKey = noKey

    val samples: Array[RangeVector] = Array.fill(100)(new RangeVector {
      val data = Stream.from(0).map { n=>
        new TransientRow(n.toLong, rand.nextDouble())
      }.take(20)
      override def key: RangeVectorKey = ignoreKey
      override def rows(): CloseableIterator[RowReader] = data.iterator
    })

    // Sum
    val agg1 = RowAggregator(AggregationOperator.Sum, Nil, tvSchema)
    val resultObs = RangeVectorAggregator.mapReduce(agg1, false, Observable.fromIterable(samples), noGrouping)
    val result = resultObs.toListL.runAsync.futureValue
    result.size shouldEqual 1
    result(0).key shouldEqual noKey
    val readyToAggr = samples.toList.map(_.rows.toList).transpose
    compareIter(result(0).rows.map(_.getDouble(1)), readyToAggr.map(_.map(_.getDouble(1)).sum).iterator)

    // Min
    val agg2 = RowAggregator(AggregationOperator.Min, Nil, tvSchema)
    val resultObs2 = RangeVectorAggregator.mapReduce(agg2, false, Observable.fromIterable(samples), noGrouping)
    val result2 = resultObs2.toListL.runAsync.futureValue
    result2.size shouldEqual 1
    result2(0).key shouldEqual noKey
    val readyToAggr2 = samples.toList.map(_.rows.toList).transpose
    compareIter(result2(0).rows.map(_.getDouble(1)), readyToAggr2.map(_.map(_.getDouble(1)).min).iterator)

    // Count
    val agg3 = RowAggregator(AggregationOperator.Count, Nil, tvSchema)
    val resultObs3a = RangeVectorAggregator.mapReduce(agg3, false, Observable.fromIterable(samples), noGrouping)
    val resultObs3 = RangeVectorAggregator.mapReduce(agg3, true, resultObs3a, rv=>rv.key)
    val result3 = resultObs3.toListL.runAsync.futureValue
    result3.size shouldEqual 1
    result3(0).key shouldEqual noKey
    val readyToAggr3 = samples.toList.map(_.rows.toList).transpose
    compareIter(result3(0).rows.map(_.getDouble(1)), readyToAggr3.map(_.map(_.getDouble(1)).size.toDouble).iterator)

    // Avg
    val agg4 = RowAggregator(AggregationOperator.Avg, Nil, tvSchema)
    val resultObs4a = RangeVectorAggregator.mapReduce(agg4, false, Observable.fromIterable(samples), noGrouping)
    val resultObs4 = RangeVectorAggregator.mapReduce(agg4, true, resultObs4a, rv=>rv.key)
    val result4 = resultObs4.toListL.runAsync.futureValue
    result4.size shouldEqual 1
    result4(0).key shouldEqual noKey
    val readyToAggr4 = samples.toList.map(_.rows.toList).transpose
    compareIter(result4(0).rows.map(_.getDouble(1)), readyToAggr4.map { v =>
      v.map(_.getDouble(1)).sum / v.map(_.getDouble(1)).size
    }.iterator)

    // BottomK
    val agg5 = RowAggregator(AggregationOperator.BottomK, Seq(3.0), tvSchema)
    val resultObs5a = RangeVectorAggregator.mapReduce(agg5, false, Observable.fromIterable(samples), noGrouping)
    val resultObs5 = RangeVectorAggregator.mapReduce(agg5, true, resultObs5a, rv=>rv.key)
    val result5 = resultObs5.toListL.runAsync.futureValue
    result5.size shouldEqual 1
    result5(0).key shouldEqual noKey
    val readyToAggr5 = samples.toList.map(_.rows.toList).transpose
    compareIter2(result5(0).rows.map(r=> Set(r.getDouble(2), r.getDouble(4), r.getDouble(6))),
      readyToAggr5.map { v =>
      v.map(_.getDouble(1)).sorted.take(3).toSet
    }.iterator)

    // TopK
    val agg6 = RowAggregator(AggregationOperator.TopK, Seq(3.0), tvSchema)
    val resultObs6a = RangeVectorAggregator.mapReduce(agg6, false, Observable.fromIterable(samples), noGrouping)
    val resultObs6 = RangeVectorAggregator.mapReduce(agg6, true, resultObs6a, rv=>rv.key)
    val result6 = resultObs6.toListL.runAsync.futureValue
    result6.size shouldEqual 1
    result6(0).key shouldEqual noKey
    val readyToAggr6 = samples.toList.map(_.rows.toList).transpose
    compareIter2(result6(0).rows.map(r=> Set(r.getDouble(2), r.getDouble(4), r.getDouble(6))),
      readyToAggr6.map { v =>
        v.map(_.getDouble(1)).sorted(Ordering[Double].reverse).take(3).toSet
      }.iterator)

    // Quantile
    val agg7 = RowAggregator(AggregationOperator.Quantile, Seq(0.70), tvSchema)
    val resultObs7a = RangeVectorAggregator.mapReduce(agg7, false, Observable.fromIterable(samples), noGrouping)
    val resultObs7 = RangeVectorAggregator.mapReduce(agg7, true, resultObs7a, rv=>rv.key)
    val resultObs7b = RangeVectorAggregator.present(agg7, resultObs7, 1000)
    val result7 = resultObs7b.toListL.runAsync.futureValue
    result7.size shouldEqual 1
    result7(0).key shouldEqual noKey
    val readyToAggr7 = samples.toList.map(_.rows.toList).transpose
    compareIter(result7(0).rows.map(_.getDouble(1)), readyToAggr7.map { v =>
      quantile(0.70, v.map(_.getDouble(1)))
    }.iterator)

    // Stdvar
    val agg8 = RowAggregator(AggregationOperator.Stdvar, Nil, tvSchema)
    val resultObs8a = RangeVectorAggregator.mapReduce(agg8, false, Observable.fromIterable(samples), noGrouping)
    val resultObs8 = RangeVectorAggregator.mapReduce(agg8, true, resultObs8a, rv=>rv.key)
    val result8 = resultObs8.toListL.runAsync.futureValue
    result8.size shouldEqual 1
    result8(0).key shouldEqual noKey

    val readyToAggr8 = samples.toList.map(_.rows.toList).transpose
    compareIter(result8(0).rows.map(_.getDouble(1)), readyToAggr8.map { v =>
      stdvar(v.map(_.getDouble(1)))
    }.iterator)

    // Stddev
    val agg9 = RowAggregator(AggregationOperator.Stddev, Nil, tvSchema)
    val resultObs9a = RangeVectorAggregator.mapReduce(agg9, false, Observable.fromIterable(samples), noGrouping)
    val resultObs9 = RangeVectorAggregator.mapReduce(agg9, true, resultObs9a, rv=>rv.key)
    val result9 = resultObs9.toListL.runAsync.futureValue
    result9.size shouldEqual 1
    result9(0).key shouldEqual noKey

    val readyToAggr9 = samples.toList.map(_.rows.toList).transpose
    compareIter(result9(0).rows.map(_.getDouble(1)), readyToAggr9.map { v =>
      stddev(v.map(_.getDouble(1)))
    }.iterator)
  }

  private def stdvar(items: List[Double]): Double = {
    val mean = items.sum / items.size
    items.map(i => math.pow((i-mean), 2)).sum / items.size
  }

  private def stddev(items: List[Double]): Double = {
    val mean = items.sum / items.size
    Math.pow(items.map(i => math.pow((i-mean), 2)).sum / items.size, 0.5)
  }

  private def quantile(q: Double, items: List[Double]): Double = {
    val tdig = TDigest.createArrayDigest(100)
    items.foreach(i => tdig.add(i))
    tdig.quantile(q)
  }

  val ignoreKey = CustomRangeVectorKey(
    Map("ignore".utf8 -> "ignore".utf8))

  val noKey = CustomRangeVectorKey(Map.empty)
  def noGrouping(rv: RangeVector): RangeVectorKey = noKey

  it ("should ignore NaN while aggregating") {

    val samples: Array[RangeVector] = Array(
      toRv(Seq((1L, Double.NaN), (2L, 5.6d))),
      toRv(Seq((1L, 4.6d), (2L, 4.4d))),
      toRv(Seq((1L, 2.1d), (2L, 5.4d)))
    )

    // Sum
    val agg1 = RowAggregator(AggregationOperator.Sum, Nil, tvSchema)
    val resultObs = RangeVectorAggregator.mapReduce(agg1, false, Observable.fromIterable(samples), noGrouping)
    val result = resultObs.toListL.runAsync.futureValue
    result.size shouldEqual 1
    result(0).key shouldEqual noKey
    compareIter(result(0).rows.map(_.getDouble(1)), Seq(6.7d, 15.4d).iterator)

    // Min
    val agg2 = RowAggregator(AggregationOperator.Min, Nil, tvSchema)
    val resultObs2 = RangeVectorAggregator.mapReduce(agg2, false, Observable.fromIterable(samples), noGrouping)
    val result2 = resultObs2.toListL.runAsync.futureValue
    result2.size shouldEqual 1
    result2(0).key shouldEqual noKey
    compareIter(result2(0).rows.map(_.getDouble(1)), Seq(2.1d, 4.4d).iterator)

    // Count
    val agg3 = RowAggregator(AggregationOperator.Count, Nil, tvSchema)
    val resultObs3a = RangeVectorAggregator.mapReduce(agg3, false, Observable.fromIterable(samples), noGrouping)
    val resultObs3 = RangeVectorAggregator.mapReduce(agg3, true, resultObs3a, rv=>rv.key)
    val result3 = resultObs3.toListL.runAsync.futureValue
    result3.size shouldEqual 1
    result3(0).key shouldEqual noKey
    compareIter(result3(0).rows.map(_.getDouble(1)), Seq(2d, 3d).iterator)

    // Avg
    val agg4 = RowAggregator(AggregationOperator.Avg, Nil, tvSchema)
    val resultObs4a = RangeVectorAggregator.mapReduce(agg4, false, Observable.fromIterable(samples), noGrouping)
    val resultObs4 = RangeVectorAggregator.mapReduce(agg4, true, resultObs4a, rv=>rv.key)
    val result4 = resultObs4.toListL.runAsync.futureValue
    result4.size shouldEqual 1
    result4(0).key shouldEqual noKey
    compareIter(result4(0).rows.map(_.getDouble(1)), Seq(3.35d, 5.133333333333333d).iterator)

    // BottomK
    val agg5 = RowAggregator(AggregationOperator.BottomK, Seq(2.0), tvSchema)
    val resultObs5a = RangeVectorAggregator.mapReduce(agg5, false, Observable.fromIterable(samples), noGrouping)
    val resultObs5 = RangeVectorAggregator.mapReduce(agg5, true, resultObs5a, rv=>rv.key)
    val resultObs5b = RangeVectorAggregator.present(agg5, resultObs5, 1000)
    val result5 = resultObs5.toListL.runAsync.futureValue
    result5.size shouldEqual 1
    result5(0).key shouldEqual noKey
    compareIter2(result5(0).rows.map(r=> Set(r.getDouble(2), r.getDouble(4))),
      Seq(Set(2.1d, 4.6d), Set(4.4, 5.4d)).iterator)
    val result5b = resultObs5b.toListL.runAsync.futureValue
    result5b.size shouldEqual 1
    result5b(0).key shouldEqual ignoreKey
    compareIter(result5b(0).rows.map(_.getDouble(1)), Seq(4.6d,2.1d,5.4d,4.4d).iterator)

    // TopK
    val agg6 = RowAggregator(AggregationOperator.TopK, Seq(2.0), tvSchema)
    val resultObs6a = RangeVectorAggregator.mapReduce(agg6, false, Observable.fromIterable(samples), noGrouping)
    val resultObs6 = RangeVectorAggregator.mapReduce(agg6, true, resultObs6a, rv=>rv.key)
    val resultObs6b = RangeVectorAggregator.present(agg6, resultObs6, 1000)
    val result6 = resultObs6.toListL.runAsync.futureValue
    result6.size shouldEqual 1
    result6(0).key shouldEqual noKey
    compareIter2(result6(0).rows.map(r=> Set(r.getDouble(2), r.getDouble(4))),
      Seq(Set(4.6d, 2.1d), Set(5.6, 5.4d)).iterator)
    val result6b = resultObs6b.toListL.runAsync.futureValue
    result6b.size shouldEqual 1
    result6b(0).key shouldEqual ignoreKey
    compareIter(result6b(0).rows.map(_.getDouble(1)), Seq(2.1d,4.6d,5.4d,5.6d).iterator)

    // Quantile
    val agg7 = RowAggregator(AggregationOperator.Quantile, Seq(0.5), tvSchema)
    val resultObs7a = RangeVectorAggregator.mapReduce(agg7, false, Observable.fromIterable(samples), noGrouping)
    val resultObs7 = RangeVectorAggregator.mapReduce(agg7, true, resultObs7a, rv=>rv.key)
    val resultObs7b = RangeVectorAggregator.present(agg7, resultObs7, 1000)
    val result7 = resultObs7b.toListL.runAsync.futureValue
    result7.size shouldEqual 1
    result7(0).key shouldEqual noKey
    compareIter(result7(0).rows.map(_.getDouble(1)), Seq(3.35d, 5.4d).iterator)

    // Stdvar
    val agg8 = RowAggregator(AggregationOperator.Stdvar, Nil, tvSchema)
    val resultObs8a = RangeVectorAggregator.mapReduce(agg8, false, Observable.fromIterable(samples), noGrouping)
    val resultObs8 = RangeVectorAggregator.mapReduce(agg8, true, resultObs8a, rv=>rv.key)
    val result8 = resultObs8.toListL.runAsync.futureValue
    result8.size shouldEqual 1
    result8(0).key shouldEqual noKey
    compareIter(result8(0).rows.map(_.getDouble(1)), Seq(1.5625d, 0.27555555555556d).iterator)

    // Stddev
    val agg9 = RowAggregator(AggregationOperator.Stddev, Nil, tvSchema)
    val resultObs9a = RangeVectorAggregator.mapReduce(agg9, false, Observable.fromIterable(samples), noGrouping)
    val resultObs9 = RangeVectorAggregator.mapReduce(agg9, true, resultObs9a, rv=>rv.key)
    val result9 = resultObs9.toListL.runAsync.futureValue
    result9.size shouldEqual 1
    result9(0).key shouldEqual noKey
    compareIter(result9(0).rows.map(_.getDouble(1)), Seq(1.25d, 0.52493385826745d).iterator)
  }

  it ("should be able to serialize to and deserialize t-digest from SerializedRangeVector") {
    val samples: Array[RangeVector] = Array(
      toRv(Seq((1L, Double.NaN), (2L, 5.6d))),
      toRv(Seq((1L, 4.6d), (2L, 4.4d))),
      toRv(Seq((1L, 2.1d), (2L, 5.4d)))
    )

    // Quantile
    val agg7 = RowAggregator(AggregationOperator.Quantile, Seq(0.5), tvSchema)
    val resultObs7a = RangeVectorAggregator.mapReduce(agg7, false, Observable.fromIterable(samples), noGrouping)
    val resultObs7 = RangeVectorAggregator.mapReduce(agg7, true, resultObs7a, rv=>rv.key)
    val result7 = resultObs7.toListL.runAsync.futureValue
    result7.size shouldEqual 1

    val recSchema = SerializedRangeVector.toSchema(Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
                                                         ColumnInfo("tdig", ColumnType.StringColumn)))
    val builder = SerializedRangeVector.newBuilder()
    val srv = SerializedRangeVector(result7(0), builder, recSchema, "Unit-Test")

    val resultObs7b = RangeVectorAggregator.present(agg7, Observable.now(srv), 1000)
    val finalResult = resultObs7b.toListL.runAsync.futureValue
    compareIter(finalResult(0).rows.map(_.getDouble(1)), Seq(3.35d, 5.4d).iterator)

  }

  private def toRv(samples: Seq[(Long, Double)]): RangeVector = {
    new RangeVector {
      override def key: RangeVectorKey = ignoreKey
      override def rows(): CloseableIterator[RowReader] = samples.map(r => new TransientRow(r._1, r._2)).iterator
    }
  }

  it ("average should work with NaN Test case 2 ") {
    val s1 = Seq( (1541190600L, Double.NaN), (1541190660L, Double.NaN), (1541190720L, Double.NaN),
         (1541190780L, Double.NaN), (1541190840L, Double.NaN), (1541190900L, 1.0), (1541190960L, 1.0))
    val s2 = Seq( (1541190600L, 1.0d), (1541190660L,1.0d), (1541190720L,1.0d),
         (1541190780L,1.0d), (1541190840L,1.0d), (1541190900L,1.0d), (1541190960L,1.0d))

    val agg = RowAggregator(AggregationOperator.Avg, Nil, tvSchema)
    val aggMR = AggregateMapReduce(AggregationOperator.Avg, Nil, Nil, Nil)
    val mapped1 = aggMR(Observable.fromIterable(Seq(toRv(s1))), querySession, 1000, tvSchema)
    val mapped2 = aggMR(Observable.fromIterable(Seq(toRv(s2))), querySession, 1000, tvSchema)

    val resultObs4 = RangeVectorAggregator.mapReduce(agg, true, mapped1 ++ mapped2, rv=>rv.key)
    val result4 = resultObs4.toListL.runAsync.futureValue
    result4.size shouldEqual 1
    result4(0).key shouldEqual noKey
    // prior to this fix, test was returning List(NaN, NaN, NaN, NaN, NaN, 1.0, 1.0)
    result4(0).rows.map(_.getDouble(1)).toList shouldEqual Seq(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
  }

  it("stdvar and stddev should work for with NaN Test case 2") {
    val samples: Array[RangeVector] = Array(
      toRv(Seq((1L, 3247.0), (2L, 3297.0))),
      toRv(Seq((1L, Double.NaN), (2L, Double.NaN))),
      toRv(Seq((1L, Double.NaN), (2L, Double.NaN))),
      toRv(Seq((1L, Double.NaN), (2L, Double.NaN))),
      toRv(Seq((1L, Double.NaN), (2L, Double.NaN))),
      toRv(Seq((1L, Double.NaN), (2L, Double.NaN))),
      toRv(Seq((1L, Double.NaN), (2L, Double.NaN))),
      toRv(Seq((1L, 5173.0), (2L, 5173.0))),
      toRv(Seq((1L, Double.NaN), (2L, Double.NaN))),
      toRv(Seq((1L, 11583.0), (2L, 11583.0))),
      toRv(Seq((1L, Double.NaN), (2L, Double.NaN)))
    )

    // Stdvar
    val agg1 = RowAggregator(AggregationOperator.Stdvar, Nil, tvSchema)
    val resultObs1a = RangeVectorAggregator.mapReduce(agg1, false, Observable.fromIterable(samples), noGrouping)
    val resultObs1 = RangeVectorAggregator.mapReduce(agg1, true, resultObs1a, rv => rv.key)
    val result1 = resultObs1.toListL.runAsync.futureValue
    result1.size shouldEqual 1
    result1(0).key shouldEqual noKey
    compareIter(result1(0).rows.map(_.getDouble(1)), Seq(12698496.88888889d, 12585030.222222222d).iterator)

    // Stddev
    val agg2 = RowAggregator(AggregationOperator.Stddev, Nil, tvSchema)
    val resultObs2a = RangeVectorAggregator.mapReduce(agg2, false, Observable.fromIterable(samples), noGrouping)
    val resultObs2 = RangeVectorAggregator.mapReduce(agg2, true, resultObs2a, rv => rv.key)
    val result2 = resultObs2.toListL.runAsync.futureValue
    result2.size shouldEqual 1
    result2(0).key shouldEqual noKey
    compareIter(result2(0).rows.map(_.getDouble(1)), Seq(3563.4950384263d, 3547.5386146203d).iterator)
  }

  it("should return NaN when all values are NaN for a timestamp ") {

    val samples: Array[RangeVector] = Array(
      toRv(Seq((1L, Double.NaN), (2L, 5.6d))),
      toRv(Seq((1L, Double.NaN), (2L, 4.4d))),
      toRv(Seq((1L, Double.NaN), (2L, 5.4d)))
    )

    // Sum
    val agg1 = RowAggregator(AggregationOperator.Sum, Nil, tvSchema)
    val resultObs = RangeVectorAggregator.mapReduce(agg1, false, Observable.fromIterable(samples), noGrouping)
    val result = resultObs.toListL.runAsync.futureValue
    result.size shouldEqual 1
    result(0).key shouldEqual noKey
    compareIter(result(0).rows.map(_.getDouble(1)), Seq(Double.NaN, 15.4d).iterator)

    // Min
    val agg2 = RowAggregator(AggregationOperator.Min, Nil, tvSchema)
    val resultObs2 = RangeVectorAggregator.mapReduce(agg2, false, Observable.fromIterable(samples), noGrouping)
    val result2 = resultObs2.toListL.runAsync.futureValue
    result2.size shouldEqual 1
    result2(0).key shouldEqual noKey
    compareIter(result2(0).rows.map(_.getDouble(1)), Seq(Double.NaN, 4.4d).iterator)

    // Count
    val agg3 = RowAggregator(AggregationOperator.Count, Nil, tvSchema)
    val resultObs3a = RangeVectorAggregator.mapReduce(agg3, false, Observable.fromIterable(samples), noGrouping)
    val resultObs3 = RangeVectorAggregator.mapReduce(agg3, true, resultObs3a, rv => rv.key)
    val result3 = resultObs3.toListL.runAsync.futureValue
    result3.size shouldEqual 1
    result3(0).key shouldEqual noKey
    compareIter(result3(0).rows.map(_.getDouble(1)), Seq(Double.NaN, 3d).iterator)

    // Avg
    val agg4 = RowAggregator(AggregationOperator.Avg, Nil, tvSchema)
    val resultObs4a = RangeVectorAggregator.mapReduce(agg4, false, Observable.fromIterable(samples), noGrouping)
    val resultObs4 = RangeVectorAggregator.mapReduce(agg4, true, resultObs4a, rv => rv.key)
    val result4 = resultObs4.toListL.runAsync.futureValue
    result4.size shouldEqual 1
    result4(0).key shouldEqual noKey
    compareIter(result4(0).rows.map(_.getDouble(1)), Seq(Double.NaN, 5.133333333333333d).iterator)

    // BottomK
    val agg5 = RowAggregator(AggregationOperator.BottomK, Seq(2.0), tvSchema)
    val resultObs5a = RangeVectorAggregator.mapReduce(agg5, false, Observable.fromIterable(samples), noGrouping)
    val resultObs5 = RangeVectorAggregator.mapReduce(agg5, true, resultObs5a, rv=>rv.key)
    val resultObs5b = RangeVectorAggregator.present(agg5, resultObs5, 1000)
    val result5 = resultObs5.toListL.runAsync.futureValue
    result5.size shouldEqual 1
    result5(0).key shouldEqual noKey
    // mapReduce returns range vector which has all values as Double.Max
    compareIter2(result5(0).rows.map(r=> Set(r.getDouble(2), r.getDouble(4))),
      Seq(Set(1.7976931348623157E308d, 1.7976931348623157E308d), Set(4.4d, 5.4d)).iterator)
    val result5b = resultObs5b.toListL.runAsync.futureValue
    result5b.size shouldEqual 1
    result5b(0).key shouldEqual ignoreKey
    // present removes the range vector which has all values as Double.Max
    compareIter(result5b(0).rows.map(_.getDouble(1)), Seq(5.4d, 4.4d).iterator)

    // TopK
    val agg6 = RowAggregator(AggregationOperator.TopK, Seq(2.0), tvSchema)
    val resultObs6a = RangeVectorAggregator.mapReduce(agg6, false, Observable.fromIterable(samples), noGrouping)
    val resultObs6 = RangeVectorAggregator.mapReduce(agg6, true, resultObs6a, rv=>rv.key)
    val resultObs6b = RangeVectorAggregator.present(agg6, resultObs6, 1000)
    val result6 = resultObs6.toListL.runAsync.futureValue
    result6.size shouldEqual 1
    result6(0).key shouldEqual noKey
    compareIter2(result6(0).rows.map(r=> Set(r.getDouble(2), r.getDouble(4))),
      Seq(Set(-1.7976931348623157E308d, -1.7976931348623157E308d), Set(5.6, 5.4d)).iterator)
    val result6b = resultObs6b.toListL.runAsync.futureValue
    result6b.size shouldEqual 1
    result6b(0).key shouldEqual ignoreKey
    compareIter(result6b(0).rows.map(_.getDouble(1)), Seq(5.4d,5.6d).iterator)

    // Stdvar
    val agg8 = RowAggregator(AggregationOperator.Stdvar, Nil, tvSchema)
    val resultObs8a = RangeVectorAggregator.mapReduce(agg8, false, Observable.fromIterable(samples), noGrouping)
    val resultObs8 = RangeVectorAggregator.mapReduce(agg8, true, resultObs8a, rv => rv.key)
    val result8 = resultObs8.toListL.runAsync.futureValue
    result8.size shouldEqual 1
    result8(0).key shouldEqual noKey
    compareIter(result8(0).rows.map(_.getDouble(1)), Seq(Double.NaN, 0.27555555555556d).iterator)

    // Stddev
    val agg9 = RowAggregator(AggregationOperator.Stddev, Nil, tvSchema)
    val resultObs9a = RangeVectorAggregator.mapReduce(agg9, false, Observable.fromIterable(samples), noGrouping)
    val resultObs9 = RangeVectorAggregator.mapReduce(agg9, true, resultObs9a, rv => rv.key)
    val result9 = resultObs9.toListL.runAsync.futureValue
    result9.size shouldEqual 1
    result9(0).key shouldEqual noKey
    compareIter(result9(0).rows.map(_.getDouble(1)), Seq(Double.NaN, 0.52493385826745d).iterator)
  }

  it("topK should not have any trailing value ") {

    // The value before NaN should not get carried over. Topk result for timestamp 1556744173L should have Double.NaN
    val samples: Array[RangeVector] = Array(
      toRv(Seq((1556744143L, 42d), (1556744158L, 42d),(1556744173L, Double.NaN)))
    )

    val agg6 = RowAggregator(AggregationOperator.TopK, Seq(5.0), tvSchema)
    val resultObs6a = RangeVectorAggregator.mapReduce(agg6, false, Observable.fromIterable(samples), noGrouping)
    val resultObs6 = RangeVectorAggregator.mapReduce(agg6, true, resultObs6a, rv=>rv
      .key)
    val resultObs6b = RangeVectorAggregator.present(agg6, resultObs6, 1000)
    val result6 = resultObs6.toListL.runAsync.futureValue
    result6(0).key shouldEqual noKey
    val result6b = resultObs6b.toListL.runAsync.futureValue
    result6b.size shouldEqual 1
    result6b(0).key shouldEqual ignoreKey
    compareIter(result6b(0).rows.map(_.getDouble(1)), Seq(42d,42d).iterator)
  }

  import filodb.memory.format.{vectors => bv}

  it("should sum histogram RVs") {
    val (data1, rv1) = histogramRV(numSamples = 5)
    val (data2, rv2) = histogramRV(numSamples = 5)
    val samples: Array[RangeVector] = Array(rv1, rv2)

    val agg1 = RowAggregator(AggregationOperator.Sum, Nil, histSchema)
    val resultObs1 = RangeVectorAggregator.mapReduce(agg1, false, Observable.fromIterable(samples), noGrouping)
    val resultObs = RangeVectorAggregator.mapReduce(agg1, true, resultObs1, rv=>rv.key)

    val result = resultObs.toListL.runAsync.futureValue
    result.size shouldEqual 1
    result(0).key shouldEqual noKey

    val sums = data1.zip(data2).map { case (row1, row2) =>
      val h1 = bv.MutableHistogram(row1(3).asInstanceOf[bv.LongHistogram])
      h1.add(row2(3).asInstanceOf[bv.LongHistogram])
      h1
    }.toList

    result(0).rows.map(_.getHistogram(1)).toList shouldEqual sums

    // Test mapReduce of empty histogram sums
    val agg2 = RowAggregator(AggregationOperator.Sum, Nil, histSchema)
    val emptyObs = RangeVectorAggregator.mapReduce(agg2, false, Observable.empty, noGrouping)
    val resultObs2 = RangeVectorAggregator.mapReduce(agg2, true, emptyObs ++ resultObs1, rv=>rv.key)
    val result2 = resultObs2.toListL.runAsync.futureValue
    result2.size shouldEqual 1
    result2(0).key shouldEqual noKey
  }

  it("should count histogram RVs") {
    val (data1, rv1) = histogramRV(numSamples = 5)
    val (data2, rv2) = histogramRV(numSamples = 5)
    val samples: Array[RangeVector] = Array(rv1, rv2)

    val agg1 = RowAggregator(AggregationOperator.Count, Nil, histSchema)
    val resultObs1 = RangeVectorAggregator.mapReduce(agg1, false, Observable.fromIterable(samples), noGrouping)
    val resultObs = RangeVectorAggregator.mapReduce(agg1, true, resultObs1, rv=>rv.key)

    val result = resultObs.toListL.runAsync.futureValue
    result.size shouldEqual 1
    result(0).key shouldEqual noKey

    val counts = data1.map(_ => 2).toList
    result(0).rows.map(_.getDouble(1)).toList shouldEqual counts
  }

  it("should sum and compute max of histogram & max RVs") {
    val (data1, rv1) = MMD.histMaxRV(100000L, numSamples = 5)
    val (data2, rv2) = MMD.histMaxRV(100000L, numSamples = 5)
    val samples: Array[RangeVector] = Array(rv1, rv2)

    val agg1 = RowAggregator(AggregationOperator.Sum, Nil, histMaxSchema)
    val resultObs1 = RangeVectorAggregator.mapReduce(agg1, false, Observable.fromIterable(samples), noGrouping)
    val resultObs = RangeVectorAggregator.mapReduce(agg1, true, resultObs1, rv=>rv.key)

    val result = resultObs.toListL.runAsync.futureValue
    result.size shouldEqual 1
    result(0).key shouldEqual noKey

    val sums = data1.zip(data2).map { case (row1, row2) =>
      val h1 = bv.MutableHistogram(row1(4).asInstanceOf[bv.LongHistogram])
      h1.add(row2(4).asInstanceOf[bv.LongHistogram])
      h1
    }.toList

    val maxes = data1.zip(data2).map { case (row1, row2) =>
      Math.max(row1(3).asInstanceOf[Double], row2(3).asInstanceOf[Double])
    }.toList

    val answers = result(0).rows.map(r => (r.getHistogram(1), r.getDouble(2))).toList
    answers.map(_._1) shouldEqual sums
    answers.map(_._2) shouldEqual maxes
  }

  it ("should work for countValues") {
    val expectedLabels = List(Map("freq".utf8 -> "5.6".utf8), Map("freq".utf8 -> "5.1".utf8),
      Map(("freq").utf8 -> "4.4".utf8), Map("freq".utf8 -> "2.0".utf8))
    val expectedRows = List((2,2.0), (1,1.0), (2,1.0), (1,1.0))
    val samples: Array[RangeVector] = Array(
      toRv(Seq((1L,5.1), (2L, 5.6d))),
      toRv(Seq((1L, 2), (2L, 4.4d))),
      toRv(Seq((1L, Double.NaN), (2L, 5.6d)))
    )

    val agg = RowAggregator(AggregationOperator.CountValues, Seq("freq"), tvSchema)
    val resultObs = RangeVectorAggregator.mapReduce(agg, false, Observable.fromIterable(samples), noGrouping)
    val resultObs1 = RangeVectorAggregator.mapReduce(agg, true, resultObs,  rv=>rv.key)

    val resultObs2 = RangeVectorAggregator.present(agg, resultObs1, 1000)
    val result = resultObs2.toListL.runAsync.futureValue
    result.size.shouldEqual(4)
    result.map(_.key.labelValues).sameElements(expectedLabels) shouldEqual true
    result.flatMap(_.rows.map(x => (x.getLong(0), x.getDouble(1))).toList).sameElements(expectedRows) shouldEqual true

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
