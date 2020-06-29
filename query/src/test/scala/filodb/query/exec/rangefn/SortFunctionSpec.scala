package filodb.query.exec.rangefn

import com.typesafe.config.{Config, ConfigFactory}

import filodb.core.MetricsTestData
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.query.exec.RangeVectorAggregator
import filodb.query.exec.aggregator.RowAggregator
import filodb.query.{exec, AggregationOperator, SortFunctionId}

import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}

class SortFunctionSpec extends FunSpec with Matchers with ScalaFutures {
  val config: Config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val resultSchema = ResultSchema(MetricsTestData.timeseriesSchema.infosFromIDs(0 to 1), 1)
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)
  val ignoreKey = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))


  val testKey1 = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-1"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("destination-value-1")))

  val testKey2 = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-2"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("destination-value-2")))

  val testSample: Array[RangeVector] = Array(
    new RangeVector {
      override def key: RangeVectorKey = testKey1

      import filodb.core.query.NoCloseIterator._
      override def rows(): CloseableIterator[RowReader] = Seq(
        new TransientRow(1L, 1d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey2

      import filodb.core.query.NoCloseIterator._
      override def rows(): CloseableIterator[RowReader] = Seq(
        new TransientRow(1L, 5d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey1

      import filodb.core.query.NoCloseIterator._
      override def rows(): CloseableIterator[RowReader] = Seq(
        new TransientRow(1L, 3d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey1

      import filodb.core.query.NoCloseIterator._
      override def rows(): CloseableIterator[RowReader] = Seq(
        new TransientRow(1L, 2d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey2

      import filodb.core.query.NoCloseIterator._
      override def rows(): CloseableIterator[RowReader] = Seq(
        new TransientRow(1L, 4d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey2

      import filodb.core.query.NoCloseIterator._
      override def rows(): CloseableIterator[RowReader] = Seq(
        new TransientRow(1L, 6d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey1

      import filodb.core.query.NoCloseIterator._
      override def rows(): CloseableIterator[RowReader] = Seq(
        new TransientRow(1L, 0d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey1

      import filodb.core.query.NoCloseIterator._
      override def rows(): CloseableIterator[RowReader] = Seq.empty[RowReader].iterator
    }
    )

  val emptySample: Array[RangeVector] = Array.empty[RangeVector]

  it("should sort instant vectors in ascending order") {
    val sortFunctionMapper = exec.SortFunctionMapper(SortFunctionId.Sort)
    val resultObs = sortFunctionMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    val resultRows = resultObs.toListL.runAsync.futureValue.flatMap(_.rows.map(_.getDouble(1)).toList)
    resultRows.shouldEqual(List(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
  }

  it("should sort instant vectors in descending order") {
    val sortFunctionMapper = exec.SortFunctionMapper(SortFunctionId.SortDesc)
    val resultObs = sortFunctionMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    val resultRows = resultObs.toListL.runAsync.futureValue.flatMap(_.rows.map(_.getDouble(1)).toList)
    resultRows.shouldEqual(List(6.0, 5.0, 4.0, 3.0, 2.0, 1.0, 0.0))
  }

  it("should return empty rangeVector when sorting empty sample") {
    val sortFunctionMapper = exec.SortFunctionMapper(SortFunctionId.Sort)
    val resultObs = sortFunctionMapper(Observable.fromIterable(emptySample), querySession, 1000, resultSchema, Nil)
    val resultRows = resultObs.toListL.runAsync.futureValue.flatMap(_.rows.map(_.getDouble(1)).toList)
    resultRows.isEmpty shouldEqual(true)
  }

  it("sort should work with aggregate with grouping") {
   val tvSchema = ResultSchema(Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
     ColumnInfo("value", ColumnType.DoubleColumn)), 1)

    val testSample: Array[RangeVector] = Array(
      new RangeVector {
        override def key: RangeVectorKey = testKey1

        import filodb.core.query.NoCloseIterator._
        override def rows(): CloseableIterator[RowReader] = Seq(
          new TransientRow(1L, 1d)).iterator
      },
      new RangeVector {
        override def key: RangeVectorKey = testKey2

        import filodb.core.query.NoCloseIterator._
        override def rows(): CloseableIterator[RowReader] = Seq(
          new TransientRow(1L, 5d)).iterator
      })

    val byLabels = List("src")

    def grouping(rv: RangeVector): RangeVectorKey = {
      val groupBy: Map[ZeroCopyUTF8String, ZeroCopyUTF8String] =
        if (byLabels.nonEmpty) rv.key.labelValues.filter(lv => byLabels.contains(lv._1.toString))
        else Map.empty
      CustomRangeVectorKey(groupBy)
    }
   val agg = RowAggregator(AggregationOperator.Sum, Nil, tvSchema)
   val resultObs1 = RangeVectorAggregator.mapReduce(agg, false, Observable.fromIterable(testSample), grouping)
   val resultObs2 = RangeVectorAggregator.mapReduce(agg, true, resultObs1, grouping)
   val resultAgg = resultObs2.toListL.runAsync.futureValue
   resultAgg.size shouldEqual 2
   resultAgg.flatMap(_.rows.map(_.getDouble(1)).toList) shouldEqual(List(5.0, 1.0))
   
   val sortFunctionMapper = exec.SortFunctionMapper(SortFunctionId.Sort)
   val resultObs = sortFunctionMapper(resultObs2, querySession, 1000, resultSchema, Nil)
   val resultRows = resultObs.toListL.runAsync.futureValue.flatMap(_.rows.map(_.getDouble(1)).toList)
   resultRows.shouldEqual(List(1.0, 5.0))
 }
}
