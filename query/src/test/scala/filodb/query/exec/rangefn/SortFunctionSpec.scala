package filodb.query.exec.rangefn

import com.typesafe.config.{Config, ConfigFactory}
import filodb.core.MetricsTestData
import filodb.core.query.{CustomRangeVectorKey, RangeVector, RangeVectorKey, ResultSchema, TransientRow}
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}

import filodb.query.{QueryConfig, SortFunctionId, exec}
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}

class SortFunctionSpec extends FunSpec with Matchers with ScalaFutures {
  val config: Config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val resultSchema = ResultSchema(MetricsTestData.timeseriesSchema.infosFromIDs(0 to 1), 1)
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val ignoreKey = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))


  val testKey1 = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-10"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("original-destination-value")))

  val testKey2 = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-20"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("original-destination-value")))

  val testSample: Array[RangeVector] = Array(
    new RangeVector {
      override def key: RangeVectorKey = testKey1

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 1d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey2

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 5d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey1

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 3d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey1

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 2d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey2

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 4d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey2

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 6d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey1

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 0d)).iterator
    })

  it("should sort instant vectors in ascending order") {
    val sortFunctionMapper = exec.SortFunctionMapper(SortFunctionId.Sort)
    val resultObs = sortFunctionMapper(Observable.fromIterable(testSample), queryConfig, 1000, resultSchema, Nil)
    val resultRows = resultObs.toListL.runAsync.futureValue.flatMap(_.rows.map(_.getDouble(1)).toList)
    resultRows.shouldEqual(List(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
  }

  it("should sort instant vectors in descending order") {
    val sortFunctionMapper = exec.SortFunctionMapper(SortFunctionId.SortDesc)
    val resultObs = sortFunctionMapper(Observable.fromIterable(testSample), queryConfig, 1000, resultSchema, Nil)
    val resultRows = resultObs.toListL.runAsync.futureValue.flatMap(_.rows.map(_.getDouble(1)).toList)
    resultRows.shouldEqual(List(6.0, 5.0, 4.0, 3.0, 2.0, 1.0, 0.0))
  }
}
