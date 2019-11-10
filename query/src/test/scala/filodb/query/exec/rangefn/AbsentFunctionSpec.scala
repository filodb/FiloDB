
package filodb.query.exec.rangefn

import com.typesafe.config.{Config, ConfigFactory}
import filodb.core.MetricsTestData
import filodb.core.query.Filter.{Equals, NotEqualsRegex}
import filodb.core.query.{ColumnFilter, CustomRangeVectorKey, RangeParams, RangeVector, RangeVectorKey, ResultSchema}
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.query.exec.TransientRow
import filodb.query.{QueryConfig, exec}
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}

class AbsentFunctionSpec extends FunSpec with Matchers with ScalaFutures {
  val config: Config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val resultSchema = ResultSchema(MetricsTestData.timeseriesSchema.infosFromIDs(0 to 1), 1)
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val ignoreKey = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))


  val testKey = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("metric") -> ZeroCopyUTF8String("test"),
      ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("destination-value")))

  val emptySample: Seq[RangeVector] = Seq.empty[RangeVector]

  val testSample: Array[RangeVector] = Array(
    new RangeVector {
      override def key: RangeVectorKey = testKey

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 1d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 5d)).iterator
    })

  it("should generate range vector for empty Sample") {
    val columnFilter = Seq(ColumnFilter("host", Equals("host1")), ColumnFilter("instance", Equals("instance1")))
    val expectedKeys = Map(ZeroCopyUTF8String("host") -> ZeroCopyUTF8String("host1"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("instance1"))
    val expectedRows = List(1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    val absentFunctionMapper = exec.AbsentFunctionMapper(columnFilter, RangeParams(1000, 20, 1100), "metric")
    val resultObs = absentFunctionMapper(Observable.fromIterable(emptySample), queryConfig, 1000, resultSchema)
    val result = resultObs.toListL.runAsync.futureValue
    result.size shouldEqual (1)
    val keys = result.map(_.key.labelValues)
    val rows = result.flatMap(_.rows.map(_.getDouble(1)).toList)
    keys.head shouldEqual expectedKeys
    rows shouldEqual expectedRows
  }

  it("should not generate range vector when sample is present") {
    val columnFilter = Seq(ColumnFilter("host", Equals("host1")), ColumnFilter("instance", Equals("instance1")))
    val absentFunctionMapper = exec.AbsentFunctionMapper(columnFilter, RangeParams(1000, 20, 1060), "metric")
    val resultObs = absentFunctionMapper(Observable.fromIterable(testSample), queryConfig, 1000, resultSchema)
    val result = resultObs.toListL.runAsync.futureValue
    result.isEmpty shouldEqual (true)
    val keys = result.map(_.key.labelValues)
    val rows = result.flatMap(_.rows.map(_.getDouble(1)).toList)
  }

  it("should not have keys Filter is not Equals") {
    val columnFilter = Seq(ColumnFilter("host", NotEqualsRegex("host1")))
    val expectedRows = List(1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    val absentFunctionMapper = exec.AbsentFunctionMapper(columnFilter, RangeParams(1000, 20, 1100), "metric")
    val resultObs = absentFunctionMapper(Observable.fromIterable(emptySample), queryConfig, 1000, resultSchema)
    val result = resultObs.toListL.runAsync.futureValue
    result.size shouldEqual (1)
    val keys = result.map(_.key.labelValues)
    val rows = result.flatMap(_.rows.map(_.getDouble(1)).toList)
    keys.head.isEmpty shouldEqual true
    rows shouldEqual expectedRows
  }

  it("should not have keys when ColumnFilter is MetricName") {
    val columnFilter = Seq(ColumnFilter("metric", Equals("http_requests")))
    val expectedKeys = Map(ZeroCopyUTF8String("host") -> ZeroCopyUTF8String("host1"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("instance1"))
    val expectedRows = List(1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    val absentFunctionMapper = exec.AbsentFunctionMapper(columnFilter, RangeParams(1000, 20, 1100), "metric")
    val resultObs = absentFunctionMapper(Observable.fromIterable(emptySample), queryConfig, 1000, resultSchema)
    val result = resultObs.toListL.runAsync.futureValue
    result.size shouldEqual (1)
    val keys = result.map(_.key.labelValues)
    val rows = result.flatMap(_.rows.map(_.getDouble(1)).toList)
    keys.head.isEmpty shouldEqual true
  }
}
