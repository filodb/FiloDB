
package filodb.query.exec.rangefn

import com.typesafe.config.{Config, ConfigFactory}
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures

import filodb.core.MetricsTestData
import filodb.core.query._
import filodb.core.query.Filter.{Equals, NotEqualsRegex}
import filodb.memory.data.ChunkMap
import filodb.memory.format.ZeroCopyUTF8String
import filodb.query.exec
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class AbsentFunctionSpec extends AnyFunSpec with Matchers with ScalaFutures with BeforeAndAfter {
  after {
    ChunkMap.validateNoSharedLocks(true)
  }

  val config: Config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val resultSchema = ResultSchema(MetricsTestData.timeseriesSchema.infosFromIDs(0 to 1), 1)
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)

  val testKey1 = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("metric") -> ZeroCopyUTF8String("test1"),
      ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("destination-value")))

  val testKey2 = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("metric") -> ZeroCopyUTF8String("test2"),
      ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("destination-value")))

  val emptySample: Seq[RangeVector] = Seq.empty[RangeVector]

  val testSample: Array[RangeVector] = Array(
    new RangeVector {
      override def key: RangeVectorKey = testKey1

      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1000L, 1d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey2

      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1000L, 5d)).iterator
    })

  val testSampleNan: Array[RangeVector] = Array(
    new RangeVector {
      override def key: RangeVectorKey = testKey1

      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1000L, Double.NaN),
        new TransientRow(2000L, 1d),
        new TransientRow(3000L, Double.NaN)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey2

      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1000L, 5d),
        new TransientRow(2000L, Double.NaN),
        new TransientRow(3000L, Double.NaN)).iterator
    })

  it("should generate range vector for empty Sample") {
    val columnFilter = Seq(ColumnFilter("host", Equals("host1")), ColumnFilter("instance", Equals("instance1")))
    val expectedKeys = Map(ZeroCopyUTF8String("host") -> ZeroCopyUTF8String("host1"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("instance1"))
    val expectedRows = List(1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    val absentFunctionMapper = exec.AbsentFunctionMapper(columnFilter, RangeParams(1, 2, 11), "metric")
    val resultObs = absentFunctionMapper(Observable.fromIterable(emptySample), querySession, 1000, resultSchema, Nil)
    val result = resultObs.toListL.runAsync.futureValue
    result.size shouldEqual (1)
    val keys = result.map(_.key.labelValues)
    val rows = result.flatMap(_.rows.map(_.getDouble(1)).toList)
    keys.head shouldEqual expectedKeys
    rows shouldEqual expectedRows
  }

  it("should not generate range vector when sample is present") {
    val columnFilter = Seq(ColumnFilter("host", Equals("host1")), ColumnFilter("instance", Equals("instance1")))
    val absentFunctionMapper = exec.AbsentFunctionMapper(columnFilter, RangeParams(1, 20, 1), "metric")
    val resultObs = absentFunctionMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    val result = resultObs.toListL.runAsync.futureValue
    val keys = result.map(_.key.labelValues)
    val rows = result.flatMap(_.rows.map(_.getDouble(1)).toList)
    rows.isEmpty shouldEqual true
  }

  it("should not have keys Filter is not Equals") {
    val columnFilter = Seq(ColumnFilter("host", NotEqualsRegex("host1")))
    val expectedRows = List(1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    val absentFunctionMapper = exec.AbsentFunctionMapper(columnFilter, RangeParams(1, 2, 11), "metric")
    val resultObs = absentFunctionMapper(Observable.fromIterable(emptySample), querySession, 1000, resultSchema, Nil)
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
    val absentFunctionMapper = exec.AbsentFunctionMapper(columnFilter, RangeParams(1, 2, 11), "metric")
    val resultObs = absentFunctionMapper(Observable.fromIterable(emptySample), querySession, 1000, resultSchema, Nil)
    val result = resultObs.toListL.runAsync.futureValue
    result.size shouldEqual (1)
    val keys = result.map(_.key.labelValues)
    val rows = result.flatMap(_.rows.map(_.getDouble(1)).toList)
    keys.head.isEmpty shouldEqual true
  }

  it("should generate range vector for Sample with NaN") {
    val columnFilter = Seq(ColumnFilter("host", Equals("host1")), ColumnFilter("instance", Equals("instance1")))
    val expectedKeys = Map(ZeroCopyUTF8String("host") -> ZeroCopyUTF8String("host1"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("instance1"))
    val expectedRows = List((3000, 1.0))
    val absentFunctionMapper = exec.AbsentFunctionMapper(columnFilter, RangeParams(1, 1, 3), "metric")
    val resultObs = absentFunctionMapper(Observable.fromIterable(testSampleNan), querySession, 1000, resultSchema, Nil)
    val result = resultObs.toListL.runAsync.futureValue
    result.size shouldEqual (1)
    val keys = result.map(_.key.labelValues)
    val rows = result.flatMap(_.rows.map(x => (x.getLong(0), x.getDouble(1))).toList)
    keys.head shouldEqual expectedKeys
    rows shouldEqual expectedRows
  }

  it("should not have keys when ColumnFilter is empty") {
    val columnFilter = Seq.empty[ColumnFilter]
    val expectedRows = List(1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    val absentFunctionMapper = exec.AbsentFunctionMapper(columnFilter, RangeParams(1, 2, 11), "metric")
    val resultObs = absentFunctionMapper(Observable.fromIterable(emptySample), querySession, 1000, resultSchema, Nil)
    val result = resultObs.toListL.runAsync.futureValue
    result.size shouldEqual (1)
    val keys = result.map(_.key.labelValues)
    val rows = result.flatMap(_.rows.map(_.getDouble(1)).toList)
    keys.head.isEmpty shouldEqual true
    rows shouldEqual expectedRows
  }

}
