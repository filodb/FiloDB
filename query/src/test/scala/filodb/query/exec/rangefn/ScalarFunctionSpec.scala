package filodb.query.exec.rangefn

import com.typesafe.config.{Config, ConfigFactory}
import filodb.core.MetricsTestData
import filodb.core.query.{CustomRangeVectorKey, RangeVector, RangeVectorKey, ResultSchema, ScalarFixedDouble, ScalarVaryingDouble, TransientRow}
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.query.{QueryConfig, ScalarFunctionId, TimeStepParams, exec}
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}

class ScalarFunctionSpec extends FunSpec with Matchers with ScalaFutures {
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
        new TransientRow(1L, 3d),
        new TransientRow(2L, 3d),
        new TransientRow(3L, 3d)).iterator
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

  val oneSample: Array[RangeVector] = Array(
    new RangeVector {
      override def key: RangeVectorKey = testKey1

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 1d),
        new TransientRow(2L, 10d),
        new TransientRow(3L, 30d)
      ).iterator
    })


  it("should generate scalar") {
    val scalarFunctionMapper = exec.ScalarFunctionMapper(ScalarFunctionId.Scalar, TimeStepParams(1,1,1) )
    val resultObs = scalarFunctionMapper(Observable.fromIterable(testSample), queryConfig, 1000, resultSchema)
    val resultRangeVectors = resultObs.toListL.runAsync.futureValue
    resultRangeVectors.forall(x => x.isInstanceOf[ScalarFixedDouble]) shouldEqual (true)
    val resultRows = resultRangeVectors.flatMap(_.rows.map(_.getDouble(1)).toList)
    resultRows.size shouldEqual (1)
    resultRows.head.isNaN shouldEqual true
  }

  it("should generate scalar values when there is one range vector") {
    val scalarFunctionMapper = exec.ScalarFunctionMapper(ScalarFunctionId.Scalar, TimeStepParams(1,1,1) )
    val resultObs = scalarFunctionMapper(Observable.fromIterable(oneSample), queryConfig, 1000, resultSchema)
    val resultRangeVectors = resultObs.toListL.runAsync.futureValue
    resultRangeVectors.forall(x => x.isInstanceOf[ScalarVaryingDouble]) shouldEqual (true)
    //resultRangeVectors.foreach(_.asInstanceOf[ScalarVaryingDouble]) shouldEqual(true)
    val resultRows = resultRangeVectors.flatMap(_.rows.map(_.getDouble(1)).toList)
    resultRows.shouldEqual(List(1, 10, 30))
  }
}
