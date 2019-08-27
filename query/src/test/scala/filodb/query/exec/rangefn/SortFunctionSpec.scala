package filodb.query.exec.rangefn

import com.typesafe.config.{Config, ConfigFactory}
import filodb.core.MetricsTestData
import filodb.core.query.{CustomRangeVectorKey, RangeVector, RangeVectorKey, ResultSchema}
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.query.{MiscellaneousFunctionId, QueryConfig, exec}
import filodb.query.exec.TransientRow
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

class SortFunctionSpec extends FunSpec with Matchers with ScalaFutures {
  val config: Config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val resultSchema = ResultSchema(MetricsTestData.timeseriesDataset.infosFromIDs(0 to 1), 1)
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
        new TransientRow(1L, 3.3d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey2

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 100d)).iterator
    })

it ("should sort instant vectors in ascending order")
  {
    val miscellaneousFunctionMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.Sort)
    val resultObs = miscellaneousFunctionMapper(MetricsTestData.timeseriesDataset,
      Observable.fromIterable(testSample), queryConfig, 1000, resultSchema)
    //val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

    println("resultRows:" + resultRows)
   //resultLabelValues.sameElements(expectedLabels) shouldEqual true
  }
}
