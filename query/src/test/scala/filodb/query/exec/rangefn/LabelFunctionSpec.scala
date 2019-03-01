package filodb.query.exec.rangefn

import com.typesafe.config.{Config, ConfigFactory}
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.core.MetricsTestData
import filodb.core.query.{CustomRangeVectorKey, RangeVector, RangeVectorKey, ResultSchema}
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.query._
import filodb.query.exec.TransientRow

class LabelFunctionSpec extends FunSpec with Matchers with ScalaFutures {

  val config: Config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val resultSchema = ResultSchema(MetricsTestData.timeseriesDataset.infosFromIDs(0 to 1), 1)
  val ignoreKey = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))
  val queryConfig = new QueryConfig(config.getConfig("query"))

  it("should  replace label only when match is found in label replace") {
    val sampleKey1 = CustomRangeVectorKey(
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("demo.io:9090"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("test")))
    val sampleKey2 = CustomRangeVectorKey(
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("demo.io:8080")))

    val sampleWithKey: Array[RangeVector] = Array(
      new RangeVector {
        override def key: RangeVectorKey = sampleKey1

        override def rows: Iterator[RowReader] = Seq(
          new TransientRow(1L, 3.3d),
          new TransientRow(2L, 5.1d)).iterator
      },
      new RangeVector {
        override def key: RangeVectorKey = sampleKey2

        override def rows: Iterator[RowReader] = Seq(
          new TransientRow(3L, 100d),
          new TransientRow(4L, 200d)).iterator
      })

    val expectedLabels = List(Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("demo.io new Label Value 90"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("test")),
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("demo.io:8080"))) // will not be replaced

    val funcParams = Seq("instance", "$1 new Label Value $2", "instance", "(.*):90(.*)")
    val labelVectorFnMapper = exec.LabelFunctionMapper(MiscellaneousFunctionId.LabelReplace, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(sampleWithKey), queryConfig, 1000, resultSchema)
    val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))
    println("resultLabelValues:" + resultLabelValues)

    resultLabelValues.sameElements(expectedLabels) shouldEqual true

    //label_replace should not change rows
    sampleWithKey.map(_.rows.map(_.getDouble(1))).zip(resultRows).foreach {
      case (ex, res) => {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 shouldEqual val2
        }
      }
    }
  }

  it("should add new label when dst_label does not exist") {
    val sampleKey1 = CustomRangeVectorKey(
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("Instance-100"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("test")))

    val sampleWithKey: Array[RangeVector] = Array(
      new RangeVector {
        override def key: RangeVectorKey = sampleKey1

        override def rows: Iterator[RowReader] = Seq(
          new TransientRow(1L, 3.3d),
          new TransientRow(2L, 5.1d)).iterator
      },
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey

        override def rows: Iterator[RowReader] = Seq(
          new TransientRow(3L, 100d),
          new TransientRow(4L, 200d)).iterator
      })

    val expectedLabels = List(Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("Instance-100"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("test"),
      ZeroCopyUTF8String("instanceNew") -> ZeroCopyUTF8String("Instance-10-Instance-10")),
      Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))

    val funcParams = Seq("instanceNew", "$1-$1", "instance", "(.*)\\d")

    val labelVectorFnMapper = exec.LabelFunctionMapper(MiscellaneousFunctionId.LabelReplace, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(sampleWithKey), queryConfig, 1000, resultSchema)
    val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

    resultLabelValues.sameElements(expectedLabels) shouldEqual true

    sampleWithKey.map(_.rows.map(_.getDouble(1))).zip(resultRows).foreach {
      case (ex, res) => {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 shouldEqual val2
        }
      }
    }
  }

  it("should not change sample when entire regex does not match") {
    val sampleKey1 = CustomRangeVectorKey(
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("Instance-9090"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("test")))

    val sampleWithKey: Array[RangeVector] = Array(
      new RangeVector {
        override def key: RangeVectorKey = sampleKey1

        override def rows: Iterator[RowReader] = Seq(
          new TransientRow(1L, 3.3d),
          new TransientRow(2L, 5.1d)).iterator
      },
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey

        override def rows: Iterator[RowReader] = Seq(
          new TransientRow(3L, 100d),
          new TransientRow(4L, 200d)).iterator
      })

    val expectedLabels = sampleWithKey.toList.map(_.key.labelValues)
    val funcParams = Seq("instance", "$1", "instance", "(.*)9")

    val labelVectorFnMapper = exec.LabelFunctionMapper(MiscellaneousFunctionId.LabelReplace, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(sampleWithKey), queryConfig, 1000, resultSchema)
    val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

    resultLabelValues.sameElements(expectedLabels) shouldEqual true

    sampleWithKey.map(_.rows.map(_.getDouble(1))).zip(resultRows).foreach {
      case (ex, res) => {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 shouldEqual val2
        }
      }
    }
  }

}