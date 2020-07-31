package filodb.query.exec.rangefn

import com.typesafe.config.{Config, ConfigFactory}
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures

import filodb.core.MetricsTestData
import filodb.core.query._
import filodb.memory.format.ZeroCopyUTF8String
import filodb.query._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class LabelReplaceSpec extends AnyFunSpec with Matchers with ScalaFutures {

  val config: Config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val resultSchema = ResultSchema(MetricsTestData.timeseriesSchema.infosFromIDs(0 to 1), 1)
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

      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 3.3d),
        new TransientRow(2L, 5.1d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey2

      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(3L, 100d),
        new TransientRow(4L, 200d)).iterator
    })

  val queryConfig = new QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)

  it("should replace label only when match is found in label replace") {
    val sampleKey1 = CustomRangeVectorKey(
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("demo.io:9090"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("test")))
    val sampleKey2 = CustomRangeVectorKey(
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("demo.io:8080")))

    val sampleWithKey: Array[RangeVector] = Array(
      new RangeVector {
        override def key: RangeVectorKey = sampleKey1

        import filodb.core.query.NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(1L, 3.3d),
          new TransientRow(2L, 5.1d)).iterator
      },
      new RangeVector {
        override def key: RangeVectorKey = sampleKey2

        import filodb.core.query.NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(3L, 100d),
          new TransientRow(4L, 200d)).iterator
      })

    val expectedLabels = List(Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("demo.io new Label Value 90"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("test")),
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("demo.io:8080"))) // will not be replaced

    val funcParams = Seq("instance", "$1 new Label Value $2", "instance", "(.*):90(.*)")
    val labelVectorFnMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelReplace, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(sampleWithKey), querySession, 1000, resultSchema, Nil)
    val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

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

        import filodb.core.query.NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(1L, 3.3d),
          new TransientRow(2L, 5.1d)).iterator
      },
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey

        import filodb.core.query.NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(3L, 100d),
          new TransientRow(4L, 200d)).iterator
      })

    val expectedLabels = List(Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("Instance-100"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("test"),
      ZeroCopyUTF8String("instanceNew") -> ZeroCopyUTF8String("Instance-10-Instance-10")),
      Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))

    val funcParams = Seq("instanceNew", "$1-$1", "instance", "(.*)\\d")

    val labelVectorFnMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelReplace, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(sampleWithKey), querySession, 1000, resultSchema, Nil)
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

        import filodb.core.query.NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(1L, 3.3d),
          new TransientRow(2L, 5.1d)).iterator
      },
      new RangeVector {
        override def key: RangeVectorKey = ignoreKey

        import filodb.core.query.NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(3L, 100d),
          new TransientRow(4L, 200d)).iterator
      })

    val expectedLabels = sampleWithKey.toList.map(_.key.labelValues)
    val funcParams = Seq("instance", "$1", "instance", "(.*)9")

    val labelVectorFnMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelReplace, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(sampleWithKey), querySession, 1000, resultSchema, Nil)
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

  it("should validate invalid function params") {

    val funcParams = Seq("instance", "$1", "instance", "(.*)9(")

    the[IllegalArgumentException] thrownBy {
      val miscellaneousFunctionMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelReplace,
        funcParams)
      miscellaneousFunctionMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    } should have message "Invalid Regular Expression for label_replace"

    the[IllegalArgumentException] thrownBy {
      val miscellaneousFunctionMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelReplace,
        Seq("instance", "$1"))
      miscellaneousFunctionMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    } should have message "requirement failed: " +
      "Cannot use LabelReplace without function parameters: " +
      "instant-vector, dst_label string, replacement string, src_label string, regex string"

    the[IllegalArgumentException] thrownBy {
      val miscellaneousFunctionMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelReplace,
        Seq("$instance", "$1", "instance", "(.*)9("))
      miscellaneousFunctionMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    } should have message "requirement failed: Invalid destination label name"
  }

  it("should do a full-string match and replace") {

    val expectedLabels = List(Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-10"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("destination-value-10")),
      Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-20"),
        ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("destination-value-20")))

    val funcParams = Seq("dst", "destination-value-$1", "src", "source-value-(.*)")

    val labelVectorFnMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelReplace, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

    resultLabelValues.sameElements(expectedLabels) shouldEqual true

    testSample.map(_.rows.map(_.getDouble(1))).zip(resultRows).foreach {
      case (ex, res) => {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 shouldEqual val2
        }
      }
    }
  }

  it("should not do a sub-string match") {

    val expectedLabels = List(Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-10"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("original-destination-value")),
      Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-20"),
        ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("original-destination-value")))

    val funcParams = Seq("dst", "destination-value-$1", "src", "value-(.*)")

    val labelVectorFnMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelReplace, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

    resultLabelValues.sameElements(expectedLabels) shouldEqual true

    testSample.map(_.rows.map(_.getDouble(1))).zip(resultRows).foreach {
      case (ex, res) => {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 shouldEqual val2
        }
      }
    }
  }

  it("should works with multiple groups and remove groups which do not exist from the replacement string") {

    val expectedLabels = List(Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-10"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("source-value-10 ")),
      Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-20"),
        ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("source-value-20 ")))

    val funcParams = Seq("dst", "$1-value-$2 $3$67", "src", "(.*)-value-(.*)")

    val labelVectorFnMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelReplace, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

    resultLabelValues.sameElements(expectedLabels) shouldEqual true

    testSample.map(_.rows.map(_.getDouble(1))).zip(resultRows).foreach {
      case (ex, res) => {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 shouldEqual val2
        }
      }
    }
  }

  it("should not overwrite the destination label if the source label does not exist") {

    val expectedLabels = List(Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-10"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("original-destination-value")),
      Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-20"),
        ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("original-destination-value")))

    val funcParams = Seq("dst", "value-$1", "nonexistent-src", "source-value-(.*)")

    val labelVectorFnMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelReplace, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

    resultLabelValues.sameElements(expectedLabels) shouldEqual true

    testSample.map(_.rows.map(_.getDouble(1))).zip(resultRows).foreach {
      case (ex, res) => {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 shouldEqual val2
        }
      }
    }
  }

  it("should overwrite destination label if the source label is empty but matched") {

    val expectedLabels = List(Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-10"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("value-")),
      Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-20"),
        ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("value-")))

    val funcParams = Seq("dst", "value-$1", "nonexistent-src", ".*")

    val labelVectorFnMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelReplace, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

    resultLabelValues.sameElements(expectedLabels) shouldEqual true

    testSample.map(_.rows.map(_.getDouble(1))).zip(resultRows).foreach {
      case (ex, res) => {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 shouldEqual val2
        }
      }
    }
  }

  it("should not overwrite the destination label if the source label is not matched") {

    val expectedLabels = List(Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-10"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("original-destination-value")),
      Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-20"),
        ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("original-destination-value")))

    val funcParams = Seq("dst", "value-$1", "src", "dummy-regex")

    val labelVectorFnMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelReplace, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

    resultLabelValues.sameElements(expectedLabels) shouldEqual true

    testSample.map(_.rows.map(_.getDouble(1))).zip(resultRows).foreach {
      case (ex, res) => {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 shouldEqual val2
        }
      }
    }
  }

  it("should drop labels that are set to empty values") {

    val expectedLabels = List(Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-10")),
      Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value-20")))

    val funcParams = Seq("dst", "", "dst", ".*")

    val labelVectorFnMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelReplace, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

    resultLabelValues.sameElements(expectedLabels) shouldEqual true

    testSample.map(_.rows.map(_.getDouble(1))).zip(resultRows).foreach {
      case (ex, res) => {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 shouldEqual val2
        }
      }
    }
  }

  it("should remove duplicated identical output label sets") {

    val expectedLabels = List(Map(ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("original-destination-value")),
      Map(ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("original-destination-value")))

    val funcParams = Seq("src", "", "", "")

    val labelVectorFnMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelReplace, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

    resultLabelValues.sameElements(expectedLabels) shouldEqual true

    testSample.map(_.rows.map(_.getDouble(1))).zip(resultRows).foreach {
      case (ex, res) => {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 shouldEqual val2
        }
      }
    }
  }


}