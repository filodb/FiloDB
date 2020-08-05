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

class LableJoinSpec extends AnyFunSpec with Matchers with ScalaFutures {
  val config: Config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val resultSchema = ResultSchema(MetricsTestData.timeseriesSchema.infosFromIDs(0 to 1), 1)
  val ignoreKey = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))

  val testKey1 = CustomRangeVectorKey(
    Map(
      ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value"),
      ZeroCopyUTF8String("src1") -> ZeroCopyUTF8String("source-value-1"),
      ZeroCopyUTF8String("src2") -> ZeroCopyUTF8String("source-value-2"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("dst-value")
    )
  )

  val testKey2 = CustomRangeVectorKey(
    Map(
      ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("src-value"),
      ZeroCopyUTF8String("src1") -> ZeroCopyUTF8String("src1-value"),
      ZeroCopyUTF8String("src2") -> ZeroCopyUTF8String("src2-value"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("original-dst-value")
    )
  )

  val testKey3 = CustomRangeVectorKey(
    Map(
      ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("src-value"),
      ZeroCopyUTF8String("src1") -> ZeroCopyUTF8String("src1-value"),
      ZeroCopyUTF8String("src2") -> ZeroCopyUTF8String("src2-value")
    )
  )

  val testKey4 = CustomRangeVectorKey(
    Map(
      ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("src-value"),
      ZeroCopyUTF8String("src1") -> ZeroCopyUTF8String("src1-value"),
      ZeroCopyUTF8String("src2") -> ZeroCopyUTF8String("src2-value")
    )
  )

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

  val sampleWithoutDst: Array[RangeVector] = Array(
    new RangeVector {
      override def key: RangeVectorKey = testKey3

      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 3.3d),
        new TransientRow(2L, 5.1d)).iterator
    },
    new RangeVector {
      override def key: RangeVectorKey = testKey4

      import filodb.core.query.NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(3L, 100d),
        new TransientRow(4L, 200d)).iterator
    })

  val queryConfig = new QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)

  it("label_join joins all src values in order") {

    val expectedLabels = List(Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value"),
      ZeroCopyUTF8String("src1") -> ZeroCopyUTF8String("source-value-1"),
      ZeroCopyUTF8String("src2") -> ZeroCopyUTF8String("source-value-2"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("source-value-source-value-1-source-value-2")
    ),
      Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("src-value"),
        ZeroCopyUTF8String("src1") -> ZeroCopyUTF8String("src1-value"),
        ZeroCopyUTF8String("src2") -> ZeroCopyUTF8String("src2-value"),
        ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("src-value-src1-value-src2-value"))
    )

    val funcParams = Seq("dst", "-", "src", "src1", "src2")
    val labelVectorFnMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelJoin, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

    resultLabelValues.sameElements(expectedLabels) shouldEqual true

    //label_join should not change rows
    testSample.map(_.rows.map(_.getDouble(1))).zip(resultRows).foreach {
      case (ex, res) => {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 shouldEqual val2
        }
      }
    }
  }
  it("label_join should treat label which is not present as empty string ") {

    val expectedLabels = List(Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value"),
      ZeroCopyUTF8String("src1") -> ZeroCopyUTF8String("source-value-1"),
      ZeroCopyUTF8String("src2") -> ZeroCopyUTF8String("source-value-2"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("source-value--source-value-1")
    ),
      Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("src-value"),
        ZeroCopyUTF8String("src1") -> ZeroCopyUTF8String("src1-value"),
        ZeroCopyUTF8String("src2") -> ZeroCopyUTF8String("src2-value"),
        ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("src-value--src1-value"))
    )

    val funcParams = Seq("dst", "-", "src", "src3", "src1")
    val labelVectorFnMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelJoin, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

    resultLabelValues.sameElements(expectedLabels) shouldEqual true

    //label_join should not change rows
    testSample.map(_.rows.map(_.getDouble(1))).zip(resultRows).foreach {
      case (ex, res) => {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 shouldEqual val2
        }
      }
    }
  }

  it("label_join should overwrite destination label even if resulting dst label is empty string") {

    val expectedLabels = List(Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value"),
      ZeroCopyUTF8String("src1") -> ZeroCopyUTF8String("source-value-1"),
      ZeroCopyUTF8String("src2") -> ZeroCopyUTF8String("source-value-2")
    ),
      Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("src-value"),
        ZeroCopyUTF8String("src1") -> ZeroCopyUTF8String("src1-value"),
        ZeroCopyUTF8String("src2") -> ZeroCopyUTF8String("src2-value")
      ))

    val funcParams = Seq("dst", "", "emptysrc", "emptysrc1", "emptysrc2")
    val labelVectorFnMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelJoin, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

    resultLabelValues.sameElements(expectedLabels) shouldEqual true

    //label_join should not change rows
    testSample.map(_.rows.map(_.getDouble(1))).zip(resultRows).foreach {
      case (ex, res) => {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 shouldEqual val2
        }
      }
    }
  }
  it("label_join should create destination label if it is not present") {

    val expectedLabels = List(Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value"),
      ZeroCopyUTF8String("src1") -> ZeroCopyUTF8String("source-value-1"),
      ZeroCopyUTF8String("src2") -> ZeroCopyUTF8String("source-value-2"),
      ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("source-value-source-value-1-source-value-2")
    ),
      Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("src-value"),
        ZeroCopyUTF8String("src1") -> ZeroCopyUTF8String("src1-value"),
        ZeroCopyUTF8String("src2") -> ZeroCopyUTF8String("src2-value"),
        ZeroCopyUTF8String("dst") -> ZeroCopyUTF8String("src-value-src1-value-src2-value"))
    )

    val funcParams = Seq("dst", "-", "src", "src1", "src2")
    val labelVectorFnMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelJoin, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

    resultLabelValues.sameElements(expectedLabels) shouldEqual true

    //label_join should not change rows
    testSample.map(_.rows.map(_.getDouble(1))).zip(resultRows).foreach {
      case (ex, res) => {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 shouldEqual val2
        }
      }
    }
  }

  it("label_join should remove destination label if source labels are not specified") {

    val expectedLabels = List(Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("source-value"),
      ZeroCopyUTF8String("src1") -> ZeroCopyUTF8String("source-value-1"),
      ZeroCopyUTF8String("src2") -> ZeroCopyUTF8String("source-value-2")
    ),
      Map(ZeroCopyUTF8String("src") -> ZeroCopyUTF8String("src-value"),
        ZeroCopyUTF8String("src1") -> ZeroCopyUTF8String("src1-value"),
        ZeroCopyUTF8String("src2") -> ZeroCopyUTF8String("src2-value")
      ))

    val funcParams = Seq("dst", "-")
    val labelVectorFnMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelJoin, funcParams)
    val resultObs = labelVectorFnMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    val resultLabelValues = resultObs.toListL.runAsync.futureValue.map(_.key.labelValues)
    val resultRows = resultObs.toListL.runAsync.futureValue.map(_.rows.map(_.getDouble(1)))

    resultLabelValues.sameElements(expectedLabels) shouldEqual true

    //label_join should not change rows
    testSample.map(_.rows.map(_.getDouble(1))).zip(resultRows).foreach {
      case (ex, res) => {
        ex.zip(res).foreach {
          case (val1, val2) =>
            val1 shouldEqual val2
        }
      }
    }
  }

  it("should validate invalid function params") {

    val funcParams1 = Seq("dst", "-", "src$", "src1", "src2")
    val funcParams2 = Seq("dst#", "-", "src", "src1", "src2")

    the[IllegalArgumentException] thrownBy {
      val miscellaneousFunctionMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelJoin,
        funcParams1)
      miscellaneousFunctionMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    } should have message "requirement failed: Invalid source label name in label_join()"

    the[IllegalArgumentException] thrownBy {
      val miscellaneousFunctionMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelJoin,
        funcParams2)
      miscellaneousFunctionMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    } should have message "requirement failed: Invalid destination label name in label_join()"

    the[IllegalArgumentException] thrownBy {
      val miscellaneousFunctionMapper = exec.MiscellaneousFunctionMapper(MiscellaneousFunctionId.LabelJoin,
        Seq("dst"))
      miscellaneousFunctionMapper(Observable.fromIterable(testSample), querySession, 1000, resultSchema, Nil)
    } should have message "requirement failed: expected at least 3 argument(s) in call to label_join"
  }
}
