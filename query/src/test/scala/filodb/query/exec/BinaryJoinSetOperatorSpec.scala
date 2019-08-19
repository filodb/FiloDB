package filodb.query.exec

import scala.concurrent.duration.FiniteDuration
import scala.util.Random
import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import filodb.core.MetricsTestData
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._
import monix.execution.Scheduler


class BinaryJoinSetOperatorSpec extends FunSpec with Matchers with ScalaFutures {

  import SelectRawPartitionsExecSpec._

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val tvSchema = ResultSchema(Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
    ColumnInfo("value", ColumnType.DoubleColumn)), 1)
  val schema = Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
    ColumnInfo("value", ColumnType.DoubleColumn))

  val rand = new Random()
  val error = 0.00000001d
  val noKey = CustomRangeVectorKey(Map.empty)

  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlan)
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Task[QueryResponse] = ???
  }
  val resultSchema = ResultSchema(MetricsTestData.timeseriesDataset.infosFromIDs(0 to 1), 1)

  val sampleHttpRequests: Array[RangeVector] = Array(
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"api-server".utf8,
          "instance".utf8 -> "0".utf8,
          "group".utf8 -> s"production".utf8)
      )

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 100)).iterator
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"api-server".utf8,
          "instance".utf8 -> "1".utf8,
          "group".utf8 -> s"production".utf8)
      )

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 200)).iterator
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"api-server".utf8,
          "instance".utf8 -> "0".utf8,
          "group".utf8 -> s"canary".utf8)
      )

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 300)).iterator
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"api-server".utf8,
          "instance".utf8 -> "1".utf8,
          "group".utf8 -> s"canary".utf8)
      )

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 400)).iterator
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"app-server".utf8,
          "instance".utf8 -> "0".utf8,
          "group".utf8 -> s"production".utf8)
      )

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 500)).iterator
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"app-server".utf8,
          "instance".utf8 -> "1".utf8,
          "group".utf8 -> s"production".utf8)
      )

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 600)).iterator
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"app-server".utf8,
          "instance".utf8 -> "0".utf8,
          "group".utf8 -> s"canary".utf8)
      )

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 700)).iterator
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"app-server".utf8,
          "instance".utf8 -> "1".utf8,
          "group".utf8 -> s"canary".utf8)
      )

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 800)).iterator
    }
  )
  val sampleNoKey: Array[RangeVector] = Array(
    new RangeVector {
      override def key: RangeVectorKey = noKey

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 1)).iterator
    }
  )

  val sampleVectorMatching: Array[RangeVector] = Array(
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"vector_matching_a".utf8,
          "l".utf8 -> "x".utf8)
      )

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 100)).iterator
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"vector_matching_a".utf8,
          "l".utf8 -> "y".utf8)
      )

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 200)).iterator
    }
  )
  val sampleCanary = sampleHttpRequests.filter(_.key.labelValues.get(ZeroCopyUTF8String("group")).get.
    toString.equals("canary"))
  val sampleProduction = sampleHttpRequests.filter(_.key.labelValues.get(ZeroCopyUTF8String("group")).get.
    toString.equals("production"))
  val sampleInstance0 = sampleHttpRequests.filter(_.key.labelValues.get(ZeroCopyUTF8String("instance")).get.
    toString.equals("0"))
  val sampleInstance1 = sampleHttpRequests.filter(_.key.labelValues.get(ZeroCopyUTF8String("instance")).get.
    toString.equals("1"))
  val sampleProductionInstance0 = sampleInstance0.filter(_.key.labelValues.get(ZeroCopyUTF8String("group")).get.
    toString.equals("production"))

  val scalarOpMapper = exec.ScalarOperationMapper(BinaryOperator.ADD, 1.0, false)

  it("should join many-to-many with and") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleInstance0.toList)
    val execPlan = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Nil, Nil)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleCanary.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue


    val expectedLabels = List(Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
      ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
    ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ))

    result.size shouldEqual 2
    result.map(_.key.labelValues) sameElements (expectedLabels) shouldEqual true
    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(300)
    result(1).rows.map(_.getDouble(1)).toList shouldEqual List(700)
  }

  it("should join many-to-many with and between vector having scalar operation ") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleInstance0.toList)

    val execPlan = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Nil, Nil)

    val canaryPlusOne = scalarOpMapper(MetricsTestData.timeseriesDataset,
      Observable.fromIterable(sampleCanary), queryConfig, 1000, resultSchema).
      toListL.runAsync.futureValue
    // scalastyle:off
    val lhs = QueryResult("someId", null, canaryPlusOne.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
      ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
    ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ))

    result.size shouldEqual 2
    result.map(_.key.labelValues) sameElements (expectedLabels) shouldEqual true

    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(301)
    result(1).rows.map(_.getDouble(1)).toList shouldEqual List(701)
  }

  it("should do LAND with on having multiple labels") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleProductionInstance0.toList)

    val execPlan = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Seq("instance", "job"), Nil)

    val canaryPlusOne = scalarOpMapper(MetricsTestData.timeseriesDataset,
      Observable.fromIterable(sampleCanary), queryConfig, 1000, resultSchema).
      toListL.runAsync.futureValue
    // scalastyle:off
    val lhs = QueryResult("someId", null, canaryPlusOne.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
      ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
    ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ))

    result.size shouldEqual 2
    result.map(_.key.labelValues) sameElements (expectedLabels) shouldEqual true
    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(301)
    result(1).rows.map(_.getDouble(1)).toList shouldEqual List(701)

  }

  it("should do LAND with on having one matching label") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleProductionInstance0.toList)

    val execPlan = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Seq("instance"), Nil)

    val canaryPlusOne = scalarOpMapper(MetricsTestData.timeseriesDataset,
      Observable.fromIterable(sampleCanary), queryConfig, 1000, resultSchema).
      toListL.runAsync.futureValue
    // scalastyle:off
    val lhs = QueryResult("someId", null, canaryPlusOne.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
      ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
    ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ))

    result.size shouldEqual 2
    result.map(_.key.labelValues) sameElements (expectedLabels) shouldEqual true
    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(301)
    result(1).rows.map(_.getDouble(1)).toList shouldEqual List(701)
  }

  it("should do LAND with ignoring having one label") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleProductionInstance0.toList)

    val execPlan = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Nil, Seq("group"))

    val canaryPlusOne = scalarOpMapper(MetricsTestData.timeseriesDataset,
      Observable.fromIterable(sampleCanary), queryConfig, 1000, resultSchema).
      toListL.runAsync.futureValue
    // scalastyle:off
    val lhs = QueryResult("someId", null, canaryPlusOne.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
      ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
    ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ))

    result.size shouldEqual 2
    result.map(_.key.labelValues) sameElements (expectedLabels)
    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(301)
    result(1).rows.map(_.getDouble(1)).toList shouldEqual List(701)
  }

  it("should do LAND with ignoring having multiple labels") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleProductionInstance0.toList)
    val execPlan = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Nil, Seq("group", "job"))

    val canaryPlusOne = scalarOpMapper(MetricsTestData.timeseriesDataset,
      Observable.fromIterable(sampleCanary), queryConfig, 1000, resultSchema).
      toListL.runAsync.futureValue
    // scalastyle:off
    val lhs = QueryResult("someId", null, canaryPlusOne.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
      ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
    ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ))

    result.size shouldEqual 2
    result.map(_.key.labelValues) sameElements (expectedLabels) shouldEqual true
    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(301)
    result(1).rows.map(_.getDouble(1)).toList shouldEqual List(701)
  }

  it("should return Lhs when LAND is done with vector having no labels with on dummy") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleNoKey.toList)
    val execPlan = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Seq("dummy"), Nil)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleHttpRequests.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue

    result.size shouldEqual 8
    result.map(_.key.labelValues) sameElements (sampleHttpRequests.map(_.key.labelValues).toList) shouldEqual true
    sampleHttpRequests.flatMap(_.rows.map(_.getDouble(1)).toList).
      sameElements(result.flatMap(_.rows.map(_.getDouble(1)).toList)) shouldEqual true
  }

  it("should return Lhs when LAND is done with vector having no labels and ignoring is used om Lhs labels") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleNoKey.toList)
    val execPlan = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Nil, Seq("group", "instance", "job"))

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleHttpRequests.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue

    result.size shouldEqual 8
    result.map(_.key.labelValues) sameElements (sampleHttpRequests.map(_.key.labelValues)) shouldEqual true
    sampleHttpRequests.flatMap(_.rows.map(_.getDouble(1)).toList).
      sameElements(result.flatMap(_.rows.map(_.getDouble(1)).toList)) shouldEqual true
  }

  it("should join many-to-many with or") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleProduction.toList)
    val execPlan = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LOR,
      Nil, Nil)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleCanary.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue

    result.size shouldEqual 8

    // Result should equal sampleHttpRequests as it has only 2 groups - production & canary
    result.flatMap(_.key.labelValues.values.toSet).sorted sameElements
      (sampleHttpRequests.flatMap(_.key.labelValues.values.toSet)).sorted shouldEqual true
    result.flatMap(_.key.labelValues.keySet).sorted sameElements
      (sampleHttpRequests.flatMap(_.key.labelValues.keySet)).sorted shouldEqual true
    (sampleHttpRequests.flatMap(_.rows.map(_.getDouble(1)).toSet)).toSet.diff(result.
      flatMap(_.rows.map(_.getDouble(1)).toSet).toSet).isEmpty shouldEqual (true)
  }

  it("should drop overlapping samples from rhs when performing LOR ") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleInstance1.toList)

    val execPlan = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LOR,
      Nil, Nil)

    val canaryPlusOne = scalarOpMapper(MetricsTestData.timeseriesDataset,
      Observable.fromIterable(sampleCanary), queryConfig, 1000, resultSchema).
      toListL.runAsync.futureValue
    // scalastyle:off
    val lhs = QueryResult("someId", null, canaryPlusOne.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue

    val expectedResult = (canaryPlusOne.toArray ++ sampleInstance1).distinct
    val expectedLabels = List(Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
      ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
    ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("production")
      ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("production")
      )

    )

    val expectedValues = List(301.0, 401.0, 701.0, 801.0, 200.0, 600.0)

    result.size shouldEqual 6
    result.flatMap(_.key.labelValues.values.toSet).sorted sameElements expectedLabels.flatMap(_.toSet).sorted
    expectedValues.toSet.diff(result.flatMap(_.rows.map(_.getDouble(1)).toSet).toSet).isEmpty shouldEqual true
  }

  it("should excludes everything that has instance=0/1 but includes entries without " +
    "the instance label when performing LOR on instance") {

    // Query (http_requests{group="canary"} + 1) or on(instance) (http_requests or vector_matching_a)
    val execPlan1 = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LOR,
      Nil, Nil)

    val canaryPlusOne = scalarOpMapper(MetricsTestData.timeseriesDataset,
      Observable.fromIterable(sampleCanary), queryConfig, 1000, resultSchema).
      toListL.runAsync.futureValue
    // scalastyle:off
    val lhs1 = QueryResult("someId", null, sampleHttpRequests.map(rv => SerializableRangeVector(rv, schema)))
    val rhs1 = QueryResult("someId", null, sampleVectorMatching.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result1 = execPlan1.compose(dataset, Observable.fromIterable(Seq((rhs1, 1), (lhs1, 0))), queryConfig)
      .toListL.runAsync.futureValue

    val execPlan2 = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LOR,
      Seq("instance"), Nil)

    // scalastyle:off
    val lhs2 = QueryResult("someId", null, canaryPlusOne.map(rv => SerializableRangeVector(rv, schema)))
    val rhs2 = QueryResult("someId", null, result1.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result2 = execPlan2.compose(dataset, Observable.fromIterable(Seq((rhs2, 1), (lhs2, 0))), queryConfig)
      .toListL.runAsync.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
      ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
    ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("vector_matching_a"),
        ZeroCopyUTF8String("l") -> ZeroCopyUTF8String("x")
      ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("vector_matching_a"),
        ZeroCopyUTF8String("l") -> ZeroCopyUTF8String("y")
      )
    )

    result2.size shouldEqual 6
    result2.map(_.key.labelValues) sameElements (expectedLabels) shouldEqual true

    result2(0).rows.map(_.getDouble(1)).toList shouldEqual List(301)
    result2(1).rows.map(_.getDouble(1)).toList shouldEqual List(401)
    result2(2).rows.map(_.getDouble(1)).toList shouldEqual List(701)
    result2(3).rows.map(_.getDouble(1)).toList shouldEqual List(801)
    result2(4).rows.map(_.getDouble(1)).toList shouldEqual List(100)
    result2(5).rows.map(_.getDouble(1)).toList shouldEqual List(200)
  }

  it("should excludes everything that has instance=0/1 but includes entries without " +
    "the instance label when performing LOR with ignoring on l, group and job") {

    // Query (http_requests{group="canary"} + 1) or ignoring(l, group, job) (http_requests or vector_matching_a)
    val execPlan1 = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LOR,
      Nil, Nil)

    val canaryPlusOne = scalarOpMapper(MetricsTestData.timeseriesDataset,
      Observable.fromIterable(sampleCanary), queryConfig, 1000, resultSchema).
      toListL.runAsync.futureValue
    // scalastyle:off
    val lhs1 = QueryResult("someId", null, sampleHttpRequests.map(rv => SerializableRangeVector(rv, schema)))
    val rhs1 = QueryResult("someId", null, sampleVectorMatching.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result1 = execPlan1.compose(dataset, Observable.fromIterable(Seq((rhs1, 1), (lhs1, 0))), queryConfig)
      .toListL.runAsync.futureValue

    val execPlan2 = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LOR,
      Nil, Seq("l", "group", "job"))

    // scalastyle:off
    val lhs2 = QueryResult("someId", null, canaryPlusOne.map(rv => SerializableRangeVector(rv, schema)))
    val rhs2 = QueryResult("someId", null, result1.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result2 = execPlan2.compose(dataset, Observable.fromIterable(Seq((rhs2, 1), (lhs2, 0))), queryConfig)
      .toListL.runAsync.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
      ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
    ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("vector_matching_a"),
        ZeroCopyUTF8String("l") -> ZeroCopyUTF8String("x")
      ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("vector_matching_a"),
        ZeroCopyUTF8String("l") -> ZeroCopyUTF8String("y")
      )
    )

    result2.size shouldEqual 6
    result2.map(_.key.labelValues) sameElements (expectedLabels) shouldEqual true

    result2(0).rows.map(_.getDouble(1)).toList shouldEqual List(301)
    result2(1).rows.map(_.getDouble(1)).toList shouldEqual List(401)
    result2(2).rows.map(_.getDouble(1)).toList shouldEqual List(701)
    result2(3).rows.map(_.getDouble(1)).toList shouldEqual List(801)
    result2(4).rows.map(_.getDouble(1)).toList shouldEqual List(100)
    result2(5).rows.map(_.getDouble(1)).toList shouldEqual List(200)
  }

  it("should join many-to-many with unless") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleInstance0.toList)
    val execPlan = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LUnless,
      Nil, Nil)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleCanary.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
      ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
    ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ))

    result.size shouldEqual 2

    result.map(_.key.labelValues) sameElements (expectedLabels) shouldEqual true
    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(400)
    result(1).rows.map(_.getDouble(1)).toList shouldEqual List(800)
  }

  it("should not return any results when rhs has same vector on joining with on labels with LUnless") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleInstance0.toList)
    val execPlan = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LUnless,
      Seq("job"), Nil)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleCanary.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
      ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
    ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ))

    // group=canary and instance=0 have same jobs. We are joining on Job so no result
    result.size shouldEqual 0
  }

  it("LUnless should return lhs samples which are not present in rhs and where on labels are not equal") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleInstance0.toList)
    val execPlan = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LUnless,
      Seq("job", "instance"), Nil)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleCanary.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
      ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
    ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ))

    result.size shouldEqual 2

    // Joining on job and instance both so vectors which have instance = 1 will come in result as instance=0 is in LHS
    result.map(_.key.labelValues) sameElements (expectedLabels) shouldEqual true
    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(400)
    result(1).rows.map(_.getDouble(1)).toList shouldEqual List(800)
  }

  it("should not return any results when rhs has same vector on joining without ignoring labels with LUnless") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleInstance0.toList)
    val execPlan = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LUnless,
      Seq("job"), Nil)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleCanary.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
      ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
    ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ))

    // group=canary and instance=0 have same jobs. We are joining on Job so no result
    result.size shouldEqual 0
  }

  it("LUnless should return lhs samples which are not present in rhs and where labels other than " +
    "ignoring labels are not equal") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleInstance0.toList)
    val execPlan = SetOperatorExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LUnless,
      Seq("job", "instance"), Nil)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleCanary.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
      ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
    ),
      Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("app-server"),
        ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("1"),
        ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("canary")
      ))

    result.size shouldEqual 2

    // Joining on job and instance both so vectors which have instance = 1 will come in result as instance=0 is in LHS
    result.map(_.key.labelValues) sameElements (expectedLabels) shouldEqual true
    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(400)
    result(1).rows.map(_.getDouble(1)).toList shouldEqual List(800)
  }
}
