package filodb.query.exec

import scala.concurrent.ExecutionContext
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
                         (implicit sched: ExecutionContext,
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
  val sampleCanary = sampleHttpRequests.filter(_.key.labelValues.get(ZeroCopyUTF8String("group")).get.
    toString.equals("canary"))
  val sampleInstance0 = sampleHttpRequests.filter(_.key.labelValues.get(ZeroCopyUTF8String("instance")).get.
    toString.equals("0"))
  val sampleProductionInstance0 = sampleInstance0.filter(_.key.labelValues.get(ZeroCopyUTF8String("group")).get.
    toString.equals("production"))

  val execPlan = BinaryJoinExec("someID", dummyDispatcher,
    Array(dummyPlan), // cannot be empty as some compose's rely on the schema
    new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
    BinaryOperator.ADD,
    Cardinality.ManyToMany,
    Nil, Nil)

  val scalarOpMapper = exec.ScalarOperationMapper(BinaryOperator.ADD, 1.0, false)


  it("should join many-to-many with and ") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleInstance0.toList)
    val execPlan = BinaryJoinExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Cardinality.ManyToMany,
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
    result.map(_.key.labelValues) sameElements (expectedLabels)
    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(300)
    result(1).rows.map(_.getDouble(1)).toList shouldEqual List(700)

    result.map(_.key).toSet.size
  }

  it("should join many-to-many with and between vector having scalar operation ") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleInstance0.toList)

    val execPlan = BinaryJoinExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Cardinality.ManyToMany,
      Nil, Nil)

    val canaryPlusOne = scalarOpMapper(Observable.fromIterable(sampleCanary), queryConfig, 1000, resultSchema).
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

  it("should do LAND with on having multiple labels") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleProductionInstance0.toList)
    //scala.util.Random.shuffle(sampleHttpRequests.toList) // they may come out of order

    val execPlan = BinaryJoinExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Cardinality.ManyToMany,
      Seq("instance", "job"), Nil)

    val canaryPlusOne = scalarOpMapper(Observable.fromIterable(sampleCanary), queryConfig, 1000, resultSchema).
      toListL.runAsync.futureValue
    // scalastyle:off
    val lhs = QueryResult("someId", null, canaryPlusOne.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue
    //        result.map(_.key.labelValues).foreach(println(_))
    //        val s = result.flatMap(_.rows.map(_.getDouble(1))).foreach(println(_))

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

  it("should do LAND with on having one matching label") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleProductionInstance0.toList)
    //scala.util.Random.shuffle(sampleHttpRequests.toList) // they may come out of order

    val execPlan = BinaryJoinExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Cardinality.ManyToMany,
      Seq("instance"), Nil)

    val canaryPlusOne = scalarOpMapper(Observable.fromIterable(sampleCanary), queryConfig, 1000, resultSchema).
      toListL.runAsync.futureValue
    // scalastyle:off
    val lhs = QueryResult("someId", null, canaryPlusOne.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue
    //        result.map(_.key.labelValues).foreach(println(_))
    //        val s = result.flatMap(_.rows.map(_.getDouble(1))).foreach(println(_))

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

  it("should do LAND with ignoring having one label") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleProductionInstance0.toList)

    val execPlan = BinaryJoinExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Cardinality.ManyToMany,
      Nil, Seq("group"))

    val canaryPlusOne = scalarOpMapper(Observable.fromIterable(sampleCanary), queryConfig, 1000, resultSchema).
      toListL.runAsync.futureValue
    // scalastyle:off
    val lhs = QueryResult("someId", null, canaryPlusOne.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue
    //            result.map(_.key.labelValues).foreach(println(_))
    //            val s = result.flatMap(_.rows.map(_.getDouble(1))).foreach(println(_))

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

    result.map(_.key).toSet.size
  }

  it("should do LAND with ignoring having multiple labels") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleProductionInstance0.toList)
    val execPlan = BinaryJoinExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Cardinality.ManyToMany,
      Nil, Seq("group", "job"))

    val canaryPlusOne = scalarOpMapper(Observable.fromIterable(sampleCanary), queryConfig, 1000, resultSchema).
      toListL.runAsync.futureValue
    // scalastyle:off
    val lhs = QueryResult("someId", null, canaryPlusOne.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhsShuffled.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(dataset, Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig)
      .toListL.runAsync.futureValue
    //            result.map(_.key.labelValues).foreach(println(_))
    //            val s = result.flatMap(_.rows.map(_.getDouble(1))).foreach(println(_))

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
    val execPlan = BinaryJoinExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Cardinality.ManyToMany,
      Seq("dummy"), Nil)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleHttpRequests.map(rv => SerializableRangeVector(rv, schema)))
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

    result.size shouldEqual 8

    result.map(_.key.labelValues) sameElements (sampleHttpRequests.map(_.key.labelValues).toList)
    sampleHttpRequests.flatMap(_.rows.map(_.getDouble(1)).toList).
      sameElements(result.flatMap(_.rows.map(_.getDouble(1)).toList)) shouldEqual true

  }

  it("should return Lhs when LAND is done with vector having no labels and ignoring is used om Lhs labels") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleNoKey.toList)
    val execPlan = BinaryJoinExec("someID", dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Cardinality.ManyToMany,
      Nil, Seq("group", "instance", "job"))

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleHttpRequests.map(rv => SerializableRangeVector(rv, schema)))
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

    result.size shouldEqual 8

    result.map(_.key.labelValues) sameElements (sampleHttpRequests.map(_.key.labelValues).toList)
    sampleHttpRequests.flatMap(_.rows.map(_.getDouble(1)).toList).
      sameElements(result.flatMap(_.rows.map(_.getDouble(1)).toList)) shouldEqual true

  }

}
