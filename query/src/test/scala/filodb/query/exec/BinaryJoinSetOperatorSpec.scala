package filodb.query.exec

import scala.util.Random

import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures

import filodb.core.MetricsTestData
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.core.store.ChunkSource
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


// scalastyle:off number.of.methods
class BinaryJoinSetOperatorSpec extends AnyFunSpec with Matchers with ScalaFutures {

  import MultiSchemaPartitionsExecSpec._

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)

  val tvSchema = ResultSchema(Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn)), 1)
  val schema = Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn))

  val rand = new Random()
  val error = 0.00000001d
  val noKey = CustomRangeVectorKey(Map.empty)
  val queryStats = QueryStats()

  val dummyDispatcher = new PlanDispatcher {

    override def dispatch(plan: ExecPlanWithClientParams, source: ChunkSource)
                         (implicit sched: Scheduler): Task[QueryResponse] = ???

    override def clusterName: String = ???

    override def isLocalCall: Boolean = ???
    override def dispatchStreaming(plan: ExecPlanWithClientParams,
                                   source: ChunkSource)(implicit sched: Scheduler): Observable[StreamQueryResponse] = ???
  }
  val resultSchema = ResultSchema(MetricsTestData.timeseriesSchema.infosFromIDs(0 to 1), 1)
  val resSchemaTask = Task.eval(resultSchema)

  val sampleHttpRequests: Array[RangeVector] = Array(
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"api-server".utf8,
          "instance".utf8 -> "0".utf8,
          "group".utf8 -> s"production".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 100)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"api-server".utf8,
          "instance".utf8 -> "1".utf8,
          "group".utf8 -> s"production".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 200)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"api-server".utf8,
          "instance".utf8 -> "0".utf8,
          "group".utf8 -> s"canary".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 300)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"api-server".utf8,
          "instance".utf8 -> "1".utf8,
          "group".utf8 -> s"canary".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 400)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"app-server".utf8,
          "instance".utf8 -> "0".utf8,
          "group".utf8 -> s"production".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 500)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"app-server".utf8,
          "instance".utf8 -> "1".utf8,
          "group".utf8 -> s"production".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 600)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"app-server".utf8,
          "instance".utf8 -> "0".utf8,
          "group".utf8 -> s"canary".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 700)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"app-server".utf8,
          "instance".utf8 -> "1".utf8,
          "group".utf8 -> s"canary".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 800)).iterator
      override def outputRange: Option[RvRange] = None
    }
  )
  val sampleNoKey: Array[RangeVector] = Array(
    new RangeVector {
      override def key: RangeVectorKey = noKey

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 1)).iterator
      override def outputRange: Option[RvRange] = None
    }
  )

  val sampleVectorMatching: Array[RangeVector] = Array(
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"vector_matching_a".utf8,
          "l".utf8 -> "x".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 100)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"vector_matching_a".utf8,
          "l".utf8 -> "y".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 200)).iterator
      override def outputRange: Option[RvRange] = None
    }
  )

  val sampleWithNaN: Array[RangeVector] = Array(
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"api-server".utf8,
          "instance".utf8 -> "0".utf8,
          "group".utf8 -> s"production".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 100),
        new TransientRow(2L, Double.NaN)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"api-server".utf8,
          "instance".utf8 -> "1".utf8,
          "group".utf8 -> s"production".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, Double.NaN)).iterator
      override def outputRange: Option[RvRange] = None
    })

  val sampleAllNaN : Array[RangeVector] = Array(
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"api-server".utf8,
          "instance".utf8 -> "0".utf8,
          "group".utf8 -> s"production".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, Double.NaN)).iterator
      override def outputRange: Option[RvRange] = None
    })

  val sampleMultipleRows: Array[RangeVector] = Array(
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"api-server".utf8,
          "instance".utf8 -> "0".utf8,
          "group".utf8 -> s"production".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 100),
        new TransientRow(2L, 300)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"http_requests".utf8,
          "job".utf8 -> s"api-server".utf8,
          "instance".utf8 -> "1".utf8,
          "group".utf8 -> s"production".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 200),
        new TransientRow(2L, 400)).iterator
      override def outputRange: Option[RvRange] = None
    })

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

  val scalarOpMapper = exec.ScalarOperationMapper(BinaryOperator.ADD, false,
    Seq(StaticFuncArgs(1.0,(RangeParams(100,20,500)))))

  it("should join many-to-many with and") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleInstance0.toList)
    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Nil, Nil, "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, sampleCanary.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleRhsShuffled.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue


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

    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Nil, Nil, "__name__", None)

    val canaryPlusOne = scalarOpMapper(Observable.fromIterable(sampleCanary), querySession, 1000, resultSchema).
      toListL.runToFuture.futureValue
    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, canaryPlusOne.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleRhsShuffled.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

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

    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Seq("instance", "job"), Nil, "__name__", None)

    val canaryPlusOne = scalarOpMapper(Observable.fromIterable(sampleCanary), querySession, 1000, resultSchema).
      toListL.runToFuture.futureValue
    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, canaryPlusOne.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleRhsShuffled.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

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

    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Seq("instance"), Nil, "__name__", None)

    val canaryPlusOne = scalarOpMapper(Observable.fromIterable(sampleCanary), querySession, 1000, resultSchema).
      toListL.runToFuture.futureValue
    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, canaryPlusOne.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleRhsShuffled.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

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

    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Nil, Seq("group"), "__name__", None)

    val canaryPlusOne = scalarOpMapper(Observable.fromIterable(sampleCanary), querySession, 1000, resultSchema).
      toListL.runToFuture.futureValue
    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, canaryPlusOne.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleRhsShuffled.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

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
    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Nil, Seq("group", "job"), "__name__", None)

    val canaryPlusOne = scalarOpMapper(Observable.fromIterable(sampleCanary), querySession, 1000, resultSchema).
      toListL.runToFuture.futureValue
    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, canaryPlusOne.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleRhsShuffled.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

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
    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Seq("dummy"), Nil, "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, sampleHttpRequests.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleRhsShuffled.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    result.size shouldEqual 8
    result.map(_.key.labelValues) sameElements (sampleHttpRequests.map(_.key.labelValues).toList) shouldEqual true
    sampleHttpRequests.flatMap(_.rows.map(_.getDouble(1)).toList).
      sameElements(result.flatMap(_.rows.map(_.getDouble(1)).toList)) shouldEqual true
  }

  it("should not return LHS when op=LAND and LHS has no labels and RHS is empty") {
    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher, Array(dummyPlan),
                       new Array[ExecPlan](1), BinaryOperator.LAND, Nil, Nil, "__name__", None)
    val rvEmptyLabels = sampleHttpRequests
      // remove the labels from the key
      .map(rv => IteratorBackedRangeVector(new CustomRangeVectorKey(Map()), rv.rows(), rv.outputRange))
      .head
    val lhs = QueryResult("someId", tvSchema, Seq(rvEmptyLabels))
    val rhs = QueryResult("someId", tvSchema, Nil)
    val childrenObservable = Observable.fromIterable(Seq((rhs, 1), (lhs, 0)))
    val result = execPlan.compose(childrenObservable, resSchemaTask, querySession)
      .toListL.runToFuture.futureValue
    result.size shouldEqual 0
  }

  it("should return Lhs when LAND is done with vector having no labels and ignoring is used om Lhs labels") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleNoKey.toList)
    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Nil, Seq("group", "instance", "job"), "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, sampleHttpRequests.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleRhsShuffled.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    result.size shouldEqual 8
    result.map(_.key.labelValues) sameElements (sampleHttpRequests.map(_.key.labelValues)) shouldEqual true
    sampleHttpRequests.flatMap(_.rows.map(_.getDouble(1)).toList).
      sameElements(result.flatMap(_.rows.map(_.getDouble(1)).toList)) shouldEqual true
  }

  it("should join many-to-many with or") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleProduction.toList)
    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LOR,
      Nil, Nil, "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, sampleCanary.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleRhsShuffled.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

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

    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LOR,
      Nil, Nil, "__name__", None)

    val canaryPlusOne = scalarOpMapper(Observable.fromIterable(sampleCanary), querySession, 1000, resultSchema).
      toListL.runToFuture.futureValue
    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, canaryPlusOne.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleRhsShuffled.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

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
    val execPlan1 = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LOR,
      Nil, Nil, "__name__", None)

    val canaryPlusOne = scalarOpMapper(Observable.fromIterable(sampleCanary), querySession, 1000, resultSchema).
      toListL.runToFuture.futureValue
    // scalastyle:off
    val lhs1 = QueryResult("someId", tvSchema, sampleHttpRequests.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs1 = QueryResult("someId", tvSchema, sampleVectorMatching.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result1 = execPlan1.compose(Observable.fromIterable(Seq((rhs1, 1), (lhs1, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    val execPlan2 = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LOR,
      Seq("instance"), Nil, "__name__", None)

    // scalastyle:off
    val lhs2 = QueryResult("someId", tvSchema, canaryPlusOne.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs2 = QueryResult("someId", tvSchema, result1.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result2 = execPlan2.compose(Observable.fromIterable(Seq((rhs2, 1), (lhs2, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

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
    result2.map(_.key.labelValues).toSet.equals(expectedLabels.toSet) shouldEqual true

    result2(0).rows.map(_.getDouble(1)).toList shouldEqual List(301)
    result2(1).rows.map(_.getDouble(1)).toList shouldEqual List(701)
    result2(2).rows.map(_.getDouble(1)).toList shouldEqual List(401)
    result2(3).rows.map(_.getDouble(1)).toList shouldEqual List(801)
    result2(4).rows.map(_.getDouble(1)).toList shouldEqual List(200)
    result2(5).rows.map(_.getDouble(1)).toList shouldEqual List(100)
  }

  it("should excludes everything that has instance=0/1 but includes entries without " +
    "the instance label when performing LOR with ignoring on l, group and job") {

    // Query (http_requests{group="canary"} + 1) or ignoring(l, group, job) (http_requests or vector_matching_a)
    val execPlan1 = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LOR,
      Nil, Nil, "__name__", None)

    val canaryPlusOne = scalarOpMapper(Observable.fromIterable(sampleCanary), querySession, 1000, resultSchema).
      toListL.runToFuture.futureValue
    // scalastyle:off
    val lhs1 = QueryResult("someId", tvSchema, sampleHttpRequests.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs1 = QueryResult("someId", tvSchema, sampleVectorMatching.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result1 = execPlan1.compose(Observable.fromIterable(Seq((rhs1, 1), (lhs1, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    val execPlan2 = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LOR,
      Nil, Seq("l", "group", "job"), "__name__", None)

    // scalastyle:off
    val lhs2 = QueryResult("someId", tvSchema, canaryPlusOne.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs2 = QueryResult("someId", tvSchema, result1.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result2 = execPlan2.compose(Observable.fromIterable(Seq((rhs2, 1), (lhs2, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

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
    result2.map(_.key.labelValues).toSet.equals(expectedLabels.toSet)

    result2(0).rows.map(_.getDouble(1)).toList shouldEqual List(301)
    result2(1).rows.map(_.getDouble(1)).toList shouldEqual List(701)
    result2(2).rows.map(_.getDouble(1)).toList shouldEqual List(401)
    result2(3).rows.map(_.getDouble(1)).toList shouldEqual List(801)
    result2(4).rows.map(_.getDouble(1)).toList shouldEqual List(200)
    result2(5).rows.map(_.getDouble(1)).toList shouldEqual List(100)
  }

  it("should join many-to-many with unless") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleInstance0.toList)
    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LUnless,
      Nil, Nil, "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, sampleCanary.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleRhsShuffled.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

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
    result.map(_.key.labelValues).toSet.equals(expectedLabels.toSet) shouldEqual true
    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(800)
    result(1).rows.map(_.getDouble(1)).toList shouldEqual List(400)
  }

  it("should not return any results when rhs has same vector on joining with on labels with LUnless") {

    val sampleRhsShuffled = scala.util.Random.shuffle(sampleInstance0.toList)
    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LUnless,
      Seq("job"), Nil, "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, sampleCanary.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleRhsShuffled.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

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
    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LUnless,
      Seq("job", "instance"), Nil, "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, sampleCanary.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleRhsShuffled.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

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
    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LUnless,
      Seq("job"), Nil, "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, sampleCanary.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleRhsShuffled.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

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
    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LUnless,
      Seq("job", "instance"), Nil, "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, sampleCanary.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleRhsShuffled.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

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

  it("AND should not return rv's when RHS has only NaN") {

    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Nil, Nil, "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, sampleHttpRequests.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleAllNaN.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

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

    result.size shouldEqual 0
  }

  it("AND should return only non NaN RangeVectors") {

    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Nil, Nil, "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, sampleHttpRequests.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleWithNaN.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
      ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("production")
    ))

    result.size shouldEqual 1 // second RV in sampleWithNaN has all Nan's
    result.map(_.key.labelValues) sameElements (expectedLabels) shouldEqual true
    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(100)
  }

  it("AND should return NaN when rhs sample has Nan even when LHS is not NaN ") {

    val execPlan = SetOperatorExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.LAND,
      Nil, Nil, "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", tvSchema, sampleMultipleRows.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleWithNaN.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("__name__") -> ZeroCopyUTF8String("http_requests"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("api-server"),
      ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("0"),
      ZeroCopyUTF8String("group") -> ZeroCopyUTF8String("production")
    ))

    result.size shouldEqual 1 // second RV in sampleWithNaN has all Nan's
    result.map(_.key.labelValues) sameElements (expectedLabels) shouldEqual true
    val rowValues = result(0).rows.map(_.getDouble(1)).toList
    rowValues.head shouldEqual 100
    // LHS second RV has value 300 for 2L, however RHS has Double.NaN for 2L so RHS value is picked
    rowValues(1).isNaN shouldEqual true
  }

  it ("should remove dupes in LHS and stitch before joining for LOR") {

    def dataRows = Stream.from(0).map(n => new TransientRow(n.toLong, n.toDouble))

    val queryContext = QueryContext(plannerParams = PlannerParams(joinQueryCardLimit = 10)) // set join card limit to 1
    val execPlan = SetOperatorExec(queryContext, dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
     BinaryOperator.LOR,
      Nil, Nil, "__name__", None)

    import NoCloseCursor._
    val lhsRv = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(20).iterator
      override def outputRange: Option[RvRange] = None
    }

    val lhsRvDupe = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(30).drop(20).iterator
      override def outputRange: Option[RvRange] = None
    }

    val lhsRvDupe2 = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(40).drop(30).iterator
      override def outputRange: Option[RvRange] = None
    }

    val rhsRv = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value2".utf8))
      val rows: RangeVectorCursor = dataRows.take(40).iterator
      override def outputRange: Option[RvRange] = None
    }

    val lhs = QueryResult("someId", tvSchema, Seq(lhsRv, lhsRvDupe, lhsRvDupe2).map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, Seq(rhsRv).map(rv => SerializedRangeVector(rv, schema, queryStats)))

    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    result.size shouldEqual 2
    result.head.key.labelValues shouldEqual Map("tag".utf8 -> s"value1".utf8)
    result.last.key.labelValues shouldEqual Map("tag".utf8 -> s"value2".utf8)
    result.head.rows().map(_.getLong(0)).toList shouldEqual (0L until 40).toList
    result.last.rows().map(_.getLong(0)).toList shouldEqual (0L until 40).toList
  }

  it ("should remove dupes in RHS and stitch before joining for LOR") {

    def dataRows = Stream.from(0).map(n => new TransientRow(n.toLong, n.toDouble))

    val queryContext = QueryContext(plannerParams = PlannerParams(joinQueryCardLimit = 10)) // set join card limit to 1
    val execPlan = SetOperatorExec(queryContext, dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.LOR,
      Nil, Nil, "__name__", None)

    import NoCloseCursor._
    val rhsRv = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(20).iterator
      override def outputRange: Option[RvRange] = None
    }

    val rhsRvDupe = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(30).drop(20).iterator
      override def outputRange: Option[RvRange] = None
    }
    val rhsRvDupe2 = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(40).drop(30).iterator
      override def outputRange: Option[RvRange] = None
    }

    val lhsRv = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value".utf8))
      val rows: RangeVectorCursor = dataRows.take(40).iterator
      override def outputRange: Option[RvRange] = None
    }

    val lhs = QueryResult("someId", tvSchema, Seq(lhsRv).map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, Seq(rhsRv, rhsRvDupe, rhsRvDupe2).map(rv => SerializedRangeVector(rv, schema, queryStats)))

    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    result.size shouldEqual 2
    result.head.key.labelValues shouldEqual Map("tag".utf8 -> s"value".utf8)
    result.last.key.labelValues shouldEqual Map("tag".utf8 -> s"value1".utf8)
    result.head.rows().map(_.getLong(0)).toList shouldEqual (0L until 40).toList
    result.last.rows().map(_.getLong(0)).toList shouldEqual (0L until 40).toList
  }

  it ("should remove dupes in LHS and stitch before joining for LAND") {

    def dataRows = Stream.from(0).map(n => new TransientRow(n.toLong, n.toDouble))

    val queryContext = QueryContext(plannerParams = PlannerParams(joinQueryCardLimit = 10)) // set join card limit to 1
    val execPlan = SetOperatorExec(queryContext, dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.LAND,
      Nil, Nil, "__name__", None)

    import NoCloseCursor._
    val lhsRv = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(20).iterator
      override def outputRange: Option[RvRange] = None
    }

    val lhsRvDupe = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(30).drop(20).iterator
      override def outputRange: Option[RvRange] = None
    }

    val lhsRvDupe2 = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(40).drop(30).iterator
      override def outputRange: Option[RvRange] = None
    }

    val rhsRv = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(40).iterator
      override def outputRange: Option[RvRange] = None
    }

    val lhs = QueryResult("someId", tvSchema, Seq(lhsRv, lhsRvDupe, lhsRvDupe2).map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, Seq(rhsRv).map(rv => SerializedRangeVector(rv, schema, queryStats)))

    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    result.size shouldEqual 1
    result.head.key.labelValues shouldEqual Map("tag".utf8 -> s"value1".utf8)
    result.head.rows().map(_.getLong(0)).toList shouldEqual (0L until 40).toList
  }

  it ("should join with LAND when RHS has dupes") {

    def dataRows = Stream.from(0).map(n => new TransientRow(n.toLong, n.toDouble))

    val queryContext = QueryContext(plannerParams = PlannerParams(joinQueryCardLimit = 10)) // set join card limit to 1
    val execPlan = SetOperatorExec(queryContext, dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.LAND,
      Nil, Nil, "__name__", None)

    import NoCloseCursor._
    val rhsRv = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(20).iterator
      override def outputRange: Option[RvRange] = None
    }

    val rhsRvDupe = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(30).drop(20).iterator
      override def outputRange: Option[RvRange] = None
    }
    val rhsRvDupe2 = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(40).drop(30).iterator
      override def outputRange: Option[RvRange] = None
    }

    val lhsRv = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(40).iterator
      override def outputRange: Option[RvRange] = None
    }

    val lhs = QueryResult("someId", tvSchema, Seq(lhsRv).map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, Seq(rhsRv, rhsRvDupe, rhsRvDupe2).map(rv => SerializedRangeVector(rv, schema, queryStats)))

    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    result.size shouldEqual 1
    result.head.key.labelValues shouldEqual Map("tag".utf8 -> s"value1".utf8)
    result.head.rows().map(_.getLong(0)).toList shouldEqual (0L until 40).toList
  }

  it ("should join with LUNLESS when RHS has dupes") {

    def dataRows = Stream.from(0).map(n => new TransientRow(n.toLong, n.toDouble))

    val queryContext = QueryContext(plannerParams = PlannerParams(joinQueryCardLimit = 10)) // set join card limit to 1
    val execPlan = SetOperatorExec(queryContext, dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.LUnless,
      Nil, Nil, "__name__", None)

    import NoCloseCursor._
    val rhsRv = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(20).iterator
      override def outputRange: Option[RvRange] = None
    }

    val rhsRvDupe = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(30).drop(20).iterator
      override def outputRange: Option[RvRange] = None
    }
    val rhsRvDupe2 = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(40).drop(30).iterator
      override def outputRange: Option[RvRange] = None
    }

    val lhsRv = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value".utf8))
      val rows: RangeVectorCursor = dataRows.take(40).iterator
      override def outputRange: Option[RvRange] = None
    }

    val lhs = QueryResult("someId", tvSchema, Seq(lhsRv).map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, Seq(rhsRv, rhsRvDupe, rhsRvDupe2).map(rv => SerializedRangeVector(rv, schema, queryStats)))

    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    result.size shouldEqual 1
    result.head.key.labelValues shouldEqual Map("tag".utf8 -> s"value".utf8)
    result.head.rows().map(_.getLong(0)).toList shouldEqual (0L until 40).toList
  }

  it ("should remove dupes in LHS and stitch before joining for LUNLESS") {

    def dataRows = Stream.from(0).map(n => new TransientRow(n.toLong, n.toDouble))

    val queryContext = QueryContext(plannerParams = PlannerParams(joinQueryCardLimit = 10)) // set join card limit to 1
    val execPlan = SetOperatorExec(queryContext, dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.LUnless,
      Nil, Nil, "__name__", None)

    import NoCloseCursor._
    val lhsRv = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(20).iterator
      override def outputRange: Option[RvRange] = None
    }

    val lhsRvDupe = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(30).drop(20).iterator
      override def outputRange: Option[RvRange] = None
    }

    val lhsRvDupe2 = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value1".utf8))
      val rows: RangeVectorCursor = dataRows.take(40).drop(30).iterator
      override def outputRange: Option[RvRange] = None
    }

    val rhsRv = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(Map("tag".utf8 -> s"value2".utf8))
      val rows: RangeVectorCursor = dataRows.take(40).iterator
      override def outputRange: Option[RvRange] = None
    }

    val lhs = QueryResult("someId", tvSchema, Seq(lhsRv, lhsRvDupe, lhsRvDupe2).map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, Seq(rhsRv).map(rv => SerializedRangeVector(rv, schema, queryStats)))

    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    result.size shouldEqual 1
    result.head.key.labelValues shouldEqual Map("tag".utf8 -> s"value1".utf8)
    result.head.rows().map(_.getLong(0)).toList shouldEqual (0L until 40).toList
  }

  it("AND should stitch dup LHS and not pick value when corresponding RHS value is NaN"){
    val sampleNaN: Array[RangeVector] = Array(
      new RangeVector {
        val key: RangeVectorKey = CustomRangeVectorKey(
          Map("__name__".utf8 -> s"http_requests".utf8,
            "job".utf8 -> s"api-server".utf8,
            "instance".utf8 -> "0".utf8,
            "group".utf8 -> s"production".utf8)
        )

        import NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(1L, 100),
          new TransientRow(2L, 200),
          new TransientRow(3L, Double.NaN)).iterator
        override def outputRange: Option[RvRange] = None
      })

    val lhs1: Array[RangeVector] = Array(
      new RangeVector {
        val key: RangeVectorKey = CustomRangeVectorKey(
          Map("__name__".utf8 -> s"http_requests".utf8,
            "job".utf8 -> s"api-server".utf8,
            "instance".utf8 -> "0".utf8,
            "group".utf8 -> s"production".utf8)
        )

        import NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(3L, 300)).iterator
        override def outputRange: Option[RvRange] = None
      })

    val lhs2: Array[RangeVector] = Array(
      new RangeVector {
        val key: RangeVectorKey = CustomRangeVectorKey(
          Map("__name__".utf8 -> s"http_requests".utf8,
            "job".utf8 -> s"api-server".utf8,
            "instance".utf8 -> "0".utf8,
            "group".utf8 -> s"production".utf8)
        )

        import NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(1L, 100),
          new TransientRow(2L, 200)).iterator
        override def outputRange: Option[RvRange] = None
      })

    val queryContext = QueryContext(plannerParams = PlannerParams(joinQueryCardLimit = 10)) // set join card limit to 1
    val execPlan = SetOperatorExec(queryContext, dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.LAND,
      Nil, Nil, "__name__", None)


    val lhs = QueryResult("someId", tvSchema, (lhs2 ++ lhs1).map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", tvSchema, sampleNaN.map(rv => SerializedRangeVector(rv, schema, queryStats)))

    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), resSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    result.size shouldEqual 1
    result.head.key.labelValues shouldEqual lhs1.head.key.labelValues
    val rows = result.head.rows().map(x => (x.getLong(0), x.getDouble(1))).toList
    rows.map(_._1) shouldEqual List(1, 2, 3)
    val rowValues = rows.map(_._2)
    rowValues.dropRight(1) shouldEqual List(100, 200)
    rowValues.last.isNaN shouldEqual(true) // As Rhs does not have any value at 3L
  }

  case class KeyedTupleRangeVector(rvKey: Map[ZeroCopyUTF8String, ZeroCopyUTF8String], values: Seq[(Long, Double)])
    extends RangeVector {

    import NoCloseCursor._

    def key: RangeVectorKey = CustomRangeVectorKey(rvKey)

    def rows(): RangeVectorCursor = values.map{ case (ts, value) => SeqRowReader(Seq[Any](ts, value))}.iterator

    /**
     * If Some, then it describes start/step/end of output data.
     * Present only for time series data that is periodic. If raw data is requested, then None.
     */
    def outputRange: Option[RvRange] = None

  }

  private def rangeVectors(keyedTs: List[(Map[ZeroCopyUTF8String, ZeroCopyUTF8String], Seq[(Long, Double)])]): List[RangeVector]
  = keyedTs.map{case (key, value) => SerializedRangeVector(KeyedTupleRangeVector(key, value), resultSchema.columns, queryStats)}.toList


  it("should return true when isEmpty called on emptyRV") {
    val exec = SetOperatorExec(QueryContext(), dummyDispatcher, Nil, Nil, BinaryOperator.LUnless, Nil, Nil, "_metric_", None)
    val emptyRv = KeyedTupleRangeVector(Map.empty, Seq.empty)
    exec.isEmpty(emptyRv, resultSchema) shouldEqual true
  }


  it("should return true when isEmpty called on rv with all NaNs") {
    val exec = SetOperatorExec(QueryContext(), dummyDispatcher, Nil, Nil, BinaryOperator.LUnless, Nil, Nil, "_metric_", None)
    val emptyRv = KeyedTupleRangeVector(Map.empty, Seq((0, Double.NaN), (10, Double.NaN), (20, Double.NaN)))
    exec.isEmpty(emptyRv, resultSchema) shouldEqual true
  }

  it("should return false when isEmpty called on rv with at least one non NaN") {
    val exec = SetOperatorExec(QueryContext(), dummyDispatcher, Nil, Nil, BinaryOperator.LUnless, Nil, Nil, "_metric_", None)
    val emptyRv = KeyedTupleRangeVector(Map.empty, Seq((0, Double.NaN), (10, 1.0), (20, Double.NaN)))
    exec.isEmpty(emptyRv, resultSchema) shouldEqual false
  }

  private def rvRowsToListOfTuples(rv: RangeVector) = {
    rv.rows().map(x => (x.getLong(0), x.getDouble(1))).toList
  }

  it("should perform A - B when no on is given correctly") {
    val exec = SetOperatorExec(QueryContext(), dummyDispatcher, Nil, Nil, BinaryOperator.LUnless, Nil, Nil, "_metric_", None)
    val lhsRv = rangeVectors(List(
      (Map( "label1".utf8 -> "value1".utf8)-> Seq((0, Double.NaN), (10, 1.0), (20, Double.NaN))),
      (Map( "label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((0, 1.0), (10, 2.0), (20, 3.0))),
      (Map( "label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((10, 1.0), (20, 2.0), (30, 3.0))),
      (Map( "label1".utf8 -> "value1".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((100, 1.0), (200, 2.0), (300, 3.0)))
    ))
    val rhsRv = rangeVectors(List(
      (Map( "label1".utf8 -> "value1".utf8)-> Seq((0, Double.NaN), (10, 1.0), (20, Double.NaN)))
    ))

    val map = exec.setOpUnless(lhsRv, rhsRv).map( rv => rv.key.labelValues -> rvRowsToListOfTuples(rv)).toMap
    map.size shouldEqual 2
    map.get(Map("label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)) match {
      case Some(matched)  => assertListEquals(matched, List((0,1.0), (10,Double.NaN), (20,Double.NaN), (30,3.0)))
      case None           => fail("Expected to find a matching RV for key Map(label2 -> value2, onLabel -> onValue1)")
    }

    map.get(Map( "label1".utf8 -> "value1".utf8, "onLabel".utf8 -> "onValue1".utf8)) match {
      case Some(matched)       => matched shouldEqual Seq((100, 1.0), (200, 2.0), (300, 3.0))
      case None                => fail("Expected to find a matching RV for key Map(label1 -> value1, onLabel -> onValue1)")
    }
  }

  it("should perform A - B correctly when on is given") {
    val exec = SetOperatorExec(QueryContext(), dummyDispatcher, Nil, Nil, BinaryOperator.LUnless, Seq("onLabel"), Nil, "_metric_", None)


    val lhsRv = rangeVectors(List(
      (Map( "label1".utf8 -> "value1".utf8)-> Seq((0, Double.NaN), (10, 1.0), (20, Double.NaN))),
      (Map( "label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((0, 1.0), (10, 2.0), (20, 3.0))),
      (Map( "label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((10, 1.0), (20, 2.0), (30, 3.0))),
      (Map( "label1".utf8 -> "value1".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((100, 1.0), (200, 2.0), (300, 3.0)))
    ))
    val rhsRv = rangeVectors(List(
      (Map( "label1".utf8 -> "value1".utf8)-> Seq((0, Double.NaN), (10, 1.0), (20, Double.NaN)))
    ))

    val map = exec.setOpUnless(lhsRv, rhsRv).map( rv => rv.key.labelValues -> rvRowsToListOfTuples(rv)).toMap
    map.size shouldBe 2
    map.get(Map("label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)) match {
      case Some(matched)  => assertListEquals(matched, List((0,1.0), (10,Double.NaN), (20,Double.NaN), (30,3.0)))
      case None           => fail("Expected to find a matching RV for key Map(label2 -> value2, onLabel -> onValue1)")
    }

    map.get(Map( "label1".utf8 -> "value1".utf8, "onLabel".utf8 -> "onValue1".utf8)) match {
      case Some(matched)       => matched shouldEqual Seq((100, 1.0), (200, 2.0), (300, 3.0))
      case None                => fail("Expected to find a matching RV for key Map(label1 -> value1, onLabel -> onValue1)")
    }

  }


  it("should perform A - B correctly only  ignoring is provided") {
    val exec = SetOperatorExec(QueryContext(), dummyDispatcher, Nil, Nil, BinaryOperator.LUnless, Nil, Seq("label1", "label2"), "_metric_", None)
    // This is same as using only onLabel for joining


    val lhsRv = rangeVectors(List(
      (Map( "label1".utf8 -> "value1".utf8)-> Seq((0, Double.NaN), (10, 1.0), (20, Double.NaN))),
      (Map( "label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((0, 1.0), (10, 2.0), (20, 3.0))),
      (Map( "label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((10, 1.0), (20, 2.0), (30, 3.0))),
      (Map( "label1".utf8 -> "value1".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((100, 1.0), (200, 2.0), (300, 3.0)))
    ))
    val rhsRv = rangeVectors(List(
      (Map( "label1".utf8 -> "value1".utf8)-> Seq((0, Double.NaN), (10, 1.0), (20, Double.NaN)))
    ))

    val map = exec.setOpUnless(lhsRv, rhsRv).map( rv => rv.key.labelValues -> rvRowsToListOfTuples(rv)).toMap
    map.size shouldBe 2
    map.get(Map("label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)) match {
      case Some(matched)  => assertListEquals(matched, List((0,1.0), (10,Double.NaN), (20,Double.NaN), (30,3.0)))
      case None           => fail("Expected to find a matching RV for key Map(label2 -> value2, onLabel -> onValue1)")
    }

    map.get(Map( "label1".utf8 -> "value1".utf8, "onLabel".utf8 -> "onValue1".utf8)) match {
      case Some(matched)       => matched shouldEqual Seq((100, 1.0), (200, 2.0), (300, 3.0))
      case None                => fail("Expected to find a matching RV for key Map(label1 -> value1, onLabel -> onValue1)")
    }
  }

  it("should perform A AND B when no on is provided") {
    val exec = SetOperatorExec(QueryContext(), dummyDispatcher, Nil, Nil, BinaryOperator.LAND, Nil, Nil, "_metric_", None)

    val lhsRv = rangeVectors(List(
      (Map( "label1".utf8 -> "value1".utf8)-> Seq((0, Double.NaN), (10, 1.0), (20, Double.NaN))),
      (Map( "label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((0, 1.0), (10, 2.0), (20, 3.0))),
      (Map( "label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((10, 1.0), (20, 2.0), (30, 3.0))),
      (Map( "label1".utf8 -> "value1".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((100, 1.0), (200, 2.0), (300, 3.0)))
    ))
    val rhsRv = rangeVectors(List(
      (Map( "label1".utf8 -> "value1".utf8)-> Seq((100, Double.NaN), (110, 1.0), (120, Double.NaN)))
    ))

    val map = exec.setOpAnd(lhsRv, rhsRv, resultSchema).map( rv => rv.key.labelValues -> rvRowsToListOfTuples(rv)).toMap
    map.size shouldBe 1
    map.get(Map("label1".utf8 -> "value1".utf8)) match {
      case Some(matched)  => assertListEquals(matched, List((0, Double.NaN), (10, 1.0), (20,Double.NaN)))
      case None           => fail("Expected to find a matching RV for key Map(label1 -> value1)")
    }
  }

  it("should perform A AND B when on is provided") {
    val exec = SetOperatorExec(QueryContext(), dummyDispatcher, Nil, Nil, BinaryOperator.LAND, Seq("onLabel"), Nil, "_metric_", None)

    val lhsRv = rangeVectors(List(
      (Map( "label1".utf8 -> "value1".utf8)-> Seq((10, Double.NaN), (20, 1.0), (30, 2.0))),
      (Map( "label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((10, 1.0), (20, 2.0), (30, 3.0))),
      (Map( "label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((10, 1.0), (20, 2.0), (30, 4.0))),
      (Map( "label1".utf8 -> "value1".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((10, 1.0), (20, 2.0), (30, 3.0))),
      (Map( "label1".utf8 -> "value1".utf8, "onLabel".utf8 -> "onValue2".utf8)-> Seq((10, 1.0), (20, 2.0), (30, 3.0)))
    ))
    val rhsRv = rangeVectors(List(
      (Map( "label1".utf8 -> "value2".utf8)-> Seq((10, Double.NaN), (20, 1.0), (30, Double.NaN))),
      (Map( "label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((10, Double.NaN), (20, 2.0), (30, 3.0))),
    ))

    val map = exec.setOpAnd(lhsRv, rhsRv, resultSchema).map( rv => rv.key.labelValues -> rvRowsToListOfTuples(rv)).toMap
    map.size shouldBe 3
    // Since on is give, all RVs with empty join keys will be present in the results
    map.get(Map("label1".utf8 -> "value1".utf8)) match {
      case Some(matched)  => assertListEquals(matched, List((10, Double.NaN), (20, 1.0), (30,Double.NaN)))
      case None           => fail("Expected to find a matching RV for key Map(label1 -> value1)")
    }

    map.get(Map("label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)) match {
      case Some(matched)  => assertListEquals(matched, List((10,1.0), (20,2.0), (30,Double.NaN)))
      case None           => fail("Expected to find a matching RV for key Map(label2 -> value2, onLabel -> onValue1)")
    }

    map.get(Map( "label1".utf8 -> "value1".utf8, "onLabel".utf8 -> "onValue1".utf8)) match {
      case Some(matched)       => assertListEquals(matched , List((10, Double.NaN), (20, 2.0), (30, 3.0)))
      case None                => fail("Expected to find a matching RV for key Map(label1 -> value1, onLabel -> onValue1)")
    }
  }

  it("should perform A AND B when ignoring is provided") {
    val exec = SetOperatorExec(QueryContext(), dummyDispatcher, Nil, Nil, BinaryOperator.LAND, Nil, Seq("label1", "label2"), "_metric_", None)
    // This is equivalent to providing on for onLabel

    val lhsRv = rangeVectors(List(
      (Map( "label1".utf8 -> "value1".utf8)-> Seq((10, Double.NaN), (20, 1.0), (30, 2.0))),
      (Map( "label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((10, 1.0), (20, 2.0), (30, 3.0))),
      (Map( "label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((10, 1.0), (20, 2.0), (30, 4.0))),
      (Map( "label1".utf8 -> "value1".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((10, 1.0), (20, 2.0), (30, 3.0))),
      (Map( "label1".utf8 -> "value1".utf8, "onLabel".utf8 -> "onValue2".utf8)-> Seq((10, 1.0), (20, 2.0), (30, 3.0)))
    ))
    val rhsRv = rangeVectors(List(
      (Map( "label1".utf8 -> "value2".utf8)-> Seq((10, Double.NaN), (20, 1.0), (30, Double.NaN))),
      (Map( "label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)-> Seq((10, Double.NaN), (20, 2.0), (30, 3.0))),
    ))

    val map = exec.setOpAnd(lhsRv, rhsRv, resultSchema).map( rv => rv.key.labelValues -> rvRowsToListOfTuples(rv)).toMap
    map.size shouldBe 3
    // Since on is give, all RVs with empty join keys will be present in the results
    map.get(Map("label1".utf8 -> "value1".utf8)) match {
      case Some(matched)  => assertListEquals(matched, List((10, Double.NaN), (20, 1.0), (30,Double.NaN)))
      case None           => fail("Expected to find a matching RV for key Map(label1 -> value1)")
    }

    map.get(Map("label2".utf8 -> "value2".utf8, "onLabel".utf8 -> "onValue1".utf8)) match {
      case Some(matched)  => assertListEquals(matched, List((10,1.0), (20,2.0), (30,Double.NaN)))
      case None           => fail("Expected to find a matching RV for key Map(label2 -> value2, onLabel -> onValue1)")
    }

    map.get(Map( "label1".utf8 -> "value1".utf8, "onLabel".utf8 -> "onValue1".utf8)) match {
      case Some(matched)       => assertListEquals(matched , List((10, Double.NaN), (20, 2.0), (30, 3.0)))
      case None                => fail("Expected to find a matching RV for key Map(label1 -> value1, onLabel -> onValue1)")
    }
  }

  def assertListEquals(l1: List[(Long, Double)], l2: List[(Long, Double)]): Boolean =
    l1.length == l2.length && (l1 zip l1).forall{
      case ((t1, Double.NaN), (t2, Double.NaN))    =>  t1 == t2
      case ((t1, v1), (t2, v2))                    =>  t1 == t2 && v1 == v2
      case _                                       => false
    }


}
// scalastyle:on number.of.methods
