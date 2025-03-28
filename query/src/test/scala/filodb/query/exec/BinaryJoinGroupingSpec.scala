package filodb.query.exec

import scala.util.Random

import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.exceptions.TestFailedException

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.core.store.ChunkSource
import filodb.memory.format.ZeroCopyUTF8String
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._
import filodb.query.exec.aggregator.RowAggregator
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BinaryJoinGroupingSpec extends AnyFunSpec with Matchers with ScalaFutures {

  import MultiSchemaPartitionsExecSpec._

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)

  val tvSchema = ResultSchema(Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn)), 1)
  val schema = Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn))
  val tvSchemaTask = Task.eval(tvSchema)
  val queryStats = QueryStats()

  val rand = new Random()

  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlanWithClientParams, source: ChunkSource)
                         (implicit sched: Scheduler): Task[QueryResponse] = ???

    override def clusterName: String = ???

    override def isLocalCall: Boolean = true
    override def dispatchStreaming(plan: ExecPlanWithClientParams,
                                   source: ChunkSource)(implicit sched: Scheduler): Observable[StreamQueryResponse] = ???
  }

  val sampleNodeCpu: Array[RangeVector] = Array(
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"node_cpu".utf8,
          "instance".utf8 -> "abc".utf8,
          "job".utf8 -> s"node".utf8,
          "mode".utf8 -> s"idle".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 3)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"node_cpu".utf8,
          "instance".utf8 -> "abc".utf8,
          "job".utf8 -> s"node".utf8,
          "mode".utf8 -> s"user".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 1)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"node_cpu".utf8,
          "instance".utf8 -> "def".utf8,
          "job".utf8 -> s"node".utf8,
          "mode".utf8 -> s"idle".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 8)).iterator
      override def outputRange: Option[RvRange] = None
    },
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"node_cpu".utf8,
          "instance".utf8 -> "def".utf8,
          "job".utf8 -> s"node".utf8,
          "mode".utf8 -> s"user".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 2)).iterator
      override def outputRange: Option[RvRange] = None
    }
  )
  val sampleNodeRole: Array[RangeVector] = Array(
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"node_role".utf8,
          "instance".utf8 -> "abc".utf8,
            "job".utf8 -> "node".utf8,
          "role".utf8 -> s"prometheus".utf8)
      )

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 1)).iterator
      override def outputRange: Option[RvRange] = None
    }
  )

  val sampleNodeVar: Array[RangeVector] = Array(
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"node_var".utf8,
          "instance".utf8 -> "abc".utf8,
          "job".utf8 -> "node".utf8
      ))

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 2)).iterator
      override def outputRange: Option[RvRange] = None
    }
  )

  it("should join many-to-one with on ") {
    val samplesRhs2 = scala.util.Random.shuffle(sampleNodeRole.toList) // they may come out of order

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.MUL,
      Cardinality.ManyToOne,
      Some(Seq("instance")), Nil, Seq("role"), "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleNodeCpu.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", null, samplesRhs2.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("abc"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
      ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("idle"),
      ZeroCopyUTF8String("role") -> ZeroCopyUTF8String("prometheus")
    ),
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("abc"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
        ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("user"),
        ZeroCopyUTF8String("role") -> ZeroCopyUTF8String("prometheus"))
    )

    result.size shouldEqual 2
    result.map(_.key.labelValues) sameElements(expectedLabels) shouldEqual true
    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(3)
    result(1).rows.map(_.getDouble(1)).toList shouldEqual List(1)
  }

  it("should join many-to-one with ignoring ") {

    val samplesRhs2 = scala.util.Random.shuffle(sampleNodeRole.toList) // they may come out of order

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.MUL,
      Cardinality.ManyToOne,
      None, Seq("role", "mode"), Seq("role"), "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleNodeCpu.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", null, samplesRhs2.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("abc"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
      ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("idle"),
      ZeroCopyUTF8String("role") -> ZeroCopyUTF8String("prometheus")
    ),
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("abc"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
        ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("user"),
        ZeroCopyUTF8String("role") -> ZeroCopyUTF8String("prometheus"))
    )

    result.size shouldEqual 2
    result.map(_.key.labelValues) sameElements(expectedLabels) shouldEqual true
    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(3)
    result(1).rows.map(_.getDouble(1)).toList shouldEqual List(1)
  }

  it("should join many-to-one with by and grouping without arguments") {

    val agg = RowAggregator(AggregationOperator.Sum, Nil, tvSchema)
    val aggMR = AggregateMapReduce(AggregationOperator.Sum, Nil,
                                   AggregateClause.byOpt(Seq("instance", "job")))
    val mapped = aggMR(Observable.fromIterable(sampleNodeCpu), querySession, 1000, tvSchema)

    val resultObs4 = RangeVectorAggregator.mapReduce(agg, true, mapped, rv=>rv.key, queryContext = QueryContext(), QueryWarnings())
    val samplesRhs = resultObs4.toListL.runToFuture.futureValue

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.DIV,
      Cardinality.ManyToOne,
      Some(Seq("instance")), Nil, Nil, "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleNodeCpu.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", null, samplesRhs.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("abc"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
      ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("idle")
    ),
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("abc"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
        ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("user")
    ),
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("def"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
        ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("idle")
      ),
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("def"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
        ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("user")
      )
    )
    result.size shouldEqual 4
    result.map(_.key.labelValues).toSet shouldEqual expectedLabels.toSet

    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(0.75)
    result(1).rows.map(_.getDouble(1)).toList shouldEqual List(0.25)
    result(2).rows.map(_.getDouble(1)).toList shouldEqual List(0.2)
    result(3).rows.map(_.getDouble(1)).toList shouldEqual List(0.8)
  }

  it("copy sample role to node using group right ") {

    val samplesRhs2 = scala.util.Random.shuffle(sampleNodeVar.toList) // they may come out of order

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.MUL,
      Cardinality.OneToMany,
      None, Seq("role"),
      Seq("role"), "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleNodeRole.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", null, samplesRhs2.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("abc"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
      ZeroCopyUTF8String("role") -> ZeroCopyUTF8String("prometheus")
    ))

    result.size shouldEqual 1
    result.map(_.key.labelValues) sameElements(expectedLabels) shouldEqual true
    result.foreach(_.rows.size shouldEqual(1))
    result(0).rows.map(_.getDouble(1)).foreach(_ shouldEqual(2))
  }

  it("should join many-to-one when group left label does not exist") {

    val agg = RowAggregator(AggregationOperator.Sum, Nil, tvSchema)
    val aggMR = AggregateMapReduce(AggregationOperator.Sum, Nil,
                                   AggregateClause.byOpt(Seq("instance", "job")))
    val mapped = aggMR(Observable.fromIterable(sampleNodeCpu), querySession, 1000, tvSchema)

    val resultObs4 = RangeVectorAggregator.mapReduce(agg, true, mapped, rv=>rv.key, queryContext = QueryContext(), QueryWarnings())
    val samplesRhs = resultObs4.toListL.runToFuture.futureValue

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.DIV,
      Cardinality.ManyToOne, None,
      Seq("mode"), Seq("dummy"), "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleNodeCpu.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", null, samplesRhs.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("abc"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
      ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("idle")
    ),
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("abc"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
        ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("user")
      ),
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("def"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
        ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("idle")
      ),
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("def"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
        ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("user")
      )
    )
    result.size shouldEqual 4
    result.map(_.key.labelValues).toSet shouldEqual expectedLabels.toSet

    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(0.75)
    result(1).rows.map(_.getDouble(1)).toList shouldEqual List(0.25)
    result(2).rows.map(_.getDouble(1)).toList shouldEqual List(0.2)
    result(3).rows.map(_.getDouble(1)).toList shouldEqual List(0.8)
  }

  it("should have metric name when operator is not MathOperator") {

    val sampleLhs: Array[RangeVector] = Array(
      new RangeVector {
        val key: RangeVectorKey = CustomRangeVectorKey(
          Map("metric".utf8 -> s"node_cpu".utf8,
            "instance".utf8 -> "abc".utf8,
            "job".utf8 -> s"node".utf8,
            "mode".utf8 -> s"idle".utf8)
        )

        import NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(1L, 3)).iterator
        override def outputRange: Option[RvRange] = None
      },
      new RangeVector {
        val key: RangeVectorKey = CustomRangeVectorKey(
          Map("metric".utf8 -> s"node_cpu".utf8,
            "instance".utf8 -> "abc".utf8,
            "job".utf8 -> s"node".utf8,
            "mode".utf8 -> s"user".utf8)
        )

        import NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(1L, 1)).iterator
        override def outputRange: Option[RvRange] = None
      })

   val sampleRhs: Array[RangeVector] = Array(
      new RangeVector {
        val key: RangeVectorKey = CustomRangeVectorKey(
          Map("metric".utf8 -> s"node_role".utf8,
            "instance".utf8 -> "abc".utf8,
            "job".utf8 -> "node".utf8,
            "role".utf8 -> s"prometheus".utf8)
        )

        import NoCloseCursor._
        override def rows(): RangeVectorCursor = Seq(
          new TransientRow(1L, 1)).iterator
        override def outputRange: Option[RvRange] = None
      }
    )

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.GTR,
      Cardinality.ManyToOne,
      Some(Seq("instance")), Nil, Seq("role"), "metric", None)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleLhs.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", null, sampleRhs.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runToFuture.futureValue

    val expectedLabels = List(Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("abc"),
      ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
      ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("idle"),
      ZeroCopyUTF8String("role") -> ZeroCopyUTF8String("prometheus"),
      ZeroCopyUTF8String("metric") -> ZeroCopyUTF8String("node_cpu")
    ),
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("abc"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
        ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("user"),
        ZeroCopyUTF8String("role") -> ZeroCopyUTF8String("prometheus"),
        ZeroCopyUTF8String("metric") -> ZeroCopyUTF8String("node_cpu"))
    )

    result.size shouldEqual 2
    result.map(_.key.labelValues).toSet shouldEqual expectedLabels.toSet
  }

  it("should throw BadQueryException - many-to-one with on - cardinality limit 1") {
    // set join card limit to 1
    val queryContext =
      QueryContext(plannerParams= PlannerParams(enforcedLimits = PerQueryLimits(joinQueryCardinality = 1)))
    val samplesRhs2 = scala.util.Random.shuffle(sampleNodeRole.toList) // they may come out of order

    val execPlan = BinaryJoinExec(queryContext, dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.MUL,
      Cardinality.ManyToOne,
      Some(Seq("instance")), Nil, Seq("role"), "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleNodeCpu.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", null, samplesRhs2.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on

    // actual query results into 2 rows. since limit is 1, this results in BadQueryException
    val thrown = intercept[TestFailedException] {
      execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
        .toListL.runToFuture.futureValue
    }

    thrown.getCause.getClass shouldEqual classOf[QueryLimitException]
    thrown.getCause.getMessage.contains("The result of this join query has cardinality 1 and has reached the " +
      "limit of 1. Try applying more filters.") shouldBe true
  }

  it("should throw BadQueryException - many-to-one with ignoring - cardinality limit 1") {
    // set join card limit to 1
    val queryContext =
      QueryContext(plannerParams = PlannerParams(enforcedLimits = PerQueryLimits(joinQueryCardinality = 1)))
    val samplesRhs2 = scala.util.Random.shuffle(sampleNodeRole.toList) // they may come out of order

    val execPlan = BinaryJoinExec(queryContext, dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.MUL,
      Cardinality.ManyToOne,
      None, Seq("role", "mode"), Seq("role"), "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleNodeCpu.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", null, samplesRhs2.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on

    // actual query results into 2 rows. since limit is 1, this results in BadQueryException
    val thrown = intercept[TestFailedException] {
      execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
        .toListL.runToFuture.futureValue
    }

    thrown.getCause.getClass shouldEqual classOf[QueryLimitException]
    thrown.getCause.getMessage.contains("The result of this join query has cardinality 1 and " +
      "has reached the limit of 1. Try applying more filters.") shouldEqual true
  }

  it("should throw BadQueryException - many-to-one with by and grouping without arguments - cardinality limit 1") {
    // set join card limit to 3
    val queryContext =
    QueryContext(plannerParams = PlannerParams(enforcedLimits = PerQueryLimits(joinQueryCardinality = 3)))
    val agg = RowAggregator(AggregationOperator.Sum, Nil, tvSchema)
    val aggMR = AggregateMapReduce(AggregationOperator.Sum, Nil,
                                   AggregateClause.byOpt(Seq("instance", "job")))
    val mapped = aggMR(Observable.fromIterable(sampleNodeCpu), querySession, 1000, tvSchema)

    val resultObs4 = RangeVectorAggregator.mapReduce(agg, true, mapped, rv=>rv.key, queryContext = QueryContext(), QueryWarnings())
    val samplesRhs = resultObs4.toListL.runToFuture.futureValue

    val execPlan = BinaryJoinExec(queryContext, dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.DIV,
      Cardinality.ManyToOne,
      Some(Seq("instance")), Nil, Nil, "__name__", None)

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleNodeCpu.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    val rhs = QueryResult("someId", null, samplesRhs.map(rv => SerializedRangeVector(rv, schema, queryStats)))
    // scalastyle:on

    // actual query results into 4 rows. since limit is 3, this results in BadQueryException
    val thrown = intercept[TestFailedException] {
      execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
        .toListL.runToFuture.futureValue
    }

    thrown.getCause.getClass shouldEqual classOf[QueryLimitException]
    thrown.getCause.getMessage.contains(
      "The result of this join query has cardinality 3 and " +
        "has reached the limit of 3. Try applying more filters."
    ) shouldBe true

  }
}
