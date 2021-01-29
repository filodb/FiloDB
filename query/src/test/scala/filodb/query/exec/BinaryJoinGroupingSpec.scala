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
import filodb.memory.format.ZeroCopyUTF8String
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._
import filodb.query.exec.aggregator.RowAggregator
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BinaryJoinGroupingSpec extends AnyFunSpec with Matchers with ScalaFutures {

  import MultiSchemaPartitionsExecSpec._

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)

  val tvSchema = ResultSchema(Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn)), 1)
  val schema = Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn))
  val tvSchemaTask = Task.now(tvSchema)

  val rand = new Random()

  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlan)
                         (implicit sched: Scheduler): Task[QueryResponse] = ???
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
    }
  )

  it("should join many-to-one with on ") {
    val samplesRhs2 = scala.util.Random.shuffle(sampleNodeRole.toList) // they may come out of order

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.MUL,
      Cardinality.ManyToOne,
      Seq("instance"), Nil, Seq("role"), "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleNodeCpu.map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs2.map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runAsync.futureValue

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
      Nil, Seq("role", "mode"), Seq("role"), "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleNodeCpu.map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs2.map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runAsync.futureValue

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
    val aggMR = AggregateMapReduce(AggregationOperator.Sum, Nil, Nil, Seq("instance", "job"))
    val mapped = aggMR(Observable.fromIterable(sampleNodeCpu), querySession, 1000, tvSchema)

    val resultObs4 = RangeVectorAggregator.mapReduce(agg, true, mapped, rv=>rv.key)
    val samplesRhs = resultObs4.toListL.runAsync.futureValue

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.DIV,
      Cardinality.ManyToOne,
      Seq("instance"), Nil, Nil, "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleNodeCpu.map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs.map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runAsync.futureValue

    val expectedLabels = List(
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("def"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
        ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("idle")
      ),
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("def"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
        ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("user")
      ),
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("abc"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
        ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("idle")
      ),
      Map(ZeroCopyUTF8String("instance") -> ZeroCopyUTF8String("abc"),
        ZeroCopyUTF8String("job") -> ZeroCopyUTF8String("node"),
        ZeroCopyUTF8String("mode") -> ZeroCopyUTF8String("user")
      )
    )
    result.size shouldEqual 4
    result.map(_.key.labelValues) sameElements(expectedLabels) shouldEqual true

    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(0.8)
    result(1).rows.map(_.getDouble(1)).toList shouldEqual List(0.2)
    result(2).rows.map(_.getDouble(1)).toList shouldEqual List(0.75)
    result(3).rows.map(_.getDouble(1)).toList shouldEqual List(0.25)
  }

  it("copy sample role to node using group right ") {

    val samplesRhs2 = scala.util.Random.shuffle(sampleNodeVar.toList) // they may come out of order

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.MUL,
      Cardinality.OneToMany,
      Nil, Seq("role"),
      Seq("role"), "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleNodeRole.map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs2.map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runAsync.futureValue

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
    val aggMR = AggregateMapReduce(AggregationOperator.Sum, Nil, Nil, Seq("instance", "job"))
    val mapped = aggMR(Observable.fromIterable(sampleNodeCpu), querySession, 1000, tvSchema)

    val resultObs4 = RangeVectorAggregator.mapReduce(agg, true, mapped, rv=>rv.key)
    val samplesRhs = resultObs4.toListL.runAsync.futureValue

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.DIV,
      Cardinality.ManyToOne, Nil,
      Seq("mode"), Seq("dummy"), "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleNodeCpu.map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs.map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runAsync.futureValue

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

    result(0).rows.map(_.getDouble(1)).toList shouldEqual List(0.8)
    result(1).rows.map(_.getDouble(1)).toList shouldEqual List(0.2)
    result(2).rows.map(_.getDouble(1)).toList shouldEqual List(0.75)
    result(3).rows.map(_.getDouble(1)).toList shouldEqual List(0.25)
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
      }
    )

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.GTR,
      Cardinality.ManyToOne,
      Seq("instance"), Nil, Seq("role"), "metric")

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleLhs.map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, sampleRhs.map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runAsync.futureValue

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
    result.map(_.key.labelValues) sameElements(expectedLabels) shouldEqual true
  }

  it("should throw BadQueryException - many-to-one with on - cardinality limit 1") {
    val queryContext = QueryContext(plannerParams= PlannerParams(joinQueryCardLimit = 1)) // set join card limit to 1
    val samplesRhs2 = scala.util.Random.shuffle(sampleNodeRole.toList) // they may come out of order

    val execPlan = BinaryJoinExec(queryContext, dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.MUL,
      Cardinality.ManyToOne,
      Seq("instance"), Nil, Seq("role"), "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleNodeCpu.map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs2.map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on

    // actual query results into 2 rows. since limit is 1, this results in BadQueryException
    val thrown = intercept[TestFailedException] {
      execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
        .toListL.runAsync.futureValue
    }

    thrown.getCause.getClass shouldEqual classOf[BadQueryException]
    thrown.getCause.getMessage shouldEqual "This query results in more than 1 join cardinality." +
      " Try applying more filters."
  }

  it("should throw BadQueryException - many-to-one with ignoring - cardinality limit 1") {
    val queryContext = QueryContext(plannerParams= PlannerParams(joinQueryCardLimit = 1)) // set join card limit to 1
    val samplesRhs2 = scala.util.Random.shuffle(sampleNodeRole.toList) // they may come out of order

    val execPlan = BinaryJoinExec(queryContext, dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.MUL,
      Cardinality.ManyToOne,
      Nil, Seq("role", "mode"), Seq("role"), "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleNodeCpu.map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs2.map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on

    // actual query results into 2 rows. since limit is 1, this results in BadQueryException
    val thrown = intercept[TestFailedException] {
      execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
        .toListL.runAsync.futureValue
    }

    thrown.getCause.getClass shouldEqual classOf[BadQueryException]
    thrown.getCause.getMessage shouldEqual "This query results in more than 1 join cardinality." +
      " Try applying more filters."
  }

  it("should throw BadQueryException - many-to-one with by and grouping without arguments - cardinality limit 1") {
    val queryContext = QueryContext(plannerParams= PlannerParams(joinQueryCardLimit = 3)) // set join card limit to 3
    val agg = RowAggregator(AggregationOperator.Sum, Nil, tvSchema)
    val aggMR = AggregateMapReduce(AggregationOperator.Sum, Nil, Nil, Seq("instance", "job"))
    val mapped = aggMR(Observable.fromIterable(sampleNodeCpu), querySession, 1000, tvSchema)

    val resultObs4 = RangeVectorAggregator.mapReduce(agg, true, mapped, rv=>rv.key)
    val samplesRhs = resultObs4.toListL.runAsync.futureValue

    val execPlan = BinaryJoinExec(queryContext, dummyDispatcher,
      Array(dummyPlan),
      new Array[ExecPlan](1),
      BinaryOperator.DIV,
      Cardinality.ManyToOne,
      Seq("instance"), Nil, Nil, "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, sampleNodeCpu.map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs.map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on

    // actual query results into 4 rows. since limit is 3, this results in BadQueryException
    val thrown = intercept[TestFailedException] {
      execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
        .toListL.runAsync.futureValue
    }

    thrown.getCause.getClass shouldEqual classOf[BadQueryException]
    thrown.getCause.getMessage shouldEqual "This query results in more than 3 join cardinality." +
      " Try applying more filters."
  }
}
