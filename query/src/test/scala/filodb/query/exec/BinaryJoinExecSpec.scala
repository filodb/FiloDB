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
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BinaryJoinExecSpec extends AnyFunSpec with Matchers with ScalaFutures {
  import MultiSchemaPartitionsExecSpec._

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)
  val rand = new Random()
  val error = 0.00000001d

  val tvSchema = ResultSchema(Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn)), 1)
  val schema = Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn))
  val tvSchemaTask = Task.now(tvSchema)

  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlan)
                         (implicit sched: Scheduler): Task[QueryResponse] = ???
  }

  private def data(i: Int) = Stream.from(0).map(n => new TransientRow(n.toLong, i.toDouble)).take(20)

  val samplesLhs: Array[RangeVector] = Array.tabulate(200) { i =>
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricLhs".utf8,
          "tag1".utf8 -> s"tag1-$i".utf8,
          "tag2".utf8 -> s"tag2-$i".utf8))
      import NoCloseCursor._
      val rows: RangeVectorCursor = data(i).iterator
    }
  }

  val samplesRhs: Array[RangeVector] = Array.tabulate(200) { i =>
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricRhs".utf8,
          "tag1".utf8 -> samplesLhs(i).key.labelValues("tag1".utf8),
          "tag2".utf8 -> samplesLhs(i).key.labelValues("tag2".utf8)))
      import NoCloseCursor._
      val rows: RangeVectorCursor = data(i).iterator
    }
  }

  val samplesLhsGrouping: Array[RangeVector] = Array.tabulate(2) { i =>
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricLhs".utf8,
          "tag1".utf8 -> s"tag1-$i".utf8,
          "tag2".utf8 -> s"tag2-1".utf8,
          "job".utf8 -> s"somejob".utf8))
      import NoCloseCursor._
      val rows: RangeVectorCursor = data(i).iterator
    }
  }

  val samplesRhsGrouping: Array[RangeVector] = Array.tabulate(2) { i =>
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricRhs".utf8,
          "tag1".utf8 -> s"tag1-$i".utf8,
          "job".utf8 -> s"somejob".utf8))
      import NoCloseCursor._
      val rows: RangeVectorCursor = data(i).iterator
    }
  }

  it("should join one-to-one without on or ignoring") {

    val samplesRhs2 = scala.util.Random.shuffle(samplesRhs.toList) // they may come out of order

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),       // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.ADD,
      Cardinality.OneToOne,
      Nil, Nil, Nil, "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, samplesLhs.map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs2.map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on
    // note below that order of lhs and rhs is reversed, but index is right. Join should take that into account
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
                         .toListL.runAsync.futureValue

    result.foreach { rv =>
      rv.key.labelValues.contains("__name__".utf8) shouldEqual false
      rv.key.labelValues.contains("tag1".utf8) shouldEqual true
      rv.key.labelValues.contains("tag2".utf8) shouldEqual true
      val i = rv.key.labelValues("tag1".utf8).asNewString.split("-")(1)
      rv.rows.map(_.getDouble(1)).foreach(_ shouldEqual i.toDouble * 2)
    }

    result.map(_.key).toSet.size shouldEqual 200
  }

  it("should join one-to-one without on or ignoring with missing elements on any side") {

    val samplesRhs2 = scala.util.Random.shuffle(samplesRhs.take(100).toList) // they may come out of order

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.ADD,
      Cardinality.OneToOne,
      Nil, Nil, Nil, "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, samplesLhs.map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs2.map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((lhs, 0), (rhs, 1))), tvSchemaTask, querySession)
                         .toListL.runAsync.futureValue

    result.foreach { rv =>
      rv.key.labelValues.contains("__name__".utf8) shouldEqual false
      rv.key.labelValues.contains("tag1".utf8) shouldEqual true
      rv.key.labelValues.contains("tag2".utf8) shouldEqual true
      val i = rv.key.labelValues("tag1".utf8).asNewString.split("-")(1)
      rv.rows.map(_.getDouble(1)).foreach(_ shouldEqual i.toDouble * 2)
    }

    result.map(_.key).toSet.size shouldEqual 100
  }

  it("should deal with additional step and pi tag as join key on OneToOne joins") {
    val lhs1: RangeVector = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricLhs".utf8, "_pi_".utf8 -> "0".utf8, "tag2".utf8 -> "tag2Val".utf8))
      import NoCloseCursor._
      val rows: RangeVectorCursor = data(2).iterator
    }

    val lhs2: RangeVector = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricLhs".utf8, "_step_".utf8 -> "0".utf8, "tag2".utf8 -> "tag2Val".utf8))
      import NoCloseCursor._
      val rows: RangeVectorCursor = data(2).iterator
    }

    val rhs1: RangeVector = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricRhs".utf8,"_pi_".utf8 -> "0".utf8, "tag2".utf8 -> "tag2Val".utf8))
      import NoCloseCursor._
      val rows: RangeVectorCursor = data(2).iterator
    }

    val rhs2: RangeVector = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricRhs".utf8, "_step_".utf8 -> "0".utf8, "tag2".utf8 -> "tag2Val".utf8))
      import NoCloseCursor._
      val rows: RangeVectorCursor = data(2).iterator
    }

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),       // cannot be empty as some compose's rely on the schema
      Array(dummyPlan), // empty since we test compose, not execute or doExecute
      BinaryOperator.ADD,
      Cardinality.OneToOne,
      Seq("_step_", "_pi_"), Nil, Nil, "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, Seq(lhs1, lhs2).map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, Seq(rhs1, rhs2).map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on

    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runAsync.futureValue

    result.size shouldEqual 2
    result(0).key.labelValues.contains("_pi_".utf8) shouldEqual true
    result(1).key.labelValues.contains("_step_".utf8) shouldEqual true

  }

  it("should deal with implictly added step and pi tag as join key on OneToMany joins") {
    val lhs1: RangeVector = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricLhs".utf8, "_pi_".utf8 -> "0".utf8, "tag2".utf8 -> "tag2Val".utf8))
      import NoCloseCursor._
      val rows: RangeVectorCursor = data(2).iterator
    }

    val lhs2: RangeVector = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricLhs".utf8, "_step_".utf8 -> "0".utf8, "tag2".utf8 -> "tag2Val".utf8))
      import NoCloseCursor._
      val rows: RangeVectorCursor = data(2).iterator
    }

    val rhs1: RangeVector = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricRhs".utf8, "_pi_".utf8 -> "0".utf8,
          "tag2".utf8 -> "tag2Val".utf8, "tag1".utf8 -> "tag1Val1".utf8))
      import NoCloseCursor._
      val rows: RangeVectorCursor = data(2).iterator
    }

    val rhs2: RangeVector = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricRhs".utf8, "_step_".utf8 -> "0".utf8,
          "tag2".utf8 -> "tag2Val".utf8, "tag1".utf8 -> "tag1Val1".utf8))
      import NoCloseCursor._
      val rows: RangeVectorCursor = data(2).iterator
    }

    val rhs3: RangeVector = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricRhs".utf8, "_pi_".utf8 -> "0".utf8,
          "tag2".utf8 -> "tag2Val".utf8, "tag1".utf8 -> "tag1Val2".utf8))
      import NoCloseCursor._
      val rows: RangeVectorCursor = data(2).iterator
    }

    val rhs4: RangeVector = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricRhs".utf8, "_step_".utf8 -> "0".utf8,
          "tag2".utf8 -> "tag2Val".utf8, "tag1".utf8 -> "tag1Val2".utf8))
      import NoCloseCursor._
      val rows: RangeVectorCursor = data(2).iterator
    }

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),       // cannot be empty as some compose's rely on the schema
      Array(dummyPlan), // empty since we test compose, not execute or doExecute
      BinaryOperator.ADD,
      Cardinality.OneToMany,
      Nil, ignoring = Seq("tag1"), include = Seq("tag2"), "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, Seq(lhs1, lhs2).map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, Seq(rhs1, rhs2, rhs3, rhs4).map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on

    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runAsync.futureValue

    result.size shouldEqual 4
    Seq("_pi_".utf8, "tag1".utf8).forall(result(0).key.labelValues.contains) shouldEqual true
    Seq("_step_".utf8, "tag1".utf8).forall(result(1).key.labelValues.contains) shouldEqual true
    Seq("_pi_".utf8, "tag1".utf8).forall(result(2).key.labelValues.contains) shouldEqual true
    Seq("_step_".utf8, "tag1".utf8).forall(result(3).key.labelValues.contains) shouldEqual true
  }

  it("should throw error if OneToOne cardinality passed, but OneToMany") {

    val duplicate: RangeVector = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricRhs".utf8,
          "tag1".utf8 -> "tag1-uniqueValue".utf8,
          "tag2".utf8 -> samplesLhs(2).key.labelValues("tag2".utf8))) // duplicate value
      import NoCloseCursor._
      val rows: RangeVectorCursor = data(2).iterator
    }

    val samplesRhs2 = scala.util.Random.shuffle(duplicate +: samplesRhs.toList) // they may come out of order
    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.ADD,
      Cardinality.OneToOne,
      Nil, Seq("tag1"), Nil, "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, samplesLhs.map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs2.map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on

    val fut = execPlan.compose(Observable.fromIterable(Seq((lhs, 0), (rhs, 1))), tvSchemaTask, querySession)
                      .toListL.runAsync
    ScalaFutures.whenReady(fut.failed) { e =>
      e shouldBe a[BadQueryException]
    }
  }

  it("should throw error if OneToOne cardinality passed, but ManyToOne") {

    val duplicate: RangeVector = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricRhs".utf8,
          "tag1".utf8 -> "tag1-uniqueValue".utf8,
          "tag2".utf8 -> samplesLhs(2).key.labelValues("tag2".utf8))) // duplicate value
      import NoCloseCursor._
      val rows: RangeVectorCursor = data(2).iterator
    }

    val samplesLhs2 = scala.util.Random.shuffle(duplicate +: samplesLhs.toList) // they may come out of order

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.ADD,
      Cardinality.OneToOne,
      Nil, Seq("tag1"), Nil, "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, samplesLhs2.map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs.map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on

    val fut = execPlan.compose(Observable.fromIterable(Seq((lhs, 0), (rhs, 1))), tvSchemaTask, querySession)
                      .toListL.runAsync
    ScalaFutures.whenReady(fut.failed) { e =>
      e.printStackTrace()
      e shouldBe a[BadQueryException]
    }
  }
  it("should join one-to-one with ignoring") {

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.ADD,
      Cardinality.OneToOne,
      Nil, Seq("tag2"), Nil, "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, samplesLhsGrouping.map(rv => SerializedRangeVector(rv, schema)))
    // val lhs = QueryResult("someId", null, samplesLhs.filter(rv => rv.key.labelValues.get(ZeroCopyUTF8String("tag2")).get.equals("tag1-1")).map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhsGrouping.map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on
    // note below that order of lhs and rhs is reversed, but index is right. Join should take that into account
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runAsync.futureValue

    result.foreach { rv =>
      rv.key.labelValues.contains("__name__".utf8) shouldEqual false
      rv.key.labelValues.contains("tag1".utf8) shouldEqual true
      rv.key.labelValues.contains("tag2".utf8) shouldEqual false
      val i = rv.key.labelValues("tag1".utf8).asNewString.split("-")(1)
      rv.rows.map(_.getDouble(1)).foreach(_ shouldEqual i.toDouble * 2)
    }

    result.map(_.key).toSet.size shouldEqual 2
  }

  it("should join one-to-one with on") {

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.ADD,
      Cardinality.OneToOne,
      Seq("tag1", "job"), Nil, Nil, "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, samplesLhsGrouping.map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhsGrouping.map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on
    // note below that order of lhs and rhs is reversed, but index is right. Join should take that into account
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runAsync.futureValue

    result.foreach { rv =>
      rv.key.labelValues.contains("__name__".utf8) shouldEqual false
      rv.key.labelValues.contains("tag1".utf8) shouldEqual true
      rv.key.labelValues.contains("tag2".utf8) shouldEqual false
      val i = rv.key.labelValues("tag1".utf8).asNewString.split("-")(1)
      rv.rows.map(_.getDouble(1)).foreach(_ shouldEqual i.toDouble * 2)
    }

    result.map(_.key).toSet.size shouldEqual 2
  }
  it("should join one-to-one when metric name is not _name_") {

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan),       // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.ADD,
      Cardinality.OneToOne,
      Nil, Nil, Nil, "metric")

    val samplesLhs: Array[RangeVector] = Array.tabulate(200) { i =>
      new RangeVector {
        val key: RangeVectorKey = CustomRangeVectorKey(
          Map("metric".utf8 -> s"someMetricLhs".utf8,
            "tag1".utf8 -> s"tag1-$i".utf8,
            "tag2".utf8 -> s"tag2-$i".utf8))
        import NoCloseCursor._
        val rows: RangeVectorCursor = data(i).iterator
      }
    }

    val samplesRhs: Array[RangeVector] = Array.tabulate(200) { i =>
      new RangeVector {
        val key: RangeVectorKey = CustomRangeVectorKey(
          Map("metric".utf8 -> s"someMetricRhs".utf8,
            "tag1".utf8 -> samplesLhs(i).key.labelValues("tag1".utf8),
            "tag2".utf8 -> samplesLhs(i).key.labelValues("tag2".utf8)))
        import NoCloseCursor._
        val rows: RangeVectorCursor = data(i).iterator
      }
    }

    val samplesRhs2 = scala.util.Random.shuffle(samplesRhs.toList) // they may come out of order
    // scalastyle:off
    val lhs = QueryResult("someId", null, samplesLhs.map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs2.map(rv => SerializedRangeVector(rv, schema)))
    // scalastyle:on
    // note below that order of lhs and rhs is reversed, but index is right. Join should take that into account
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runAsync.futureValue

    result.foreach { rv =>
      rv.key.labelValues.contains("metric".utf8) shouldEqual false
      rv.key.labelValues.contains("tag1".utf8) shouldEqual true
      rv.key.labelValues.contains("tag2".utf8) shouldEqual true
      val i = rv.key.labelValues("tag1".utf8).asNewString.split("-")(1)
      rv.rows.map(_.getDouble(1)).foreach(_ shouldEqual i.toDouble * 2)
    }

    result.map(_.key).toSet.size shouldEqual 200
  }

  it("should have metric name when operator is not MathOperator") {

    val samplesLhs: Array[RangeVector] = Array.tabulate(200) { i =>
      new RangeVector {
        val key: RangeVectorKey = CustomRangeVectorKey(
          Map("metric".utf8 -> s"someMetricLhs".utf8,
            "tag1".utf8 -> s"tag1-$i".utf8,
            "tag2".utf8 -> s"tag2-$i".utf8))
        import NoCloseCursor._
        val rows: RangeVectorCursor = data(i).iterator
      }
    }

    val samplesRhs: Array[RangeVector] = Array.tabulate(200) { i =>
      new RangeVector {
        val key: RangeVectorKey = CustomRangeVectorKey(
          Map("metric".utf8 -> s"someMetricRhs".utf8,
            "tag1".utf8 -> samplesLhs(i).key.labelValues("tag1".utf8),
            "tag2".utf8 -> samplesLhs(i).key.labelValues("tag2".utf8)))
        import NoCloseCursor._
        val rows: RangeVectorCursor = data(i).iterator
      }
    }

    val execPlan = BinaryJoinExec(QueryContext(), dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.GTR,
      Cardinality.OneToOne,
      Nil, Seq("tag2"), Nil, "metric")

    // scalastyle:off
    val lhs = QueryResult("someId", null, samplesLhs.map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs.map(rv => SerializedRangeVector (rv, schema)))
    // scalastyle:on
    // note below that order of lhs and rhs is reversed, but index is right. Join should take that into account
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), tvSchemaTask, querySession)
      .toListL.runAsync.futureValue

    result.foreach { rv =>
      rv.key.labelValues.contains("metric".utf8) shouldEqual true
      rv.key.labelValues.contains("tag1".utf8) shouldEqual true
      rv.key.labelValues.contains("tag2".utf8) shouldEqual false
    }

    result.map(_.key).toSet.size shouldEqual 200
  }

  it("should throw BadQueryException - one-to-one with ignoring - cardinality limit 1") {
    val queryContext = QueryContext(joinQueryCardLimit = 1) // set join card limit to 1
    val execPlan = BinaryJoinExec(queryContext, dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.ADD,
      Cardinality.OneToOne,
      Nil, Seq("tag2"), Nil, "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, samplesLhsGrouping.map(rv => SerializedRangeVector(rv, schema)))
    // val lhs = QueryResult("someId", null, samplesLhs.filter(rv => rv.key.labelValues.get(ZeroCopyUTF8String("tag2")).get.equals("tag1-1")).map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhsGrouping.map(rv => SerializedRangeVector(rv, schema)))
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

  it("should throw BadQueryException - one-to-one with on - cardinality limit 1") {
    val queryContext = QueryContext(joinQueryCardLimit = 1) // set join card limit to 1
    val execPlan = BinaryJoinExec(queryContext, dummyDispatcher,
      Array(dummyPlan), // cannot be empty as some compose's rely on the schema
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.ADD,
      Cardinality.OneToOne,
      Seq("tag1", "job"), Nil, Nil, "__name__")

    // scalastyle:off
    val lhs = QueryResult("someId", null, samplesLhsGrouping.map(rv => SerializedRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhsGrouping.map(rv => SerializedRangeVector(rv, schema)))
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
}
