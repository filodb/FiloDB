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

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.RowReader
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query._

class BinaryJoinExecSpec extends FunSpec with Matchers with ScalaFutures {

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val rand = new Random()
  val error = 0.00000001d

  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlan)
                         (implicit sched: ExecutionContext,
                          timeout: FiniteDuration): Task[QueryResponse] = ???
  }

  private def data(i: Int) = Stream.from(0).map(n => new TransientRow(n.toLong, i.toDouble)).take(20)

  val samplesLhs: Array[RangeVector] = Array.tabulate(200) { i =>
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricLhs".utf8,
          "tag1".utf8 -> s"tag1-$i".utf8,
          "tag2".utf8 -> s"tag2-$i".utf8))
      val rows: Iterator[RowReader] = data(i).iterator
    }
  }

  val samplesRhs: Array[RangeVector] = Array.tabulate(200) { i =>
    new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricRhs".utf8,
          "tag1".utf8 -> samplesLhs(i).key.labelValues("tag1".utf8),
          "tag2".utf8 -> samplesLhs(i).key.labelValues("tag2".utf8)))
      val rows: Iterator[RowReader] = data(i).iterator
    }
  }

  it("should join one-to-one without on or ignoring") {

    val samplesRhs2 = scala.util.Random.shuffle(samplesRhs.toList) // they may come out of order

    val execPlan = BinaryJoinExec("someID", dummyDispatcher,
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.ADD,
      Cardinality.OneToOne,
      Nil, Nil)

    val schema = Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
      ColumnInfo("value", ColumnType.DoubleColumn))

    // scalastyle:off
    val lhs = QueryResult("someId", null, samplesLhs.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs2.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    // note below that order of lhs and rhs is reversed, but index is right. Join should take that into account
    val result = execPlan.compose(Observable.fromIterable(Seq((rhs, 1), (lhs, 0))), queryConfig).toListL.runAsync.futureValue

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

    val execPlan = BinaryJoinExec("someID", dummyDispatcher,
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.ADD,
      Cardinality.OneToOne,
      Nil, Nil)

    val schema = Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
      ColumnInfo("value", ColumnType.DoubleColumn))

    // scalastyle:off
    val lhs = QueryResult("someId", null, samplesLhs.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs2.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on
    val result = execPlan.compose(Observable.fromIterable(Seq((lhs, 0), (rhs, 1))), queryConfig).toListL.runAsync.futureValue

    result.foreach { rv =>
      rv.key.labelValues.contains("__name__".utf8) shouldEqual false
      rv.key.labelValues.contains("tag1".utf8) shouldEqual true
      rv.key.labelValues.contains("tag2".utf8) shouldEqual true
      val i = rv.key.labelValues("tag1".utf8).asNewString.split("-")(1)
      rv.rows.map(_.getDouble(1)).foreach(_ shouldEqual i.toDouble * 2)
    }

    result.map(_.key).toSet.size shouldEqual 100
  }

  it("should throw error if OneToOne cardinality passed, but OneToMany") {

    val duplicate: RangeVector = new RangeVector {
      val key: RangeVectorKey = CustomRangeVectorKey(
        Map("__name__".utf8 -> s"someMetricRhs".utf8,
          "tag1".utf8 -> "tag1-uniqueValue".utf8,
          "tag2".utf8 -> samplesLhs(2).key.labelValues("tag2".utf8))) // duplicate value
      val rows: Iterator[RowReader] = data(2).iterator
    }

    val samplesRhs2 = scala.util.Random.shuffle(duplicate +: samplesRhs.toList) // they may come out of order

    val execPlan = BinaryJoinExec("someID", dummyDispatcher,
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.ADD,
      Cardinality.OneToOne,
      Nil, Seq("tag1"))

    val schema = Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
      ColumnInfo("value", ColumnType.DoubleColumn))

    // scalastyle:off
    val lhs = QueryResult("someId", null, samplesLhs.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs2.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on

    val fut = execPlan.compose(Observable.fromIterable(Seq((lhs, 0), (rhs, 1))), queryConfig).toListL.runAsync
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
      val rows: Iterator[RowReader] = data(2).iterator
    }

    val samplesLhs2 = scala.util.Random.shuffle(duplicate +: samplesLhs.toList) // they may come out of order

    val execPlan = BinaryJoinExec("someID", dummyDispatcher,
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      new Array[ExecPlan](1), // empty since we test compose, not execute or doExecute
      BinaryOperator.ADD,
      Cardinality.OneToOne,
      Nil, Seq("tag1"))

    val schema = Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
      ColumnInfo("value", ColumnType.DoubleColumn))

    // scalastyle:off
    val lhs = QueryResult("someId", null, samplesLhs2.map(rv => SerializableRangeVector(rv, schema)))
    val rhs = QueryResult("someId", null, samplesRhs.map(rv => SerializableRangeVector(rv, schema)))
    // scalastyle:on

    val fut = execPlan.compose(Observable.fromIterable(Seq((lhs, 0), (rhs, 1))), queryConfig).toListL.runAsync
    ScalaFutures.whenReady(fut.failed) { e =>
      e shouldBe a[BadQueryException]
    }
  }
}
