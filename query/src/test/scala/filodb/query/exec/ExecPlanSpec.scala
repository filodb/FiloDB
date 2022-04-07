package filodb.query.exec

import com.typesafe.config.ConfigFactory
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, TimeSeriesMemStore}
import filodb.core.metadata.Column.ColumnType
import filodb.core.query.NoCloseCursor.NoCloseCursor
import filodb.core.DatasetRef
import filodb.core.query.{ColumnInfo, CustomRangeVectorKey, QueryConfig, QueryContext, QuerySession,
                          RangeParams, RangeVector, RangeVectorCursor, RangeVectorKey, ResultSchema, RvRange}
import filodb.core.store.{ChunkSource, InMemoryMetaStore, NullColumnStore}
import filodb.memory.format.vectors.{HistogramBuckets, LongHistogram}
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.query.{BinaryOperator, QueryResult}
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import ZeroCopyUTF8String._

class ExecPlanSpec extends AnyFunSpec with Matchers with ScalaFutures {
  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))
  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)
  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))

  def makeFixedLeafExecPlan(rvs: Seq[RangeVector], schema: ResultSchema): ExecPlan = {
    new LeafExecPlan {
      override protected def args: String = ???
      override def queryContext: QueryContext = QueryContext()
      override def dataset: DatasetRef = ???
      override def dispatcher: PlanDispatcher = ???
      override def doExecute(source: ChunkSource,
                             querySession: QuerySession)(implicit sched: Scheduler): ExecResult = {
        ExecResult(Observable.fromIterable(rvs), Task.now(schema))
      }
    }
  }

  it ("should not return an RV if all rows are empty/NaN") {
    // Conditions for emtpy/NaN are as follows (from SerializedRangeVector object):
    //    schema.columns(1).colType == DoubleColumn && !nextRow.getDouble(1).isNaN
    //    schema.columns(1).colType == HistogramColumn && !nextRow.getHistogram(1).isEmpty

    val rvDouble = new RangeVector {
      override def key: RangeVectorKey =
        CustomRangeVectorKey(Map("foo".utf8 -> "bar".utf8))

      override def rows(): RangeVectorCursor =
        new NoCloseCursor(Seq(
          SeqRowReader(Seq(1, Double.NaN)),
          SeqRowReader(Seq(2, Double.NaN)),
          SeqRowReader(Seq(3, Double.NaN))).iterator)

      override def outputRange: Option[RvRange] = Some(RvRange(1000, 10, 2000))
    }
    val schemaDouble = ResultSchema(
      Seq(ColumnInfo("c0", ColumnType.TimestampColumn),
          ColumnInfo("c1", ColumnType.DoubleColumn)),
      0 // numRowKeyColumns
    )

    val rvHist = new RangeVector {
      override def key: RangeVectorKey =
        CustomRangeVectorKey(Map("foo".utf8 -> "bar".utf8))

      override def rows(): RangeVectorCursor =
        new NoCloseCursor(Seq(
          SeqRowReader(Seq(1, LongHistogram.empty(HistogramBuckets.emptyBuckets))),
          SeqRowReader(Seq(2, LongHistogram.empty(HistogramBuckets.emptyBuckets))),
          SeqRowReader(Seq(3, LongHistogram.empty(HistogramBuckets.emptyBuckets)))).iterator)

      override def outputRange: Option[RvRange] = Some(RvRange(1000, 10, 2000))
    }
    val schemaHist = ResultSchema(
      Seq(ColumnInfo("c0", ColumnType.TimestampColumn),
          ColumnInfo("c1", ColumnType.HistogramColumn)),
      0 // numRowKeyColumns
    )

    val rvSchemaPairs = Seq(
      (rvDouble, schemaDouble),
      (rvHist, schemaHist)
    )

    rvSchemaPairs.foreach { case (rv, schema) =>
      makeFixedLeafExecPlan(Seq(rv), schema).execute(memStore, querySession)
        .runToFuture.futureValue.asInstanceOf[QueryResult].result.isEmpty shouldEqual true
    }
  }

  it ("should not return an RV if all rows are made NaN/empty by RVT") {
    val rv = new RangeVector {
      override def key: RangeVectorKey =
        CustomRangeVectorKey(Map("foo".utf8 -> "bar".utf8))

      override def rows(): RangeVectorCursor =
        new NoCloseCursor(Seq(
          SeqRowReader(Seq[Any](1L, 17834.4)),
          SeqRowReader(Seq[Any](2L, 999.5)),
          SeqRowReader(Seq[Any](3L, 8765.123))).iterator)

      override def outputRange: Option[RvRange] = Some(RvRange(1000, 10, 2000))
    }

    val schema = ResultSchema(
      Seq(ColumnInfo("c0", ColumnType.TimestampColumn),
          ColumnInfo("c1", ColumnType.DoubleColumn)),
      0 // numRowKeyColumns
    )

    val ep = makeFixedLeafExecPlan(Seq(rv), schema)
    ep.addRangeVectorTransformer(
      ScalarOperationMapper(BinaryOperator.EQL,
                            scalarOnLhs = false,
                            Seq(StaticFuncArgs(1.0, RangeParams(1000, 10, 2000)))))

    ep.execute(memStore, querySession).runToFuture.futureValue
      .asInstanceOf[QueryResult].result.isEmpty shouldEqual true
  }
}
