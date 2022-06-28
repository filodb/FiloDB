package filodb.query.exec

import com.typesafe.config.ConfigFactory
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, TimeSeriesMemStore}
import filodb.core.metadata.Column.ColumnType
import filodb.core.query.NoCloseCursor.NoCloseCursor
import filodb.core.DatasetRef
import filodb.core.query.{ColumnInfo, CustomRangeVectorKey, PlannerParams, QueryConfig, QueryContext, QuerySession, RangeParams, RangeVector, RangeVectorCursor, RangeVectorKey, ResultSchema, RvRange, TransientHistRow}
import filodb.core.store.{ChunkSource, InMemoryMetaStore, NullColumnStore}
import filodb.memory.format.vectors.{GeometricBuckets, HistogramBuckets, LongHistogram}
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.query.{BinaryOperator, QueryError, QueryResult}
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
  val queryConfig = QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)
  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))

  def makeFixedLeafExecPlan(rvs: Seq[RangeVector], schema: ResultSchema,
                            qContext: QueryContext = QueryContext()): ExecPlan = {
    new LeafExecPlan {
      override protected def args: String = ???
      override def queryContext: QueryContext = qContext
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
      1 // numRowKeyColumns
    )

    val ep = makeFixedLeafExecPlan(Seq(rv), schema)
    ep.addRangeVectorTransformer(
      ScalarOperationMapper(BinaryOperator.EQL,
        scalarOnLhs = false,
        Seq(StaticFuncArgs(1.0, RangeParams(1000, 10, 2000)))))

    ep.execute(memStore, querySession).runToFuture.futureValue
      .asInstanceOf[QueryResult].result.isEmpty shouldEqual true
  }

  it ("should correctly apply scalar operations to histogram") {
    val scalar = 500.0
    val buckets = GeometricBuckets(0.1, 10, 5)
    val bucketValues = Seq(
      Array(1L, 2L, 3L, 4L, 5L),
      Array(1L, 3L, 4L, 5L, 6L),
      Array(3L, 4L, 7L, 18L, 52L)
    )

    val rvHist = new RangeVector {
      override def key: RangeVectorKey =
        CustomRangeVectorKey(Map("foo".utf8 -> "bar".utf8))

      override def rows(): RangeVectorCursor =
        new NoCloseCursor(
          bucketValues.zipWithIndex.map{ case (arr, i) =>
            new TransientHistRow(i.toLong, LongHistogram(buckets, arr))
          }.toIterator)

      override def outputRange: Option[RvRange] = Some(RvRange(1000, 1, 1000))
    }

    val schemaHist = ResultSchema(
      Seq(ColumnInfo("c0", ColumnType.TimestampColumn),
        ColumnInfo("c1", ColumnType.HistogramColumn)),
      1 // numRowKeyColumns
    )

    val plan = makeFixedLeafExecPlan(Seq(rvHist), schemaHist)
    plan.addRangeVectorTransformer(
      ScalarOperationMapper(
        BinaryOperator.ADD,
        scalarOnLhs = false,
        Seq(StaticFuncArgs(scalar, RangeParams(1000, 1, 1000)))
      )
    )

    val rows = plan.execute(memStore, querySession).runToFuture.futureValue.asInstanceOf[QueryResult].result.head.rows
    // cannot assert size; will exhaust the iterator
    for ((row, irow) <- rows.zipWithIndex) {
      val hist = row.getHistogram(1)
      val arr = bucketValues(irow)
      (0 until hist.numBuckets).foreach{ ibucket =>
        hist.bucketValue(ibucket) shouldEqual arr(ibucket) + scalar
      }
    }
  }

  it ("should fail if result size limit is exceeded") {
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
      1 // numRowKeyColumns
    )

    val rawQueryConfig = config.getConfig("query")
    val confEnforce = ConfigFactory.parseString("enforce-result-byte-limit = true").withFallback(rawQueryConfig)
    val confAllow = ConfigFactory.parseString("enforce-result-byte-limit = false").withFallback(rawQueryConfig)

    val qContextWithLowLimit = QueryContext(plannerParams = PlannerParams(resultByteLimit = 5))
    val qContextWithHighLimit = QueryContext(plannerParams = PlannerParams(resultByteLimit = Long.MaxValue))

    // All combos between [low, high] limit and [enforce, allow] protocol
    val querySessionEnforceLow = QuerySession(qContextWithLowLimit, QueryConfig(confEnforce))
    val querySessionAllowLow = QuerySession(qContextWithLowLimit, QueryConfig(confAllow))
    val querySessionEnforceHigh = QuerySession(qContextWithHighLimit, QueryConfig(confEnforce))
    val querySessionAllowHigh = QuerySession(qContextWithHighLimit, QueryConfig(confAllow))

    // Plan's QContext is irrelevant; the QSession's QContext is used in the execution pipeline.
    val plan = makeFixedLeafExecPlan(Seq(rv), schema, QueryContext())

    // Only the low limit should yield a QueryError when enforced.
    plan.execute(memStore, querySessionEnforceLow).runToFuture.futureValue.isInstanceOf[QueryError] shouldEqual true
    plan.execute(memStore, querySessionEnforceHigh).runToFuture.futureValue.isInstanceOf[QueryResult] shouldEqual true
    plan.execute(memStore, querySessionAllowLow).runToFuture.futureValue.isInstanceOf[QueryResult] shouldEqual true
    plan.execute(memStore, querySessionAllowHigh).runToFuture.futureValue.isInstanceOf[QueryResult] shouldEqual true
  }
}
