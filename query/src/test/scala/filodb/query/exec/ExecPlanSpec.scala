package filodb.query.exec

import com.typesafe.config.ConfigFactory
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, TimeSeriesMemStore}
import filodb.core.metadata.Column.ColumnType
import filodb.core.query.NoCloseCursor.NoCloseCursor
import filodb.core.DatasetRef
import filodb.core.query.{ColumnInfo, CustomRangeVectorKey, PerQueryLimits, PlannerParams,
                          QueryConfig, QueryContext, QuerySession, RangeParams, RangeVector,
                          RangeVectorCursor, RangeVectorKey, ResultSchema, RvRange, SerializedRangeVector,
                          TransientHistRow, TransientRow}
import filodb.core.store.{ChunkSource, InMemoryMetaStore, NullColumnStore}
import filodb.memory.format.vectors.{GeometricBuckets, HistogramBuckets, LongHistogram}
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.query.{BinaryOperator, QueryError, QueryResponse, QueryResult, StreamQueryResponse}
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
  val memStore = new TimeSeriesMemStore(
    config, new NullColumnStore, new NullColumnStore, new InMemoryMetaStore(), Some(policy)
  )

  def makeSetOperatorExecPlan(lhs: Seq[ExecPlan], rhs: Seq[ExecPlan],
                              binOp: BinaryOperator,
                              opRange: Option[RvRange],
                              qContext: QueryContext = QueryContext()
                             ): NonLeafExecPlan = {
    SetOperatorExec(qContext,
      InProcessPlanDispatcher(queryConfig),
      lhs: Seq[ExecPlan],
      rhs: Seq[ExecPlan],
      binOp,
      None,
      Nil,
      "",
      opRange)
  }

  def makeFixedLeafExecPlan(rvs: Seq[RangeVector], schema: ResultSchema,
                            qContext: QueryContext = QueryContext(),
                            planDispatcher: Option[PlanDispatcher] = None,
                            doExecuteParamAssertion: (ChunkSource, QuerySession) => Boolean = (_, _) => true): ExecPlan = {
    new LeafExecPlan {
      override protected def args: String = ???
      override def queryContext: QueryContext = qContext
      override def dataset: DatasetRef = ???
      override def dispatcher: PlanDispatcher = planDispatcher.getOrElse(new PlanDispatcher {
        override def clusterName: String = ???

        override def dispatch(plan: ExecPlanWithClientParams, source: ChunkSource)
                             (implicit sched: Scheduler): Task[QueryResponse] = ???

        override def dispatchStreaming(plan: ExecPlanWithClientParams, source: ChunkSource)
                                      (implicit sched: Scheduler): Observable[StreamQueryResponse] = ???

        // Force serialization by making this false, in case of true, no such filtering is performed
        override def isLocalCall: Boolean = false
      })
      override def doExecute(source: ChunkSource,
                             querySession: QuerySession)(implicit sched: Scheduler): ExecResult = {
        assert(doExecuteParamAssertion(source, querySession), "Failed in parameter assertion in doExecute")
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
      val res = makeFixedLeafExecPlan(Seq(rv), schema).execute(memStore, querySession).runToFuture.futureValue
      res match {
        case QueryError(id, queryStats, t) => {
          t.printStackTrace()
          fail()
        }
        case QueryResult(_, _, result, _, _, _, _) => result.isEmpty shouldEqual true

      }
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

    val qContextWithLowLimit =
      QueryContext(plannerParams = PlannerParams(enforcedLimits = PerQueryLimits(execPlanResultBytes = 5)))
    val qContextWithHighLimit =
      QueryContext(plannerParams = PlannerParams(enforcedLimits = PerQueryLimits(execPlanResultBytes = Long.MaxValue)))

    // All combos between [low, high] limit and [enforce, allow] protocol
    val querySessionEnforceLow = QuerySession(qContextWithLowLimit, QueryConfig(confEnforce))
    val querySessionAllowLow = QuerySession(qContextWithLowLimit, QueryConfig(confAllow))
    val querySessionEnforceHigh = QuerySession(qContextWithHighLimit, QueryConfig(confEnforce))
    val querySessionAllowHigh = QuerySession(qContextWithHighLimit, QueryConfig(confAllow))

    val planLow = makeFixedLeafExecPlan(Seq(rv), schema, qContextWithLowLimit)
    val planHigh = makeFixedLeafExecPlan(Seq(rv), schema, qContextWithHighLimit)

    // Only the low limit should yield a QueryError when enforced.
    planLow.execute(memStore, querySessionEnforceLow).runToFuture.futureValue.isInstanceOf[QueryError] shouldEqual true
    planHigh.execute(memStore, querySessionEnforceHigh).runToFuture.futureValue.isInstanceOf[QueryResult] shouldEqual true
    planLow.execute(memStore, querySessionAllowLow).runToFuture.futureValue.isInstanceOf[QueryResult] shouldEqual true
    planHigh.execute(memStore, querySessionAllowHigh).runToFuture.futureValue.isInstanceOf[QueryResult] shouldEqual true
  }

  it("should serialize/not serialize the rangeVector based on the parameter " +
    "preventRangeVectorSerialization param in leaf exec") {
    val rv = new RangeVector {
      override def key: RangeVectorKey =
        CustomRangeVectorKey(Map("foo".utf8 -> "bar".utf8))

      override def rows(): RangeVectorCursor =
        new NoCloseCursor(
          (0 until 10).zipWithIndex.map{ case (obs, i) =>
            new TransientRow(i.toLong, obs)
          }.toIterator)

      override def outputRange: Option[RvRange] = Some(RvRange(1000, 1, 1000))
    }

    val schema = ResultSchema(
      Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
        ColumnInfo("value", ColumnType.DoubleColumn)),
      1 // numRowKeyColumns
    )

    val plan = makeFixedLeafExecPlan(Seq(rv), schema)
    plan.execute(memStore, querySession).runToFuture.futureValue match {
      case res: QueryResult  =>
        val rv = res.result.head
        // By default we expect a SerializableRangeVector
        assert(rv.isInstanceOf[SerializedRangeVector])
        rv.rows().map(rr => (rr.getLong(0), rr.getDouble(1))).toList shouldEqual
          (0 until 10).zipWithIndex.map{ case (obs, i) => (i, obs.toDouble)}.toList
      case error: QueryError =>
        assert(false, "expected QueryResult got " + error)
    }

    // Same call with preventRangeVectorSerialization=true should not give a SerializedRangeVector
    val plan1 = makeFixedLeafExecPlan(Seq(rv), schema)
    plan1.execute(memStore, querySession.copy(preventRangeVectorSerialization=true)).runToFuture.futureValue match {
      case res: QueryResult  =>
        val rv = res.result.head
        // By default we expect a SerializableRangeVector
        // Now we expect a non-SerializedRangeVector because preventRangeVectorSerialization=true
        assert(!rv.isInstanceOf[SerializedRangeVector])
        rv.rows().map(rr => (rr.getLong(0), rr.getDouble(1))).toList shouldEqual
          (0 until 10).zipWithIndex.map{ case (obs, i) => (i, obs.toDouble)}.toList
      case error: QueryError =>
        assert(false, "expected QueryResult got " + error)
    }

  }

  it("should serialize/not serialize the rangeVector based on the parameter " +
    "preventRangeVectorSerialization param in non leaf exec") {
    val range = Some(RvRange(0, 1, 9))
    val rv = new RangeVector {
      override def key: RangeVectorKey =
        CustomRangeVectorKey(Map("foo".utf8 -> "bar".utf8))

      override def rows(): RangeVectorCursor =
        new NoCloseCursor(
          (0 until 10).zipWithIndex.map{ case (obs, i) =>
            new TransientRow(i.toLong, obs)
          }.toIterator)

      override def outputRange: Option[RvRange] = range
    }

    val schema = ResultSchema(
      Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
        ColumnInfo("value", ColumnType.DoubleColumn)),
      1 // numRowKeyColumns
    )

    val dispatcher = Some(InProcessPlanDispatcher(queryConfig))
    val lhs = makeFixedLeafExecPlan(Seq(rv), schema, planDispatcher = dispatcher,
      doExecuteParamAssertion = (_, qs) => !qs.preventRangeVectorSerialization)::Nil
    val rhs = makeFixedLeafExecPlan(Seq(rv), schema, planDispatcher = dispatcher,
      doExecuteParamAssertion = (_, qs) => !qs.preventRangeVectorSerialization)::Nil
    val plan = makeSetOperatorExecPlan(lhs, rhs, BinaryOperator.LOR, range)
    plan.execute(memStore, querySession).runToFuture.futureValue match {
      case res: QueryResult  =>
        val rv = res.result.head
        // By default we expect a SerializableRangeVector
        assert(rv.isInstanceOf[SerializedRangeVector])
        rv.rows().map(rr => (rr.getLong(0), rr.getDouble(1))).toList shouldEqual
          (0 until 10).zipWithIndex.map{ case (obs, i) => (i, obs.toDouble)}.toList
      case error: QueryError =>
        assert(false, "expected QueryResult got " + error)
    }

    // Same call with preventRangeVectorSerialization=true should not give a SerializedRangeVector
    // Also ensure the flag is propagated to the child exec plan
    val lhs1 = makeFixedLeafExecPlan(Seq(rv), schema, planDispatcher = dispatcher,
      doExecuteParamAssertion = (_, qs) => qs.preventRangeVectorSerialization)::Nil
    val rhs1 = makeFixedLeafExecPlan(Seq(rv), schema, planDispatcher = dispatcher,
      doExecuteParamAssertion = (_, qs) => qs.preventRangeVectorSerialization)::Nil
    val plan1 = makeSetOperatorExecPlan(lhs1, rhs1, BinaryOperator.LOR, range)
    val qs = querySession.copy(preventRangeVectorSerialization=true,
                                queryConfig = queryConfig.copy(enableLocalDispatch = true))
    plan1.execute(memStore, qs).runToFuture.futureValue match {
      case res: QueryResult  =>
        val rv = res.result.head
        // By default we expect a SerializableRangeVector
        assert(!rv.isInstanceOf[SerializedRangeVector])
        rv.rows().map(rr => (rr.getLong(0), rr.getDouble(1))).toList shouldEqual
          (0 until 10).zipWithIndex.map{ case (obs, i) => (i, obs.toDouble)}.toList
      case error: QueryError =>
        assert(false, "expected QueryResult got " + error)
    }

    // Another case is when the parent exec plan doesnt serialize the response but it needs to tell its children who
    // execute locally to not serialize the Rvs. This happens when a sub plan is dispatched to a remote node and the
    // result of the top node needs to be serialized, however, the child exec plans all are local and should not
    // serialize the range vectors

    // Start by enabling the feature in planner params
    val qConfig = querySession.queryConfig.copy(enableLocalDispatch=true)
    // The root is expected to be serialized but the leaves are not
    val qs1 = querySession.copy(
          queryConfig = qConfig,
          preventRangeVectorSerialization = false)
    val lhs2 = makeFixedLeafExecPlan(Seq(rv), schema, planDispatcher = dispatcher,
      doExecuteParamAssertion = (_, qs) => qs.preventRangeVectorSerialization)::Nil
    val rhs2 = makeFixedLeafExecPlan(Seq(rv), schema, planDispatcher = dispatcher,
      doExecuteParamAssertion = (_, qs) => qs.preventRangeVectorSerialization)::Nil
    val plan2 = makeSetOperatorExecPlan(lhs2, rhs2, BinaryOperator.LOR, range)
    plan2.execute(memStore, qs1)
      .runToFuture.futureValue match {
      case res: QueryResult  =>
        val rv = res.result.head
        // By default we expect a SerializableRangeVector for parent
        assert(rv.isInstanceOf[SerializedRangeVector])
        rv.rows().map(rr => (rr.getLong(0), rr.getDouble(1))).toList shouldEqual
          (0 until 10).zipWithIndex.map{ case (obs, i) => (i, obs.toDouble)}.toList
      case error: QueryError =>
        assert(false, "expected QueryResult got " + error)
    }


  }
}
