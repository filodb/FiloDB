package filodb.coordinator.queryplanner

import java.util.concurrent.ThreadLocalRandom

import akka.serialization.SerializationExtension
import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.{ActorPlanDispatcher, ActorSystemHolder, GrpcPlanDispatcher, RemoteActorPlanDispatcher}
import filodb.core.metadata.{Dataset, DatasetOptions, Schemas}
import filodb.core.query._
import filodb.core.query.Filter.Equals
import filodb.core.store.{AllChunkScan, ChunkScanMethod, InMemoryChunkScan, TimeRangeChunkScan, WriteBufferChunkScan}
import filodb.prometheus.ast.Vectors.PromMetricLabel
import filodb.query._
import filodb.query.InstantFunctionId.HistogramBucket
import filodb.query.LogicalPlan._
import filodb.query.exec._
import filodb.query.exec.InternalRangeFunction.Last

//scalastyle:off file.size.limit
/**
  * Intermediate Plan Result includes the exec plan(s) along with any state to be passed up the
  * plan building call tree during query planning.
  *
  * Not for runtime use.
  */
case class PlanResult(plans: Seq[ExecPlan], needsStitch: Boolean = false)

trait  DefaultPlanner {
    def queryConfig: QueryConfig
    def dataset: Dataset
    def schemas: Schemas
    def dsOptions: DatasetOptions
    private[queryplanner] val inProcessPlanDispatcher = InProcessPlanDispatcher(queryConfig)
    def materializeVectorPlan(qContext: QueryContext,
                              lp: VectorPlan,
                              forceInProcess: Boolean = false): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.scalars, qContext, forceInProcess)
      vectors.plans.foreach(_.addRangeVectorTransformer(VectorFunctionMapper()))
      vectors
    }

    def toChunkScanMethod(rangeSelector: RangeSelector): ChunkScanMethod = {
      rangeSelector match {
        case IntervalSelector(from, to) => TimeRangeChunkScan(from, to)
        case AllChunksSelector => AllChunkScan
        case WriteBufferSelector => WriteBufferChunkScan
        case InMemoryChunksSelector => InMemoryChunkScan
        case x@_ => throw new IllegalArgumentException(s"Unsupported range selector '$x' found")
      }
    }

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan


    def materializeFunctionArgs(functionParams: Seq[FunctionArgsPlan],
                                qContext: QueryContext): Seq[FuncArgs] = functionParams map {
        case num: ScalarFixedDoublePlan => StaticFuncArgs(num.scalar, num.timeStepParams)
        case s: ScalarVaryingDoublePlan => ExecPlanFuncArgs(materialize(s, qContext),
                                           RangeParams(s.startMs, s.stepMs, s.endMs))
        case t: ScalarTimeBasedPlan     => TimeFuncArgs(t.rangeParams)
        case s: ScalarBinaryOperation   => ExecPlanFuncArgs(materialize(s, qContext),
                                           RangeParams(s.startMs, s.stepMs, s.endMs))
    }
    /**
     * @param logicalPlan The LogicalPlan instance
     * @param qContext The QueryContext
     * @param forceInProcess if true, all materialized plans for this entire
     *                       logical plan will dispatch via an InProcessDispatcher
     * @return The PlanResult containing the ExecPlan
     */
    def walkLogicalPlanTree(logicalPlan: LogicalPlan,
                            qContext: QueryContext,
                            forceInProcess: Boolean = false): PlanResult

    /**
     * DefaultPlanner has logic to handle multiple LogicalPlans, classes implementing this trait may choose to override
     * the behavior and delegate to the default implementation when no special implementation if required.
     * The method is similar to walkLogicalPlanTree but deliberately chosen to have a different name to for the
     * classes to implement walkLogicalPlanTree and explicitly delegate to defaultWalkLogicalPlanTree if needed. The
     * method essentially pattern matches all LogicalPlans and invoke the default implementation in the
     * DefaultPlanner trait
     */
    // scalastyle:off cyclomatic.complexity
    def defaultWalkLogicalPlanTree(logicalPlan: LogicalPlan,
                                   qContext: QueryContext,
                                   forceInProcess: Boolean = false): PlanResult = logicalPlan match {

        case lp: ApplyInstantFunction        => this.materializeApplyInstantFunction(qContext, lp, forceInProcess)
        case lp: ApplyInstantFunctionRaw     => this.materializeApplyInstantFunctionRaw(qContext, lp, forceInProcess)
        case lp: Aggregate                   => this.materializeAggregate(qContext, lp, forceInProcess)
        case lp: BinaryJoin                  => this.materializeBinaryJoin(qContext, lp, forceInProcess)
        case lp: ScalarVectorBinaryOperation => this.materializeScalarVectorBinOp(qContext, lp, forceInProcess)

        case lp: ApplyMiscellaneousFunction  => this.materializeApplyMiscellaneousFunction(qContext, lp, forceInProcess)
        case lp: ApplySortFunction           => this.materializeApplySortFunction(qContext, lp, forceInProcess)
        case lp: ScalarVaryingDoublePlan     => this.materializeScalarPlan(qContext, lp, forceInProcess)
        case lp: ScalarTimeBasedPlan         => this.materializeScalarTimeBased(qContext, lp)
        case lp: VectorPlan                  => this.materializeVectorPlan(qContext, lp, forceInProcess)
        case lp: ScalarFixedDoublePlan       => this.materializeFixedScalar(qContext, lp)
        case lp: ApplyAbsentFunction         => this.materializeAbsentFunction(qContext, lp, forceInProcess)
        case lp: ApplyLimitFunction          => this.materializeLimitFunction(qContext, lp, forceInProcess)
        case lp: ScalarBinaryOperation       => this.materializeScalarBinaryOperation(qContext, lp, forceInProcess)
        case lp: SubqueryWithWindowing       => this.materializeSubqueryWithWindowing(qContext, lp, forceInProcess)
        case lp: TopLevelSubquery            => this.materializeTopLevelSubquery(qContext, lp, forceInProcess)
        case lp: PeriodicSeries              => this.materializePeriodicSeries(qContext, lp, forceInProcess)
        case lp: PeriodicSeriesWithWindowing =>
                                              this.materializePeriodicSeriesWithWindowing(qContext, lp, forceInProcess)
        case _: RawSeries                   |
             _: RawChunkMeta                |
             _: MetadataQueryPlan           |
             _: TsCardinalities              => throw new IllegalArgumentException("Unsupported operation")
    }


  private[queryplanner] def materializePeriodicSeriesWithWindowing(qContext: QueryContext,
                                                     lp: PeriodicSeriesWithWindowing,
                                                     forceInProcess: Boolean): PlanResult = {
    val logicalPlanWithoutBucket = if (queryConfig.translatePromToFilodbHistogram) {
      removeBucket(Right(lp))._3.right.get
    } else lp

    val series = walkLogicalPlanTree(logicalPlanWithoutBucket.series, qContext, forceInProcess)
    val rawSource = logicalPlanWithoutBucket.series.isRaw && (logicalPlanWithoutBucket.series match {
      case r: RawSeries   => !r.supportsRemoteDataCall
      case _              => true
    })   // the series is raw and supports raw export, its going to yield an iterator

    /* Last function is used to get the latest value in the window for absent_over_time
    If no data is present AbsentFunctionMapper will return range vector with value 1 */

    val execRangeFn = if (logicalPlanWithoutBucket.function == RangeFunctionId.AbsentOverTime) Last
    else InternalRangeFunction.lpToInternalFunc(logicalPlanWithoutBucket.function)

    val paramsExec = materializeFunctionArgs(logicalPlanWithoutBucket.functionArgs, qContext)
    val window = if (execRangeFn == InternalRangeFunction.Timestamp) None else Some(logicalPlanWithoutBucket.window)
    series.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(logicalPlanWithoutBucket.startMs,
      logicalPlanWithoutBucket.stepMs, logicalPlanWithoutBucket.endMs, window, Some(execRangeFn),
      logicalPlanWithoutBucket.stepMultipleNotationUsed,
      paramsExec, logicalPlanWithoutBucket.offsetMs, rawSource = rawSource)))
    if (logicalPlanWithoutBucket.function == RangeFunctionId.AbsentOverTime) {
      val aggregate = Aggregate(AggregationOperator.Sum, logicalPlanWithoutBucket, Nil,
        AggregateClause.byOpt(Seq("job")))
      // Add sum to aggregate all child responses
      // If all children have NaN value, sum will yield NaN and AbsentFunctionMapper will yield 1
      val aggregatePlanResult = PlanResult(Seq(addAggregator(aggregate, qContext.copy(plannerParams =
        qContext.plannerParams.copy(skipAggregatePresent = true)), series)))
      addAbsentFunctionMapper(aggregatePlanResult, logicalPlanWithoutBucket.columnFilters,
        RangeParams(logicalPlanWithoutBucket.startMs / 1000, logicalPlanWithoutBucket.stepMs / 1000,
          logicalPlanWithoutBucket.endMs / 1000), qContext)
    } else series
  }

  private[queryplanner] def removeBucket(lp: Either[PeriodicSeries, PeriodicSeriesWithWindowing]):
              (Option[String], Option[String], Either[PeriodicSeries, PeriodicSeriesWithWindowing])= {
    val rawSeries = lp match {
      case Right(value) => value.series
      case Left(value) => value.rawSeries
    }

    rawSeries match {
      case rawSeriesLp: RawSeries =>

        val nameFilter = rawSeriesLp.filters.find(_.column.equals(PromMetricLabel)).
          map(_.filter.valuesStrings.head.toString)
        val leFilter = rawSeriesLp.filters.find(_.column == "le").map(_.filter.valuesStrings.head.toString)

        if (nameFilter.isEmpty) (nameFilter, leFilter, lp)
        else {
          // the convention for histogram bucket queries is to have the "_bucket" string in the suffix
          if (!nameFilter.get.endsWith("_bucket")) {
            (nameFilter, leFilter, lp)
          }
          else {
            val filtersWithoutBucket = rawSeriesLp.filters.filterNot(_.column.equals(PromMetricLabel)).
              filterNot(_.column == "le") :+ ColumnFilter(PromMetricLabel,
              Equals(PlannerUtil.replaceLastBucketOccurenceStringFromMetricName(nameFilter.get)))
            val newLp =
              if (lp.isLeft)
                Left(lp.left.get.copy(rawSeries = rawSeriesLp.copy(filters = filtersWithoutBucket)))
              else
                Right(lp.right.get.copy(series = rawSeriesLp.copy(filters = filtersWithoutBucket)))
            (nameFilter, leFilter, newLp)
          }
        }
      case _ => (None, None, lp)
    }
  }

  private[queryplanner] def materializePeriodicSeries(qContext: QueryContext,
                                lp: PeriodicSeries,
                                forceInProcess: Boolean): PlanResult = {

    // Convert to FiloDB histogram by removing le label and bucket prefix
    // _sum and _count are removed in MultiSchemaPartitionsExec since we need to check whether there is a metric name
    // with _sum/_count as suffix
    val (nameFilter: Option[String], leFilter: Option[String], lpWithoutBucket: PeriodicSeries) =
    if (queryConfig.translatePromToFilodbHistogram) {
      val result = removeBucket(Left(lp))
      (result._1, result._2, result._3.left.get)

    } else (None, None, lp)

    val rawSeries = walkLogicalPlanTree(lpWithoutBucket.rawSeries, qContext, forceInProcess)
    val rawSource = lpWithoutBucket.rawSeries.isRaw && (lpWithoutBucket.rawSeries match {
      case r: RawSeries => !r.supportsRemoteDataCall
      case _ => true
    })
    rawSeries.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(lp.startMs, lp.stepMs, lp.endMs,
      window = None, functionId = None,
      stepMultipleNotationUsed = false, funcParams = Nil,
      lp.offsetMs, rawSource = rawSource)))

    if (nameFilter.isDefined && nameFilter.head.endsWith("_bucket") && leFilter.isDefined) {
      val paramsExec = StaticFuncArgs(leFilter.head.toDouble, RangeParams(lp.startMs / 1000, lp.stepMs / 1000,
        lp.endMs / 1000))
      rawSeries.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(HistogramBucket,
        Seq(paramsExec))))
    }
    rawSeries
  }

    def materializeApplyInstantFunction(qContext: QueryContext,
                                        lp: ApplyInstantFunction,
                                        forceInProcess: Boolean = false): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vectors, qContext, forceInProcess)
      val paramsExec = materializeFunctionArgs(lp.functionArgs, qContext)
      vectors.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(lp.function, paramsExec)))
      vectors
    }

    def materializeApplyMiscellaneousFunction(qContext: QueryContext,
                                              lp: ApplyMiscellaneousFunction,
                                              forceInProcess: Boolean = false): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vectors, qContext, forceInProcess)
      if (lp.function == MiscellaneousFunctionId.OptimizeWithAgg) {
        // Optimize with aggregation is a no-op, doing no transformation. It must pass through
        // the execution plan to apply optimization logic correctly during aggregation.
        vectors
      } else {
        if (lp.function == MiscellaneousFunctionId.HistToPromVectors)
          vectors.plans.foreach(_.addRangeVectorTransformer(HistToPromSeriesMapper(schemas.part)))
        else
          vectors.plans.foreach(_.addRangeVectorTransformer(MiscellaneousFunctionMapper(lp.function, lp.stringArgs)))
        vectors
      }
    }

    def materializeApplyInstantFunctionRaw(qContext: QueryContext,
                                           lp: ApplyInstantFunctionRaw,
                                           forceInProcess: Boolean = false): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vectors, qContext, forceInProcess)
      val paramsExec = materializeFunctionArgs(lp.functionArgs, qContext)
      vectors.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(lp.function, paramsExec)))
      vectors
    }

    def materializeScalarVectorBinOp(qContext: QueryContext,
                                     lp: ScalarVectorBinaryOperation,
                                     forceInProcess: Boolean = false): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vector, qContext, forceInProcess)
      val funcArg = materializeFunctionArgs(Seq(lp.scalarArg), qContext)
      vectors.plans.foreach(_.addRangeVectorTransformer(ScalarOperationMapper(lp.operator, lp.scalarIsLhs, funcArg)))
      vectors
    }

    def materializeApplySortFunction(qContext: QueryContext,
                                     lp: ApplySortFunction,
                                     forceInProcess: Boolean = false): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vectors, qContext, forceInProcess)
      if (vectors.plans.length > 1) {
        val targetActor = PlannerUtil.pickDispatcher(vectors.plans)
        val topPlan = LocalPartitionDistConcatExec(qContext, targetActor, vectors.plans)
        topPlan.addRangeVectorTransformer(SortFunctionMapper(lp.function))
        PlanResult(Seq(topPlan), vectors.needsStitch)
      } else {
        vectors.plans.foreach(_.addRangeVectorTransformer(SortFunctionMapper(lp.function)))
        vectors
      }
    }

    def materializeScalarPlan(qContext: QueryContext,
                              lp: ScalarVaryingDoublePlan,
                              forceInProcess: Boolean = false): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vectors, qContext, forceInProcess)
      if (vectors.plans.length > 1) {
        val targetActor = PlannerUtil.pickDispatcher(vectors.plans)
        val topPlan = LocalPartitionDistConcatExec(qContext, targetActor, vectors.plans)
        topPlan.addRangeVectorTransformer(ScalarFunctionMapper(lp.function,
          RangeParams(lp.startMs, lp.stepMs, lp.endMs)))
        PlanResult(Seq(topPlan), vectors.needsStitch)
      } else {
        vectors.plans.foreach(_.addRangeVectorTransformer(ScalarFunctionMapper(lp.function,
          RangeParams(lp.startMs, lp.stepMs, lp.endMs))))
        vectors
      }
    }

    def addAbsentFunctionMapper(vectors: PlanResult,
                               columnFilters: Seq[ColumnFilter],
                               rangeParams: RangeParams,
                               queryContext: QueryContext): PlanResult = {
      vectors.plans.foreach(_.addRangeVectorTransformer(AbsentFunctionMapper(columnFilters, rangeParams,
        dsOptions.metricColumn )))
      vectors
    }

   // scalastyle:off method.length
  /**
   * @param forceRootDispatcher if occupied, the dispatcher used at the root reducer node.
   */
   def addAggregator(lp: Aggregate,
                     qContext: QueryContext,
                     toReduceLevel: PlanResult,
                     forceRootDispatcher: Option[PlanDispatcher] = None):
   LocalPartitionReduceAggregateExec = {

    // Now we have one exec plan per shard
    /*
     * Note that in order for same overlapping RVs to not be double counted when spread is increased,
     * one of the following must happen
     * 1. Step instants must be chosen so time windows dont span shards.
     * 2. We pump data into multiple shards for sometime so atleast one shard will fully contain any time window
     *
     * Pulling all data into one node and stitch before reducing (not feasible, doesnt scale). So we will
     * not stitch
     *
     * Starting off with solution 1 first until (2) or some other approach is decided on.
     */

    // rename the clause labels (if they exist)
    val renamedLabelsClauseOpt =
      lp.clauseOpt.map{ clause =>
        val renamedLabels = LogicalPlanUtils.renameLabels(clause.labels, dsOptions.metricColumn)
        AggregateClause(clause.clauseType, renamedLabels)
      }

    toReduceLevel.plans.foreach {
      _.addRangeVectorTransformer(
        AggregateMapReduce(lp.operator, lp.params, renamedLabelsClauseOpt)
      )
    }

    val toReduceLevel2 =
      if (toReduceLevel.plans.size >= 16) {
        // If number of children is above a threshold, parallelize aggregation
        val groupSize = Math.sqrt(toReduceLevel.plans.size).ceil.toInt
        toReduceLevel.plans.grouped(groupSize).map { nodePlans =>
          val reduceDispatcher = PlannerUtil.pickDispatcher(nodePlans)
          LocalPartitionReduceAggregateExec(qContext, reduceDispatcher, nodePlans, lp.operator, lp.params)
        }.toList
      } else toReduceLevel.plans

   /*
    * NOTE: this LocalPartitionReduceAggregateExec wrapper is always created even when toReduceLevel2
    *   contains only one plan; this may artificially bloat QueryStats. However, ExecPlans currently
    *   offer no simple method to update their dispatchers, and forceRootDispatcher here must be
    *   honored to support target-schema pushdowns (see materializeWithPushdown in the SingleClusterPlanner).
    *   Given that the work required to allow dispatcher updates and/or rework pushdown logic is nontrivial
    *   and the QueryStats bloat given by unnecessary aggregation plans is likely small, the fix is skipped for now.
    */
    val reduceDispatcher = forceRootDispatcher.getOrElse(PlannerUtil.pickDispatcher(toReduceLevel2))
    val reducer = LocalPartitionReduceAggregateExec(qContext, reduceDispatcher, toReduceLevel2, lp.operator, lp.params)

    if (!qContext.plannerParams.skipAggregatePresent)
      reducer.addRangeVectorTransformer(AggregatePresenter(lp.operator, lp.params, RangeParams(
        lp.startMs / 1000, lp.stepMs / 1000, lp.endMs / 1000)))

    reducer
  }
  // scalastyle:on method.length

  def materializeAbsentFunction(qContext: QueryContext,
                                  lp: ApplyAbsentFunction,
                                forceInProcess: Boolean = false): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, qContext, forceInProcess)
    val aggregate = Aggregate(AggregationOperator.Sum, lp, Nil, AggregateClause.byOpt(Seq("job")))

    // Add sum to aggregate all child responses
    // If all children have NaN value, sum will yield NaN and AbsentFunctionMapper will yield 1
    val aggregatePlanResult = PlanResult(Seq(addAggregator(aggregate, qContext.copy(plannerParams =
      qContext.plannerParams.copy(skipAggregatePresent = true)), vectors))) // No need for present for sum
    addAbsentFunctionMapper(aggregatePlanResult, lp.columnFilters,
      RangeParams(lp.startMs / 1000, lp.stepMs / 1000, lp.endMs / 1000), qContext)
  }

  def materializeLimitFunction(qContext: QueryContext,
                                lp: ApplyLimitFunction,
                               forceInProcess: Boolean = false): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, qContext, forceInProcess)
    if (vectors.plans.length > 1) {
      val targetActor = PlannerUtil.pickDispatcher(vectors.plans)
      val topPlan = LocalPartitionDistConcatExec(qContext, targetActor, vectors.plans)
      topPlan.addRangeVectorTransformer(LimitFunctionMapper(lp.limit))
      PlanResult(Seq(topPlan), vectors.needsStitch)
    } else {
      vectors.plans.foreach(_.addRangeVectorTransformer(LimitFunctionMapper(lp.limit)))
      vectors
    }
  }

   def materializeAggregate(qContext: QueryContext,
                            lp: Aggregate,
                            forceInProcess: Boolean = false): PlanResult = {
    // Child plan should not skip Aggregate Present such as Topk in Sum(Topk)
    val toReduceLevel1 = walkLogicalPlanTree(lp.vectors,
      qContext.copy(plannerParams = qContext.plannerParams.copy(skipAggregatePresent = false)), forceInProcess)
    val reducer = addAggregator(lp, qContext, toReduceLevel1)
    PlanResult(Seq(reducer)) // since we have aggregated, no stitching
  }

   def materializeFixedScalar(qContext: QueryContext,
                                     lp: ScalarFixedDoublePlan): PlanResult = {
    val scalarFixedDoubleExec = ScalarFixedDoubleExec(qContext, dataset = dataset.ref,
      lp.timeStepParams, lp.scalar, inProcessPlanDispatcher)
    PlanResult(Seq(scalarFixedDoubleExec))
  }

  /**
   * Example:
   * min_over_time(foo{job="bar"}[3m:1m])
   *
   * outerStart = S
   * outerEnd = E
   * outerStep = 90s
   *
   * Resulting Exec Plan (hand edited and simplified to make it easier to read):
   * E~LocalPartitionDistConcatExec()
   * -T~PeriodicSamplesMapper(start=S, step=90s, end=E, window=3m, functionId=MinOverTime)
   * --T~PeriodicSamplesMapper(start=((S-3m)/1m)*1m+1m, step=1m, end=(E/1m)*1m, window=None, functionId=None)
   * ---E~MultiSchemaPartitionsExec
   */
  def materializeSubqueryWithWindowing(qContext: QueryContext,
                                       sqww: SubqueryWithWindowing,
                                       forceInProcess: Boolean = false): PlanResult = {
    // This physical plan will try to execute only one range query instead of a number of individual subqueries.
    // If ranges of each of the subqueries have overlap, retrieving the total range
    // is optimal, if there is no overlap and even worse significant gap between the individual subqueries, retrieving
    // the entire range might be suboptimal, this still might be a better option than issuing and concatenating numerous
    // subqueries separately
    val window = Some(sqww.subqueryWindowMs)
    // Unfortunately the logical plan by itself does not always contain start/end parameters like periodic series plans
    // such as subquery. We have to carry qContext together with the plans to walk the logical tree.
    // Some planners such as MultiPartitionPlanner theoretically can split the query across time and send them to
    // different partitions. If QueryContext is used to figure out start/end time it needs to be updated/in sync with
    // the inner logical plan. Subquery logic on its own relies on the logical plan as it has start/end/step.
    // MultiPartitionPlanner is modified, so, that if it encounters periodic series plan, it takes start and end from
    // the plan itself, not from the query context. At this point query context start/end are almost guaranteed to be
    // different from start/end of the inner logical plan. This is rather confusing, the intent, however, was to keep
    // query context "original" without modification, so, as to capture the original intent of the user. This, however,
    // does not hold true across the entire code base, there are a number of place where we modify query context.
    val innerExecPlan = walkLogicalPlanTree(sqww.innerPeriodicSeries, qContext, forceInProcess)
    if (sqww.functionId != RangeFunctionId.AbsentOverTime) {
      val rangeFn = InternalRangeFunction.lpToInternalFunc(sqww.functionId)
      val paramsExec = materializeFunctionArgs(sqww.functionArgs, qContext)
      val rangeVectorTransformer =
        PeriodicSamplesMapper(
          sqww.atMs.getOrElse(sqww.startMs), sqww.stepMs, sqww.atMs.getOrElse(sqww.endMs),
          window,
          Some(rangeFn),
          stepMultipleNotationUsed = false,
          paramsExec,
          sqww.offsetMs,
          rawSource = false,
          leftInclusiveWindow = true
        )
      innerExecPlan.plans.foreach { p => {
        p.addRangeVectorTransformer(rangeVectorTransformer)
        sqww.atMs.map(_ => p.addRangeVectorTransformer(RepeatTransformer(sqww.startMs, sqww.stepMs, sqww.endMs
          , p.queryWithPlanName(qContext))))
      }}
      innerExecPlan
    } else {
      val innerPlan = sqww.innerPeriodicSeries
      createAbsentOverTimePlan(innerExecPlan, innerPlan, qContext, window, sqww.offsetMs, sqww)
    }
  }

   def createAbsentOverTimePlan( innerExecPlan: PlanResult,
                                        innerPlan: PeriodicSeriesPlan,
                                        qContext: QueryContext,
                                        window: Option[Long],
                                        offsetMs : Option[Long],
                                        sqww: SubqueryWithWindowing
                                      ) : PlanResult = {
    // absent over time is essentially sum(last(series)) sent through AbsentFunctionMapper
     val realScanStartMs = sqww.atMs.getOrElse(sqww.startMs)
     val realScanEndMs = sqww.atMs.getOrElse(sqww.endMs)
     val realScanStep = sqww.atMs.map(_ => 0L).getOrElse(sqww.stepMs)

     innerExecPlan.plans.foreach(plan => {
       plan.addRangeVectorTransformer(PeriodicSamplesMapper(
         realScanStartMs, realScanStep, realScanEndMs,
         window,
         Some(InternalRangeFunction.lpToInternalFunc(RangeFunctionId.Last)),
         stepMultipleNotationUsed = false,
         Seq(),
         offsetMs,
         rawSource = false
       ))
       sqww.atMs.map(_ => plan.addRangeVectorTransformer(RepeatTransformer(sqww.startMs, sqww.stepMs, sqww.endMs,
         plan.queryWithPlanName(qContext))))
     }
    )
    val aggregate = Aggregate(AggregationOperator.Sum, innerPlan, Nil,
                              AggregateClause.byOpt(Seq("job")))
    val aggregatePlanResult = PlanResult(
      Seq(
        addAggregator(
          aggregate,
          qContext.copy(plannerParams = qContext.plannerParams.copy(skipAggregatePresent = true)),
          innerExecPlan)
      )
    )
    val plans = addAbsentFunctionMapper(
      aggregatePlanResult,
      Seq(),
      RangeParams(realScanStartMs / 1000, realScanStep / 1000, realScanEndMs / 1000),
      qContext
    ).plans

    if (sqww.atMs.nonEmpty) {
       plans.foreach(p => p.addRangeVectorTransformer(RepeatTransformer(sqww.startMs, sqww.stepMs, sqww.endMs,
         p.queryWithPlanName(qContext))))
    }
    aggregatePlanResult
  }

  def materializeTopLevelSubquery(qContext: QueryContext,
                                  tlsq: TopLevelSubquery,
                                  forceInProcess: Boolean = false): PlanResult = {
    // This physical plan will try to execute only one range query instead of a number of individual subqueries.
    // If ranges of each of the subqueries have overlap, retrieving the total range
    // is optimal, if there is no overlap and even worse significant gap between the individual subqueries, retrieving
    // the entire range might be suboptimal, this still might be a better option than issuing and concatenating numerous
    // subqueries separately
    walkLogicalPlanTree(tlsq.innerPeriodicSeries, qContext, forceInProcess)
  }

  def materializeScalarTimeBased(qContext: QueryContext,
                                  lp: ScalarTimeBasedPlan): PlanResult = {
    val scalarTimeBasedExec = TimeScalarGeneratorExec(qContext, dataset.ref,
      lp.rangeParams, lp.function, inProcessPlanDispatcher)
    PlanResult(Seq(scalarTimeBasedExec))
  }

   def materializeScalarBinaryOperation(qContext: QueryContext,
                                        lp: ScalarBinaryOperation,
                                        forceInProcess: Boolean = false): PlanResult = {
    val lhs = if (lp.lhs.isRight) {
      // Materialize as lhs is a logical plan
      val lhsExec = walkLogicalPlanTree(lp.lhs.right.get, qContext, forceInProcess)
      Right(lhsExec.plans.map(_.asInstanceOf[ScalarBinaryOperationExec]).head)
    } else Left(lp.lhs.left.get)

    val rhs = if (lp.rhs.isRight) {
      val rhsExec = walkLogicalPlanTree(lp.rhs.right.get, qContext, forceInProcess)
      Right(rhsExec.plans.map(_.asInstanceOf[ScalarBinaryOperationExec]).head)
    } else Left(lp.rhs.left.get)

    val scalarBinaryExec = ScalarBinaryOperationExec(qContext, dataset.ref,
      lp.rangeParams, lhs, rhs, lp.operator, inProcessPlanDispatcher)
    PlanResult(Seq(scalarBinaryExec))
  }

  /**
   * Function to optimize and convert `foo or vector(0)` like queries which would normally
   * result in an expensive SetOperatorExec plan into a simple InstantFunctionMapper
   * which is far more efficient.
   *
   * Note that vector(0) on left side of operator, example `vector(0) or foo` is not
   * optimized yet since it is uncommon and almost unseen, and not worth the additional
   * complexity at the moment.
   *
   * 05/01/2023: We are disabling this optimization until we handle the corner cases like:
   * 1. when the given metrics is not present or is not actively ingesting
   * rdar://108803361 (Fix vector(0) optimzation corner cases)
   */
  def optimizeOrVectorDouble(qContext: QueryContext, logicalPlan: BinaryJoin): Option[PlanResult] = {
    None
//    if (logicalPlan.operator == BinaryOperator.LOR) {
//      logicalPlan.rhs match {
//        case VectorPlan(ScalarFixedDoublePlan(value, rangeParams)) =>
//          val planRes = materializeApplyInstantFunction(
//                              qContext, ApplyInstantFunction(logicalPlan.lhs,
//                                                             InstantFunctionId.OrVectorDouble,
//                                                             Seq(ScalarFixedDoublePlan(value, rangeParams))))
//          Some(planRes)
//        case _ => None // TODO add more matchers to cover cases where rhs is a fully scalar plan
//      }
//    } else None
  }

  def materializeBinaryJoin(qContext: QueryContext,
                            logicalPlan: BinaryJoin,
                            forceInProcess: Boolean = false): PlanResult = {

    optimizeOrVectorDouble(qContext, logicalPlan).getOrElse {
      val lhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
        copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.lhs)))
      val rhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
        copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.rhs)))

      val lhs = walkLogicalPlanTree(logicalPlan.lhs, lhsQueryContext, forceInProcess)
      val rhs = walkLogicalPlanTree(logicalPlan.rhs, rhsQueryContext, forceInProcess)

      val dispatcher = if (!lhs.plans.head.dispatcher.isLocalCall && !rhs.plans.head.dispatcher.isLocalCall) {
        val lhsCluster = lhs.plans.head.dispatcher.clusterName
        val rhsCluster = rhs.plans.head.dispatcher.clusterName
        if (rhsCluster.equals(lhsCluster)) PlannerUtil.pickDispatcher(lhs.plans ++ rhs.plans)
        else inProcessPlanDispatcher
      } else inProcessPlanDispatcher

      val stitchedLhs = if (lhs.needsStitch) Seq(StitchRvsExec(qContext,
        dispatcher, rvRangeFromPlan(logicalPlan), lhs.plans))
      else lhs.plans

      val stitchedRhs = if (rhs.needsStitch) Seq(StitchRvsExec(qContext,
        dispatcher, rvRangeFromPlan(logicalPlan), rhs.plans))
      else rhs.plans

      val execPlan =
        if (logicalPlan.operator.isInstanceOf[SetOperator])
          SetOperatorExec(qContext, dispatcher, stitchedLhs, stitchedRhs, logicalPlan.operator,
            logicalPlan.on.map(LogicalPlanUtils.renameLabels(_, dsOptions.metricColumn)),
            LogicalPlanUtils.renameLabels(logicalPlan.ignoring, dsOptions.metricColumn), dsOptions.metricColumn,
            rvRangeFromPlan(logicalPlan))
        else
          BinaryJoinExec(qContext, dispatcher, stitchedLhs, stitchedRhs, logicalPlan.operator,
            logicalPlan.cardinality,
            logicalPlan.on.map(LogicalPlanUtils.renameLabels(_, dsOptions.metricColumn)),
            LogicalPlanUtils.renameLabels(logicalPlan.ignoring, dsOptions.metricColumn), logicalPlan.include,
            dsOptions.metricColumn, rvRangeFromPlan(logicalPlan))

      PlanResult(Seq(execPlan))
    }
  }
}

object PlannerUtil extends StrictLogging {

   /**
   * Returns URL params for label values which is used to create Metadata remote exec plan
   */
   def getLabelValuesUrlParams(lp: LabelValues, queryParams: PromQlQueryParams): Map[String, String] = {
     val quote = queryParams.remoteQueryPath match {
       case Some(s) if s.contains("""/v2/label/""") => """""""
       case _ => ""
     }
    // Filter value should be enclosed in quotes for label values v2 endpoint
    val filters = lp.filters.map{ f => s"""${f.column}${f.filter.operatorString}$quote${f.filter.valuesStrings.
      head}$quote"""}.mkString(",")
    Map("filter" -> filters, "labels" -> lp.labelNames.mkString(","))
  }


  /**
   * Picks one dispatcher randomly from child exec plans passed in as parameter
   */
  def pickDispatcher(children: Seq[ExecPlan]): PlanDispatcher = {
    val plannerParams = children.iterator.next().queryContext.plannerParams
    // there is a very subtle point on how to decide how we need to pick up the dispatcher
    // HA planner would inject the children that are created with HA planner with the active shard map
    // of the local and buddy clusters. However, if children were not scheduled by HA Planner, these maps
    // would be missing. Until we get rid of the hierarchy of planners, we cannot deal with children
    // scheduled by other planners. So, this is what we check below.
    if (plannerParams.failoverMode == ShardLevelFailoverMode && plannerParams.localShardMapper.isDefined) {
      // here we can encounter the following dispatchers to pick from:
      // (a) ActorPlanDispatcher - happy case
      // (b) RemoteActorPlanDispatcher - we cannot use because non leaf plans can be running
      //                                 only locally
      // (c) GrpcPlanDispatcher - we cannot use because non leaf plans can be running
      //                          only locally
      // (d) we don't find an ActorPlanDispatcher at all, hence, we need to pick
      //     from the list of available and active local shards
      val localActiveShardMapper = plannerParams.localShardMapper.get
      val buddyActiveShardMapper = plannerParams.buddyShardMapper.get
      if (!localActiveShardMapper.allShardsActive && buddyActiveShardMapper.allShardsActive) {
        pickDispatcherNormal(children)
      } else {
        pickDispatcherShardLevelFailover(children, plannerParams)
      }
    } else {
      // this case is used for both:
      // (1) legacy failover mode (ie no shard failover at all). In that case
      //     we expect to pick ActorDispatchers out of ActorPlanDispatchers
      //     provided (there cannot be any other dispatchers in the pool)
      // (2) all remote failover
      //     we expect to pick RemoteActorDispatcher out of RemoteActorPlanDispatchers
      pickDispatcherNormal(children)
    }

  }

  def pickDispatcherNormal(children: Seq[ExecPlan]): PlanDispatcher = {
    children.find(_.dispatcher.isLocalCall).map(_.dispatcher).getOrElse {
      val childTargets = children.map(_.dispatcher)
      // Above list can contain duplicate dispatchers, and we don't make them distinct.
      // Those with more shards must be weighed higher
      val rnd = ThreadLocalRandom.current()
      childTargets.iterator.drop(rnd.nextInt(childTargets.size)).next
    }

  }

  /**
   * Picks one dispatcher randomly from child exec plans passed in as parameter
   */
  def pickDispatcherShardLevelFailover(
    children: Seq[ExecPlan], plannerParams: PlannerParams
  ): PlanDispatcher = {

    children.find(_.dispatcher.isLocalCall).map(_.dispatcher).getOrElse {

      val childTargets = children.map(_.dispatcher)
      // Above list can contain duplicate dispatchers, and we don't make them distinct.
      // Those with more shards must be weighed higher
      val localDispatchers = childTargets.filter( d => isScheduledLocally(d))
      val rnd = ThreadLocalRandom.current()
      if (localDispatchers.isEmpty) {
        // This should be vary rare. We tried to assign a dispatcher for a non leaf plan by
        // picking one of the dispatchers of its own children but none of the children plans
        // have a local dispatcher. So, we have to assign this plan to a random active
        // shard of this cluster. We do shard level fail over only if there are decent
        // umber of active shards in the local cluster.
        // The very fact that a non leaf plan has no children
        // assigned to an active local shard means that it probably has to be a target
        // schema case of some sort. //TODO verify this is the only explanation
        val ep = children.iterator.next
        val clusterName = ep.dispatcher.clusterName
        val localShardMapper = ep.queryContext.plannerParams.localShardMapper.get
        val activeShards = localShardMapper.activeShards
        val randomActiveShard = activeShards.iterator.drop(rnd.nextInt(activeShards.size)).next()
        val path = localShardMapper.shards(randomActiveShard).address
        val serialization = SerializationExtension(ActorSystemHolder.system)
        val deserializedActorRef = serialization.system.provider.resolveActorRef(path)
        val dispatcher = ActorPlanDispatcher(
          deserializedActorRef, clusterName
        )
        dispatcher
      } else {
        val dispatcher =
          localDispatchers.iterator.drop(rnd.nextInt(localDispatchers.size)).next
        dispatcher
      }
   }
  }

  def isScheduledLocally(d : PlanDispatcher): Boolean = {
    val scheduledLocally = (!d.isInstanceOf[RemoteActorPlanDispatcher]) && (!d.isInstanceOf[GrpcPlanDispatcher])
    scheduledLocally
  }

  //scalastyle:off method.length
  def rewritePlanWithRemoteRawExport(lp: LogicalPlan,
                                     rangeSelector: IntervalSelector,
                                     additionalLookbackMs: Long = 0): LogicalPlan =
    lp match {
      case lp: ApplyInstantFunction =>
        lp.copy(vectors = rewritePlanWithRemoteRawExport(lp.vectors, rangeSelector, additionalLookbackMs)
          .asInstanceOf[PeriodicSeriesPlan],
          functionArgs = lp.functionArgs.map(
          rewritePlanWithRemoteRawExport(_, rangeSelector, additionalLookbackMs).asInstanceOf[FunctionArgsPlan]))
      case lp: ApplyInstantFunctionRaw =>
        lp.copy(vectors = rewritePlanWithRemoteRawExport(lp.vectors, rangeSelector, additionalLookbackMs)
          .asInstanceOf[RawSeries],
          functionArgs = lp.functionArgs.map(
            rewritePlanWithRemoteRawExport(_, rangeSelector, additionalLookbackMs).asInstanceOf[FunctionArgsPlan]))
      case lp: Aggregate =>
        lp.copy(vectors = rewritePlanWithRemoteRawExport(lp.vectors, rangeSelector, additionalLookbackMs)
          .asInstanceOf[PeriodicSeriesPlan])
      case lp: BinaryJoin =>
        lp.copy(lhs = rewritePlanWithRemoteRawExport(lp.lhs, rangeSelector, additionalLookbackMs)
          .asInstanceOf[PeriodicSeriesPlan],
          rhs = rewritePlanWithRemoteRawExport(lp.rhs, rangeSelector, additionalLookbackMs)
            .asInstanceOf[PeriodicSeriesPlan])
      case lp: ScalarVectorBinaryOperation =>
        lp.copy(scalarArg = rewritePlanWithRemoteRawExport(lp.scalarArg, rangeSelector, additionalLookbackMs)
          .asInstanceOf[ScalarPlan],
          vector = rewritePlanWithRemoteRawExport(lp.vector, rangeSelector, additionalLookbackMs)
          .asInstanceOf[PeriodicSeriesPlan])
      case lp: ApplyMiscellaneousFunction =>
        lp.copy(vectors = rewritePlanWithRemoteRawExport(lp.vectors, rangeSelector, additionalLookbackMs)
          .asInstanceOf[PeriodicSeriesPlan])
      case lp: ApplySortFunction =>
        lp.copy(vectors = rewritePlanWithRemoteRawExport(lp.vectors, rangeSelector, additionalLookbackMs)
          .asInstanceOf[PeriodicSeriesPlan])
      case lp: ScalarVaryingDoublePlan =>
        lp.copy(vectors = rewritePlanWithRemoteRawExport(lp.vectors, rangeSelector, additionalLookbackMs)
          .asInstanceOf[PeriodicSeriesPlan],
          functionArgs = lp.functionArgs.map(
            rewritePlanWithRemoteRawExport(_, rangeSelector, additionalLookbackMs).asInstanceOf[FunctionArgsPlan]))
      case lp: ScalarTimeBasedPlan => lp.copy(
        rangeParams = lp.rangeParams.copy(startSecs = rangeSelector.from / 1000L, endSecs = rangeSelector.to / 1000L))
      case lp: VectorPlan =>
        lp.copy(scalars = rewritePlanWithRemoteRawExport(lp.scalars, rangeSelector, additionalLookbackMs)
          .asInstanceOf[ScalarPlan])
      case lp: ScalarFixedDoublePlan => lp.copy(timeStepParams =
        lp.timeStepParams.copy(startSecs = rangeSelector.from / 1000L, endSecs = rangeSelector.to / 1000L))
      case lp: ApplyAbsentFunction =>
        lp.copy(vectors = rewritePlanWithRemoteRawExport(lp.vectors, rangeSelector, additionalLookbackMs)
          .asInstanceOf[PeriodicSeriesPlan])
      case lp: ApplyLimitFunction =>
        lp.copy(vectors = rewritePlanWithRemoteRawExport(lp.vectors, rangeSelector, additionalLookbackMs)
          .asInstanceOf[PeriodicSeriesPlan])
      case lp: ScalarBinaryOperation => lp.copy(
        rangeParams = lp.rangeParams.copy(startSecs = rangeSelector.from / 1000L, endSecs = rangeSelector.to / 1000L))
      case lp: SubqueryWithWindowing =>
        lp.copy(innerPeriodicSeries =
          rewritePlanWithRemoteRawExport(lp.innerPeriodicSeries, rangeSelector, additionalLookbackMs)
            .asInstanceOf[PeriodicSeriesPlan],
          functionArgs = lp.functionArgs.map(
            rewritePlanWithRemoteRawExport(_, rangeSelector, additionalLookbackMs).asInstanceOf[FunctionArgsPlan]))
      case lp: TopLevelSubquery =>
        lp.copy(innerPeriodicSeries =
          rewritePlanWithRemoteRawExport(lp.innerPeriodicSeries, rangeSelector,
            additionalLookbackMs = additionalLookbackMs).asInstanceOf[PeriodicSeriesPlan])
      case lp: RawSeries =>
        // IMPORTANT: When we export raw data over remote data call, offset does not mean anything, instead
        // do a raw lookback of original lookback + offset and set offset to 0
        val newLookback = lp.lookbackMs.getOrElse(0L) + lp.offsetMs.getOrElse(0L) + additionalLookbackMs
        lp.copy(supportsRemoteDataCall = true, rangeSelector = rangeSelector,
          lookbackMs = if (newLookback == 0) None else Some(newLookback), offsetMs = None)
      case lp: RawChunkMeta =>  lp.copy(rangeSelector = rangeSelector)
      case lp: PeriodicSeries =>
        lp.copy(rawSeries = rewritePlanWithRemoteRawExport(lp.rawSeries, rangeSelector, additionalLookbackMs)
          .asInstanceOf[RawSeriesLikePlan], startMs = rangeSelector.from, endMs = rangeSelector.to)
      case lp: PeriodicSeriesWithWindowing =>
        lp.copy(
          startMs = rangeSelector.from,
          endMs = rangeSelector.to,
          functionArgs = lp.functionArgs.map(
            rewritePlanWithRemoteRawExport(_, rangeSelector, additionalLookbackMs).asInstanceOf[FunctionArgsPlan]),
          series = rewritePlanWithRemoteRawExport(lp.series, rangeSelector, additionalLookbackMs)
          .asInstanceOf[RawSeriesLikePlan])
      // wont bother rewriting and adjusting the start and end for metadata calls
      case lp: MetadataQueryPlan => lp
      case lp: TsCardinalities => lp
    }
    //scalastyle:on method.length

  /**
   * Replaces the last occurence of '_bucket' string from the given input
   * @param metricName Metric Name
   * @return updated metric name without the last occurence of _bucket
   */
  def replaceLastBucketOccurenceStringFromMetricName(metricName: String): String = {
    metricName.replaceAll("_bucket$", "")
  }
}
