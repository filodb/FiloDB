package filodb.coordinator.queryplanner

import java.util.concurrent.ThreadLocalRandom

import com.typesafe.scalalogging.StrictLogging

import filodb.core.metadata.{Dataset, DatasetOptions, Schemas}
import filodb.core.query._
import filodb.query._
import filodb.query.LogicalPlan._
import filodb.query.exec._

/**
  * Intermediate Plan Result includes the exec plan(s) along with any state to be passed up the
  * plan building call tree during query planning.
  *
  * Not for runtime use.
  */
case class PlanResult(plans: Seq[ExecPlan], needsStitch: Boolean = false)

trait  PlannerHelper {
    def queryConfig: QueryConfig
    def dataset: Dataset
    def schemas: Schemas
    def dsOptions: DatasetOptions
    private[queryplanner] val inProcessPlanDispatcher = InProcessPlanDispatcher(queryConfig)
    def materializeVectorPlan(qContext: QueryContext,
                              lp: VectorPlan): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.scalars, qContext)
      vectors.plans.foreach(_.addRangeVectorTransformer(VectorFunctionMapper()))
      vectors
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



    def walkLogicalPlanTree(logicalPlan: LogicalPlan,
                            qContext: QueryContext): PlanResult

    def materializeApplyInstantFunction(qContext: QueryContext,
                                        lp: ApplyInstantFunction): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vectors, qContext)
      val paramsExec = materializeFunctionArgs(lp.functionArgs, qContext)
      vectors.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(lp.function, paramsExec)))
      vectors
    }

    def materializeApplyMiscellaneousFunction(qContext: QueryContext,
                                              lp: ApplyMiscellaneousFunction): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vectors, qContext)
      if (lp.function == MiscellaneousFunctionId.HistToPromVectors)
        vectors.plans.foreach(_.addRangeVectorTransformer(HistToPromSeriesMapper(schemas.part)))
      else
        vectors.plans.foreach(_.addRangeVectorTransformer(MiscellaneousFunctionMapper(lp.function, lp.stringArgs)))
      vectors
    }

    def materializeApplyInstantFunctionRaw(qContext: QueryContext,
                                           lp: ApplyInstantFunctionRaw): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vectors, qContext)
      val paramsExec = materializeFunctionArgs(lp.functionArgs, qContext)
      vectors.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(lp.function, paramsExec)))
      vectors
    }

    def materializeScalarVectorBinOp(qContext: QueryContext,
                                     lp: ScalarVectorBinaryOperation): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vector, qContext)
      val funcArg = materializeFunctionArgs(Seq(lp.scalarArg), qContext)
      vectors.plans.foreach(_.addRangeVectorTransformer(ScalarOperationMapper(lp.operator, lp.scalarIsLhs, funcArg)))
      vectors
    }

    def materializeApplySortFunction(qContext: QueryContext,
                                     lp: ApplySortFunction): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vectors, qContext)
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
                              lp: ScalarVaryingDoublePlan): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vectors, qContext)
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

   def addAggregator(lp: Aggregate, qContext: QueryContext, toReduceLevel: PlanResult):
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
    toReduceLevel.plans.foreach {
      _.addRangeVectorTransformer(AggregateMapReduce(lp.operator, lp.params,
        LogicalPlanUtils.renameLabels(lp.without, dsOptions.metricColumn),
        LogicalPlanUtils.renameLabels(lp.by, dsOptions.metricColumn)))
    }

    val toReduceLevel2 =
      if (toReduceLevel.plans.size >= 16) {
        // If number of children is above a threshold, parallelize aggregation
        val groupSize = Math.sqrt(toReduceLevel.plans.size).ceil.toInt
        toReduceLevel.plans.grouped(groupSize).map { nodePlans =>
          val reduceDispatcher = nodePlans.head.dispatcher
          LocalPartitionReduceAggregateExec(qContext, reduceDispatcher, nodePlans, lp.operator, lp.params)
        }.toList
      } else toReduceLevel.plans

    val reduceDispatcher = PlannerUtil.pickDispatcher(toReduceLevel2)
    val reducer = LocalPartitionReduceAggregateExec(qContext, reduceDispatcher, toReduceLevel2, lp.operator, lp.params)

    if (!qContext.plannerParams.skipAggregatePresent)
      reducer.addRangeVectorTransformer(AggregatePresenter(lp.operator, lp.params, RangeParams(
        lp.startMs / 1000, lp.stepMs / 1000, lp.endMs / 1000)))

    reducer
  }

  def materializeAbsentFunction(qContext: QueryContext,
                                  lp: ApplyAbsentFunction): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, qContext)
    val aggregate = Aggregate(AggregationOperator.Sum, lp, Nil, Seq("job"))
    // Add sum to aggregate all child responses
    // If all children have NaN value, sum will yield NaN and AbsentFunctionMapper will yield 1
    val aggregatePlanResult = PlanResult(Seq(addAggregator(aggregate, qContext.copy(plannerParams =
      qContext.plannerParams.copy(skipAggregatePresent = true)), vectors))) // No need for present for sum
    addAbsentFunctionMapper(aggregatePlanResult, lp.columnFilters,
      RangeParams(lp.startMs / 1000, lp.stepMs / 1000, lp.endMs / 1000), qContext)
  }

  def materializeLimitFunction(qContext: QueryContext,
                                lp: ApplyLimitFunction): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, qContext)
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
                                   lp: Aggregate): PlanResult = {
    val toReduceLevel1 = walkLogicalPlanTree(lp.vectors, qContext)
    val reducer = addAggregator(lp, qContext, toReduceLevel1)
    PlanResult(Seq(reducer)) // since we have aggregated, no stitching
  }

   def materializeFixedScalar(qContext: QueryContext,
                                     lp: ScalarFixedDoublePlan): PlanResult = {
    val scalarFixedDoubleExec = ScalarFixedDoubleExec(qContext, dataset = dataset.ref, lp.timeStepParams, lp.scalar)
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
  def materializeSubqueryWithWindowing(qContext: QueryContext, sqww: SubqueryWithWindowing): PlanResult = {
    // This physical plan will try to execute only one range query instead of a number of individual subqueries.
    // If ranges of each of the subqueries have overlap, retrieving the total range
    // is optimal, if there is no overlap and even worse significant gap between the individual subqueries, retrieving
    // the entire range might be suboptimal, this still might be a better option than issuing and concatenating numerous
    // subqueries separately
    val innerPlan = sqww.innerPeriodicSeries
    val window = Some(sqww.subqueryWindowMs)
    // Here the inner periodic series already has start/end/step populated
    // in Function's toSeriesPlan(), Functions.scala subqqueryArgument() method.
    val innerExecPlan = walkLogicalPlanTree(sqww.innerPeriodicSeries, qContext)
    if (sqww.functionId != RangeFunctionId.AbsentOverTime) {
      val rangeFn = InternalRangeFunction.lpToInternalFunc(sqww.functionId)
      val paramsExec = materializeFunctionArgs(sqww.functionArgs, qContext)
      val rangeVectorTransformer =
        PeriodicSamplesMapper(
          sqww.startMs, sqww.stepMs, sqww.endMs,
          window,
          Some(rangeFn),
          qContext,
          stepMultipleNotationUsed = false,
          paramsExec,
          sqww.offsetMs,
          rawSource = false,
          leftInclusiveWindow = true
        )
      innerExecPlan.plans.foreach { p => p.addRangeVectorTransformer(rangeVectorTransformer)}
      innerExecPlan
    } else {
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
    innerExecPlan.plans.foreach(
      _.addRangeVectorTransformer(PeriodicSamplesMapper(
        sqww.startMs, sqww.stepMs, sqww.endMs,
        window,
        Some(InternalRangeFunction.lpToInternalFunc(RangeFunctionId.Last)),
        qContext,
        stepMultipleNotationUsed = false,
        Seq(),
        offsetMs,
        rawSource = false
      ))
    )
    val aggregate = Aggregate(AggregationOperator.Sum, innerPlan, Nil, Seq("job"))
    val aggregatePlanResult = PlanResult(
      Seq(
        addAggregator(
          aggregate,
          qContext.copy(plannerParams = qContext.plannerParams.copy(skipAggregatePresent = true)),
          innerExecPlan)
      )
    )
    addAbsentFunctionMapper(
      aggregatePlanResult,
      Seq(),
      RangeParams(
        sqww.startMs / 1000, sqww.stepMs / 1000, sqww.endMs / 1000
      ),
      qContext
    )
    aggregatePlanResult
  }

  def materializeTopLevelSubquery(qContext: QueryContext, tlsq: TopLevelSubquery): PlanResult = {
    // This physical plan will try to execute only one range query instead of a number of individual subqueries.
    // If ranges of each of the subqueries have overlap, retrieving the total range
    // is optimal, if there is no overlap and even worse significant gap between the individual subqueries, retrieving
    // the entire range might be suboptimal, this still might be a better option than issuing and concatenating numerous
    // subqueries separately
    walkLogicalPlanTree(tlsq.innerPeriodicSeries, qContext)
  }

  def materializeScalarTimeBased(qContext: QueryContext,
                                  lp: ScalarTimeBasedPlan): PlanResult = {
    val scalarTimeBasedExec = TimeScalarGeneratorExec(qContext, dataset.ref, lp.rangeParams, lp.function)
    PlanResult(Seq(scalarTimeBasedExec))
  }

   def materializeScalarBinaryOperation(qContext: QueryContext,
                                        lp: ScalarBinaryOperation): PlanResult = {
    val lhs = if (lp.lhs.isRight) {
      // Materialize as lhs is a logical plan
      val lhsExec = walkLogicalPlanTree(lp.lhs.right.get, qContext)
      Right(lhsExec.plans.map(_.asInstanceOf[ScalarBinaryOperationExec]).head)
    } else Left(lp.lhs.left.get)

    val rhs = if (lp.rhs.isRight) {
      val rhsExec = walkLogicalPlanTree(lp.rhs.right.get, qContext)
      Right(rhsExec.plans.map(_.asInstanceOf[ScalarBinaryOperationExec]).head)
    } else Left(lp.rhs.left.get)

    val scalarBinaryExec = ScalarBinaryOperationExec(qContext, dataset.ref, lp.rangeParams, lhs, rhs, lp.operator)
    PlanResult(Seq(scalarBinaryExec))
  }

  def materializeBinaryJoin(qContext: QueryContext, logicalPlan: BinaryJoin): PlanResult = {

    val lhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.lhs)))
    val rhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.rhs)))

    val lhs = walkLogicalPlanTree(logicalPlan.lhs, lhsQueryContext)
    val rhs = walkLogicalPlanTree(logicalPlan.rhs, rhsQueryContext)

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
          LogicalPlanUtils.renameLabels(logicalPlan.on, dsOptions.metricColumn),
          LogicalPlanUtils.renameLabels(logicalPlan.ignoring, dsOptions.metricColumn), dsOptions.metricColumn,
          rvRangeFromPlan(logicalPlan))
      else
        BinaryJoinExec(qContext, dispatcher, stitchedLhs, stitchedRhs, logicalPlan.operator,
          logicalPlan.cardinality, LogicalPlanUtils.renameLabels(logicalPlan.on, dsOptions.metricColumn),
          LogicalPlanUtils.renameLabels(logicalPlan.ignoring, dsOptions.metricColumn), logicalPlan.include,
          dsOptions.metricColumn, rvRangeFromPlan(logicalPlan))

    PlanResult(Seq(execPlan))
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

    children.find(_.dispatcher.isLocalCall).map(_.dispatcher).getOrElse {
    val childTargets = children.map(_.dispatcher)
    // Above list can contain duplicate dispatchers, and we don't make them distinct.
    // Those with more shards must be weighed higher
    val rnd = ThreadLocalRandom.current()
    childTargets.iterator.drop(rnd.nextInt(childTargets.size)).next
   }
  }
}
