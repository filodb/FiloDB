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
                            qContext: QueryContext,
                            forceInProcess: Boolean = false): PlanResult

    /**
     * DefaultPlanner has logic to handle multiple LogicalPlans, classes implementing this trait may choose to override
     * the behavior and delegate to the default implementation when no special implementation if required.
     * The method is similar to walkLogicalPlanTree but deliberately chosen to have a different name to for the
     * classes to implement walkLogicalPlanTree and explicitly delegate to defaultWalkLogicalPlanTree if needed. The
     * method essentially pattern matches all LogicalPlans and invoke the default implementation in the
     * DefaultPlanner trait
     *
     * @param logicalPlan The LogicalPlan instance
     * @param qContext The QueryContext
     * @param forceInProcess if true, all materialized plans will dispatch via an InProcessDispatcher
     * @return The PlanResult containing the ExecPlan
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
        case _: RawSeries                   |
             _: RawChunkMeta                |
             _: PeriodicSeries              |
             _: PeriodicSeriesWithWindowing |
             _: MetadataQueryPlan           |
             _: TsCardinalities              => throw new IllegalArgumentException("Unsupported operation")
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
      if (lp.function == MiscellaneousFunctionId.HistToPromVectors)
        vectors.plans.foreach(_.addRangeVectorTransformer(HistToPromSeriesMapper(schemas.part)))
      else
        vectors.plans.foreach(_.addRangeVectorTransformer(MiscellaneousFunctionMapper(lp.function, lp.stringArgs)))
      vectors
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
   def addAggregator(lp: Aggregate,
                     qContext: QueryContext,
                     toReduceLevel: PlanResult,
                     disp: Option[PlanDispatcher] = None):
   LocalPartitionReduceAggregateExec = {

    import filodb.query.AggregateClause.ClauseType

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
        clause.clauseType match {
          case ClauseType.By =>
            AggregateClause(ClauseType.By, renamedLabels)
          case ClauseType.Without =>
            AggregateClause(ClauseType.Without, renamedLabels)
        }
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
          val reduceDispatcher = nodePlans.head.dispatcher
          LocalPartitionReduceAggregateExec(qContext, reduceDispatcher, nodePlans, lp.operator, lp.params)
        }.toList
      } else toReduceLevel.plans

    val reduceDispatcher = disp.getOrElse(PlannerUtil.pickDispatcher(toReduceLevel2))
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
    val toReduceLevel1 = walkLogicalPlanTree(lp.vectors, qContext, forceInProcess)
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
    val scalarTimeBasedExec = TimeScalarGeneratorExec(qContext, dataset.ref, lp.rangeParams, lp.function)
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

    val scalarBinaryExec = ScalarBinaryOperationExec(qContext, dataset.ref, lp.rangeParams, lhs, rhs, lp.operator)
    PlanResult(Seq(scalarBinaryExec))
  }

  def materializeBinaryJoin(qContext: QueryContext,
                            logicalPlan: BinaryJoin,
                            forceInProcess: Boolean = false): PlanResult = {

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
