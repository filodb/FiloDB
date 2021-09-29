package filodb.coordinator.queryplanner

import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.query.{ColumnFilter, PromQlQueryParams, QueryConfig, QueryContext, RangeParams}
import filodb.query.exec.InternalRangeFunction.Last
import filodb.query._
import filodb.query.exec._

/**
 * Holder for the shard key regex matcher results.
 *
 * @param columnFilters - non metric shard key filters
 * @param query - query
 */
case class ShardKeyMatcher(columnFilters: Seq[ColumnFilter], query: String)

/**
  * Responsible for query planning for queries having regex in shard column
  *
  * @param dataset         dataset
  * @param queryPlanner    multiPartition query planner
  * @param shardKeyMatcher used to get values for regex shard keys. Each inner sequence corresponds to matching regex
  * value. For example: Seq(ColumnFilter(ws, Equals(demo)), ColumnFilter(ns, EqualsRegex(App*)) returns
  * Seq(Seq(ColumnFilter(ws, Equals(demo)), ColumnFilter(ns, Equals(App1))), Seq(ColumnFilter(ws, Equals(demo)),
  * ColumnFilter(ns, Equals(App2))
  */

class ShardKeyRegexPlanner(dataset: Dataset,
                           queryPlanner: QueryPlanner,
                           shardKeyMatcher: Seq[ColumnFilter] => Seq[Seq[ColumnFilter]],
                           queryConfig: QueryConfig)
  extends QueryPlanner with PlannerMaterializer {
  val datasetMetricColumn = dataset.options.metricColumn
  val inProcessPlanDispatcher = InProcessPlanDispatcher(queryConfig)

  override val schemas = Schemas(dataset.schema)

  /**
   * Returns true when regex has single matching value
   * Example: sum(test1{_ws_ = "demo", _ns_ =~ "App-1"}) + sum(test2{_ws_ = "demo", _ns_ =~ "App-1"})
   */
  private def hasSingleShardKeyMatch(nonMetricShardKeyFilters: Seq[Seq[ColumnFilter]]) = {
    val shardKeyMatchers = nonMetricShardKeyFilters.map(shardKeyMatcher(_))
    shardKeyMatchers.forall(_.size == 1) &&
      shardKeyMatchers.forall(_.head.toSet.sameElements(shardKeyMatchers.head.head.toSet))
    // ^^ For Binary join LHS and RHS should have same value
  }

  /**
    * Converts a logical plan to execution plan.
    *
    * @param logicalPlan Logical plan after converting PromQL -> AST -> LogicalPlan
    * @param qContext    holder for additional query parameters
    * @return materialized Execution Plan which can be dispatched
    */
  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    val nonMetricShardKeyFilters =
      LogicalPlan.getNonMetricShardKeyFilters(logicalPlan, dataset.options.nonMetricShardColumns)
    if (LogicalPlan.hasShardKeyEqualsOnly(logicalPlan, dataset.options.nonMetricShardColumns)) {
      queryPlanner.materialize(logicalPlan, qContext)
    } else if (hasSingleShardKeyMatch(nonMetricShardKeyFilters)) {
      // For queries like topk(2, test{_ws_ = "demo", _ns_ =~ "App-1"}) which have just one matching value
      generateExecWithoutRegex(logicalPlan, nonMetricShardKeyFilters.head, qContext).head
    } else walkLogicalPlanTree(logicalPlan, qContext).plans.head
  }

   def walkLogicalPlanTree(logicalPlan: LogicalPlan,
                           qContext: QueryContext): PlanResult = {
    logicalPlan match {
      case lp: ApplyMiscellaneousFunction  => materializeApplyMiscellaneousFunction(qContext, lp)
      case lp: ApplyInstantFunction        => materializeApplyInstantFunction(qContext, lp)
      case lp: ApplyInstantFunctionRaw     => materializeApplyInstantFunctionRaw(qContext, lp)
      case lp: ScalarVectorBinaryOperation => materializeScalarVectorBinOp(qContext, lp)
      case lp: ApplySortFunction           => materializeApplySortFunction(qContext, lp)
      case lp: ScalarVaryingDoublePlan     => materializeScalarPlan(qContext, lp)
      case lp: ApplyAbsentFunction         => materializeAbsentFunction(qContext, lp)
      case lp: VectorPlan                  => materializeVectorPlan(qContext, lp)
      case lp: Aggregate                   => materializeAggregate(lp, qContext)
      case lp: BinaryJoin                  => materializeBinaryJoin(lp, qContext)
      case lp: LabelValues                 => PlanResult(Seq(queryPlanner.materialize(lp, qContext)))
      case lp: LabelNames                  => PlanResult(Seq(queryPlanner.materialize(lp, qContext)))
      case lp: SeriesKeysByFilters         => PlanResult(Seq(queryPlanner.materialize(lp, qContext)))
      case lp: PeriodicSeriesWithWindowing => if (lp.function == RangeFunctionId.AbsentOverTime)
                                              materializeAbsentOverTime(lp, qContext)
                                             else materializeOthers(logicalPlan, qContext)
      case _                               => materializeOthers(logicalPlan, qContext)
    }
  }

  private def generateExecWithoutRegex(logicalPlan: LogicalPlan, nonMetricShardKeyFilters: Seq[ColumnFilter],
                                       qContext: QueryContext): Seq[ExecPlan] = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val shardKeyMatches = shardKeyMatcher(nonMetricShardKeyFilters)
    val skipAggregatePresentValue = if (shardKeyMatches.length == 1) false else true
    shardKeyMatches.map { result =>
        val newLogicalPlan = logicalPlan.replaceFilters(result)
        // Querycontext should just have the part of query which has regex
        // For example for exp(sum(test{_ws_ = "demo", _ns_ =~ "App.*"})), sub queries should be
        // sum(test{_ws_ = "demo", _ns_ = "App-1"}), sum(test{_ws_ = "demo", _ns_ = "App-2"}) etc
        val newQueryParams = queryParams.copy(promQl = LogicalPlanParser.convertToQuery(newLogicalPlan))
        val newQueryContext = qContext.copy(origQueryParams = newQueryParams, plannerParams = qContext.plannerParams.
          copy(skipAggregatePresent = skipAggregatePresentValue))
        queryPlanner.materialize(logicalPlan.replaceFilters(result), newQueryContext)
      }
  }

  /**
    * For binary join queries like test1{_ws_ = "demo", _ns_ =~ "App.*"} + test2{_ws_ = "demo", _ns_ =~ "App.*"})
    * LHS and RHS could be across multiple partitions
    */
  private def materializeBinaryJoin(logicalPlan: BinaryJoin, qContext: QueryContext): PlanResult = {
    val lhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.lhs)))
    val rhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.rhs)))

    val lhsExec = materialize(logicalPlan.lhs, lhsQueryContext)
    val rhsExec = materialize(logicalPlan.rhs, rhsQueryContext)

    val onKeysReal = ExtraOnByKeysUtil.getRealOnLabels(logicalPlan, queryConfig.addExtraOnByKeysTimeRanges)

    val execPlan = if (logicalPlan.operator.isInstanceOf[SetOperator])
         SetOperatorExec(qContext, inProcessPlanDispatcher, Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
          LogicalPlanUtils.renameLabels(onKeysReal, datasetMetricColumn),
          LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn), datasetMetricColumn)
      else
         BinaryJoinExec(qContext, inProcessPlanDispatcher, Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
          logicalPlan.cardinality, LogicalPlanUtils.renameLabels(onKeysReal, datasetMetricColumn),
          LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn),
          LogicalPlanUtils.renameLabels(logicalPlan.include, datasetMetricColumn), datasetMetricColumn)
     PlanResult(Seq(execPlan))
  }

  /***
    * For aggregate queries like sum(test{_ws_ = "demo", _ns_ =~ "App.*"})
    * It will be broken down to sum(test{_ws_ = "demo", _ns_ = "App-1"}), sum(test{_ws_ = "demo", _ns_ = "App-2"}) etc
    * Sub query could be across multiple partitions so aggregate using MultiPartitionReduceAggregateExec
    * */
  private def materializeAggregate(aggregate: Aggregate, queryContext: QueryContext): PlanResult = {
    val execPlans = generateExecWithoutRegex(aggregate,
      LogicalPlan.getNonMetricShardKeyFilters(aggregate, dataset.options.nonMetricShardColumns).head, queryContext)
    val exec = if (execPlans.size == 1) execPlans.head
    else {
      if (aggregate.operator.equals(AggregationOperator.TopK) || aggregate.operator.equals(AggregationOperator.BottomK)
        || aggregate.operator.equals(AggregationOperator.CountValues))
         throw new UnsupportedOperationException(s"Shard Key regex not supported for ${aggregate.operator}")
      val reducer = MultiPartitionReduceAggregateExec(queryContext, inProcessPlanDispatcher,
        execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlRemoteExec]), aggregate.operator, aggregate.params)
      val promQlQueryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
      reducer.addRangeVectorTransformer(AggregatePresenter(aggregate.operator, aggregate.params,
        RangeParams(promQlQueryParams.startSecs, promQlQueryParams.stepSecs, promQlQueryParams.endSecs)))
      reducer
    }
    PlanResult(Seq(exec))
  }

  /***
    * For non aggregate & non binary join queries like test{_ws_ = "demo", _ns_ =~ "App.*"}
    * It will be broken down to test{_ws_ = "demo", _ns_ = "App-1"}, test{_ws_ = "demo", _ns_ = "App-2"} etc
    * Sub query could be across multiple partitions so concatenate using MultiPartitionDistConcatExec
    * */
  private def materializeOthers(logicalPlan: LogicalPlan, queryContext: QueryContext): PlanResult = {
    val nonMetricShardKeyFilters =
      LogicalPlan.getNonMetricShardKeyFilters(logicalPlan, dataset.options.nonMetricShardColumns)
    // For queries which don't have RawSeries filters like metadata and fixed scalar queries
    val exec = if (nonMetricShardKeyFilters.head.isEmpty) queryPlanner.materialize(logicalPlan, queryContext)
    else {
      val execPlans = generateExecWithoutRegex(logicalPlan, nonMetricShardKeyFilters.head, queryContext)
      if (execPlans.size == 1) execPlans.head else MultiPartitionDistConcatExec(queryContext, inProcessPlanDispatcher,
        execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlRemoteExec]))
    }
    PlanResult(Seq(exec))
  }

  private def materializeAbsentOverTime(logicalPlan: PeriodicSeriesWithWindowing, queryContext: QueryContext):
  PlanResult = {
    val vectors = walkLogicalPlanTree(logicalPlan.series, queryContext)
    val paramsExec = materializeFunctionArgs(logicalPlan.functionArgs, queryContext)
    // For queries which don't have RawSeries filters like metadata and fixed scalar queries
    val aggregate = Aggregate(AggregationOperator.Sum, logicalPlan, Nil, Seq("job"))
      vectors.plans.
        map {
        e => if (!e.isInstanceOf[PromQlRemoteExec]) {
          e.children.foreach(c => c.children.foreach(_.addRangeVectorTransformer(
            PeriodicSamplesMapper(logicalPlan.startMs,
            logicalPlan.stepMs, logicalPlan.endMs, Some(logicalPlan.window), Some(Last), queryContext,
            logicalPlan.stepMultipleNotationUsed,
            paramsExec, logicalPlan.offsetMs, logicalPlan.series.isRaw))))


          PlanResult(e.children)
        } else
         PlanResult(Seq(e))
      }


    val aggregatePlanResult = PlanResult (Seq(addAggregator(aggregate, queryContext.copy(plannerParams =
      queryContext.plannerParams.copy(skipAggregatePresent = true)), vectors, Seq.empty)))
      // Add sum to aggregate all child responses
      // If all children have NaN value, sum will yield NaN and AbsentFunctionMapper will yield 1

      addAbsentFunctionMapper(aggregatePlanResult, logicalPlan.columnFilters,
        RangeParams(logicalPlan.startMs / 1000, logicalPlan.stepMs / 1000,
          logicalPlan.endMs / 1000), queryContext)
      aggregatePlanResult
    }

}
