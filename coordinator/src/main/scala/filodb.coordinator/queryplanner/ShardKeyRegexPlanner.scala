package filodb.coordinator.queryplanner

import filodb.core.metadata.{Dataset, DatasetOptions, Schemas}
import filodb.core.query.{ColumnFilter, PromQlQueryParams, QueryConfig, QueryContext, RangeParams}
import filodb.query._
import filodb.query.LogicalPlan._
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

class ShardKeyRegexPlanner(val dataset: Dataset,
                           queryPlanner: QueryPlanner,
                           shardKeyMatcher: Seq[ColumnFilter] => Seq[Seq[ColumnFilter]],
                           config: QueryConfig)
  extends QueryPlanner with DefaultPlanner {

  override def queryConfig: QueryConfig = config
  override val schemas: Schemas = Schemas(dataset.schema)
  override val dsOptions: DatasetOptions = schemas.part.options
  private val datasetMetricColumn = dataset.options.metricColumn

  /**
   * Returns true when regex has single matching value
   * Example: sum(test1{_ws_ = "demo", _ns_ =~ "App-1"}) + sum(test2{_ws_ = "demo", _ns_ =~ "App-1"})
   */
  private def hasSingleShardKeyMatch(nonMetricShardKeyFilters: Seq[Seq[ColumnFilter]]) = {
    // Filter out the empty Seq's (which indicate scalars).
    val shardKeyMatchers = nonMetricShardKeyFilters.map(shardKeyMatcher(_)).filter(_.nonEmpty)
    shardKeyMatchers.forall(_.size == 1) &&
      shardKeyMatchers.forall(_.head.toSet == shardKeyMatchers.head.head.toSet)
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
    if (isMetadataQuery(logicalPlan)
      || (hasRequiredShardKeysPresent(nonMetricShardKeyFilters, dataset.options.nonMetricShardColumns) &&
      LogicalPlan.hasShardKeyEqualsOnly(logicalPlan, dataset.options.nonMetricShardColumns))) {
      queryPlanner.materialize(logicalPlan, qContext)
    } else if (hasSingleShardKeyMatch(nonMetricShardKeyFilters)) {
      // For queries like topk(2, test{_ws_ = "demo", _ns_ =~ "App-1"}) which have just one matching value
      generateExecWithoutRegex(logicalPlan, nonMetricShardKeyFilters.head, qContext).head
    } else walkLogicalPlanTree(logicalPlan, qContext).plans.head
  }


  /**
   * Checks if all the nonMetricShardKeyFilters are wither empty or have the required shard columns in them.
   *
   * @param nonMetricShardKeyFilters The leaf level plan's shard key columns
   * @param nonMetricShardColumns The required shard key columns defined in the schema
   * @return true of all nonMetricShardKeyFilters are either empty or have the shard key columns
   */
  private[queryplanner] def hasRequiredShardKeysPresent(nonMetricShardKeyFilters: Seq[Seq[ColumnFilter]],
                                  nonMetricShardColumns: Seq[String]): Boolean = {
    val nonMetricShardColumnsSet = nonMetricShardColumns.toSet
    nonMetricShardKeyFilters.forall { filterGroup =>
      filterGroup.isEmpty || nonMetricShardColumnsSet.subsetOf(filterGroup.map(_.column).toSet)
    }
  }


  def isMetadataQuery(logicalPlan: LogicalPlan): Boolean = {
    logicalPlan match {
      case _: MetadataQueryPlan => true
      case _: TsCardinalities => true
      case _ => false
    }
  }

  override def walkLogicalPlanTree(logicalPlan: LogicalPlan,
                                   qContext: QueryContext,
                                   forceInProcess: Boolean = false): PlanResult = {
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
      case lp: SubqueryWithWindowing       => super.materializeSubqueryWithWindowing(qContext, lp)
      case lp: TopLevelSubquery            => super.materializeTopLevelSubquery(qContext, lp)
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
      queryPlanner.materialize(newLogicalPlan, newQueryContext)
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

    // FIXME, Optimize and push the lhs and rhs to wrapped planner if they belong to same partition

    val lhsExec = materialize(logicalPlan.lhs, lhsQueryContext)
    val rhsExec = materialize(logicalPlan.rhs, rhsQueryContext)

    val execPlan = if (logicalPlan.operator.isInstanceOf[SetOperator])
      SetOperatorExec(qContext, inProcessPlanDispatcher, Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
        LogicalPlanUtils.renameLabels(logicalPlan.on, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn), datasetMetricColumn,
        rvRangeFromPlan(logicalPlan))
    else
      BinaryJoinExec(qContext, inProcessPlanDispatcher, Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
        logicalPlan.cardinality, LogicalPlanUtils.renameLabels(logicalPlan.on, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.include, datasetMetricColumn), datasetMetricColumn,
        rvRangeFromPlan(logicalPlan))

    PlanResult(Seq(execPlan))
  }

  private def canSupportMultiPartitionCalls(execPlans: Seq[ExecPlan]): Boolean =
    execPlans.forall{
      case _: PromQlRemoteExec  => false
      case _                    => true
    }

  /***
   * For aggregate queries like sum(test{_ws_ = "demo", _ns_ =~ "App.*"})
   * It will be broken down to sum(test{_ws_ = "demo", _ns_ = "App-1"}), sum(test{_ws_ = "demo", _ns_ = "App-2"}) etc
   * Sub query could be across multiple partitions so aggregate using MultiPartitionReduceAggregateExec
   * */
  private def materializeAggregate(aggregate: Aggregate, queryContext: QueryContext): PlanResult = {
    // Pushing down aggregates to run on individual partitions is most efficient, however, if we have multiple
    // nested aggregation we should be pushing down the lowest aggregate for correctness. Consider the following query
    // count(sum by(foo)(test1{_ws_ = "demo", _ns_ =~ "App-.*"})), doing a count of the counts received from individual
    // partitions may not give the correct answer, we should push down the sum to multiple partitions, perform a reduce
    // locally and then run count on the results. The following implementation checks for descendant aggregates, if
    // there are any, the provided aggregation needs to be done using inProcess, else we can materialize the aggregate
    // using the wrapped planner
    val plan = if (LogicalPlanUtils.hasDescendantAggregate(aggregate.vectors)) {
      val childPlan = materialize(aggregate.vectors, queryContext)
      // We are here because we have descendent aggregate, if that was multi-partition, the dispatcher will
      // be InProcessPlanDispatcher and adding the current aggregate using addAggregate will use the same dispatcher
      // If the underlying plan however is not multi partition, adding the aggregator using addAggregator will
      // use the same dispatcher
      addAggregator(aggregate, queryContext, PlanResult(Seq(childPlan)))
    } else {
      val execPlans = generateExecWithoutRegex(aggregate,
        LogicalPlan.getNonMetricShardKeyFilters(aggregate, dataset.options.nonMetricShardColumns).head, queryContext)
      val exec = if (execPlans.size == 1) execPlans.head
      else {
        if ((aggregate.operator.equals(AggregationOperator.TopK)
          || aggregate.operator.equals(AggregationOperator.BottomK)
          || aggregate.operator.equals(AggregationOperator.CountValues)
          ) && !canSupportMultiPartitionCalls(execPlans))
          throw new UnsupportedOperationException(s"Shard Key regex not supported for ${aggregate.operator}")
        else {
          val reducer = MultiPartitionReduceAggregateExec(queryContext, inProcessPlanDispatcher,
            execPlans.sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec]), aggregate.operator, aggregate.params)
          val promQlQueryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
          reducer.addRangeVectorTransformer(AggregatePresenter(aggregate.operator, aggregate.params,
            RangeParams(promQlQueryParams.startSecs, promQlQueryParams.stepSecs, promQlQueryParams.endSecs)))
          reducer
        }
      }
      exec
    }
    PlanResult(Seq(plan))
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
      if (execPlans.size == 1)
        execPlans.head
      // TODO
      // here we essentially do not allow to optimize the physical plan for subqueries
      // as we concat the results with MultiPartitionDisConcatExec,
      // below queries:
      // max_over_time(rate(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }[10m])[1h:1m])
      // sum_over_time(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }[5d:300s])
      // would have suboptimal performance. See subquery tests in PlannerHierarchySpec
      else
        MultiPartitionDistConcatExec(
          queryContext, inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec])
        )
    }
    PlanResult(Seq(exec))
  }
}
