package filodb.coordinator.queryplanner

import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.query.{ColumnFilter, PromQlQueryParams, QueryConfig, QueryContext, RangeParams}
import filodb.query.{SetOperator, _}
import filodb.query.LogicalPlan.{getNonMetricShardKeyFilters, _}
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
  val datasetMetricColumn: String = dataset.options.metricColumn
  val inProcessPlanDispatcher: PlanDispatcher = InProcessPlanDispatcher(queryConfig)

  override val schemas: Schemas = Schemas(dataset.schema)

  /**
   * Returns true when regex has single matching value
   * Example: sum(test1{_ws_ = "demo", _ns_ =~ "App-1"}) + sum(test2{_ws_ = "demo", _ns_ =~ "App-1"})
   */
  private def hasSingleShardKeyMatch(nonMetricShardKeyFilters: Seq[Seq[ColumnFilter]]): Boolean = {
    val shardKeyMatchers = nonMetricShardKeyFilters.map(shardKeyMatcher(_))
    shardKeyMatchers.forall(_.size == 1) &&
      shardKeyMatchers.forall(_.head.toSet.equals(shardKeyMatchers.head.head.toSet))
    // ^^ For Binary join LHS and RHS should have same value
  }

  /**
   * Converts a logical plan to execution plan.
   *
   * @param logicalPlan Logical plan after converting PromQL -> AST -> LogicalPlan
   * @param qContext    holder for additional query parameters
   * @return materialized Execution Plan which can be dispatched
   */
  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan =
    walkLogicalPlanTree(logicalPlan, qContext).plans.head

  def walkLogicalPlanTree(logicalPlan: LogicalPlan,
                          qContext: QueryContext): PlanResult =
   walkLogicalPlanTreeInternal(logicalPlan, qContext, isRoot = true).map(x => PlanResult(Seq(x))).head

  /**
   * Takes a LogicalPlan and converts then into a sequence of ExecPlan instances, one for each possible match for the
   * regex given in the _ns_ label
   *
   * @param logicalPlan the LogicalPLan instance
   * @param nonMetricShardKeyFilters Seq of ColumnFilters for except for the Metric Column
   * @param qContext the QueryContext instance
   * @return Seq of ExecPlans, one for each match as determined by the shardKeyMatcher
   */
  private def generateExecWithoutRegex(logicalPlan: LogicalPlan, nonMetricShardKeyFilters: Seq[ColumnFilter],
                                       qContext: QueryContext): Seq[ExecPlan] = {
    if (nonMetricShardKeyFilters.forall(f => f.filter.isInstanceOf[filodb.core.query.Filter.Equals]))
      // This means there is no EqualsRegex and we don't need to invoke the shardKeyMatcher, simply materialize
      Seq(queryPlanner.materialize(logicalPlan, qContext))
    else {
      val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
      val shardKeyMatches = shardKeyMatcher(nonMetricShardKeyFilters)
      val skipAggregatePresentValue = if (shardKeyMatches.length == 1) false else true
      shardKeyMatches.map { result =>
        val newLogicalPlan = logicalPlan.replaceFilters(result)
        // QueryContext should just have the part of query which has regex
        // For example for exp(sum(test{_ws_ = "demo", _ns_ =~ "App.*"})), sub queries should be
        // sum(test{_ws_ = "demo", _ns_ = "App-1"}), sum(test{_ws_ = "demo", _ns_ = "App-2"}) etc
        val newQueryParams = queryParams.copy(promQl = LogicalPlanParser.convertToQuery(newLogicalPlan))
        val newQueryContext = qContext.copy(origQueryParams = newQueryParams, plannerParams = qContext.plannerParams.
          copy(skipAggregatePresent = skipAggregatePresentValue))
        queryPlanner.materialize(logicalPlan.replaceFilters(result), newQueryContext)
      }
    }
  }

  def walkLogicalPlanTreeInternal(plan: LogicalPlan, qContext: QueryContext, isRoot: Boolean): Option[ExecPlan] = {

    plan match {
      case lp: ApplyMiscellaneousFunction => materializeApplyMiscellaneousFunction(lp, qContext, isRoot)
      case lp: ApplyInstantFunction => materializeApplyInstantFunction(lp, qContext, isRoot)
      // TODO: What would be a good test case for ApplyInstantFunctionRaw ?
      case lp: ApplyInstantFunctionRaw => materializeApplyInstantFunctionRaw(lp, qContext, isRoot)
      case lp: ScalarVectorBinaryOperation => materializeScalarVectorBinOp(lp, qContext, isRoot)
      case lp: ApplySortFunction => materializeApplySortFunction(lp, qContext, isRoot)
      case lp: ScalarVaryingDoublePlan => materializeScalarPlan(lp, qContext, isRoot)
      case lp: ApplyAbsentFunction => materializeAbsentFunction(lp, qContext, isRoot)
      case lp: VectorPlan => materializeVectorPlan(lp, qContext, isRoot)
      case aggregate: Aggregate => materializeAggregate(aggregate, qContext, isRoot)
      case bin: BinaryJoin => materializeBinaryJoin(qContext, bin, isRoot)
      case lp: LabelValues  => Some(queryPlanner.materialize(lp, qContext))
      // TODO: No test case for SeriesKeysByFilters, what can be a good Unit test
      case lp: SeriesKeysByFilters => Some(queryPlanner.materialize(lp, qContext))
      // TODO: Can we prevent the materializing leaf node if not necessary?
      //case _: PeriodicSeries  | _: RawSeries if !isRoot => None
      case _ => materializeOthers(plan, qContext).plans match {
        case plan :: Nil => Some(plan)
        case _ => None
      }
    }
  }

  // TODO: IMPORTANT, some of the super class methods assume we can get a list of plans, this however is not the case
  //  at least in case of ShardKeyRegexPlanner, is this correct assumption?


  private def materializeAbsentFunction(lp: ApplyAbsentFunction, qContext: QueryContext, isRoot: Boolean) =
    materializeApplyGenericFunction(lp, qContext, isRoot, x => {
      val aggregate = Aggregate(AggregationOperator.Sum, lp, Nil, Seq("job"))
      // Add sum to aggregate all child responses
      // If all children have NaN value, sum will yield NaN and AbsentFunctionMapper will yield 1
      val aggregatePlanResult = addAggregator(aggregate, qContext.copy(plannerParams =
        qContext.plannerParams.copy(skipAggregatePresent = true)),// No need for present for sum
        PlanResult(Seq(x)),
        Seq.empty)
      aggregatePlanResult.addRangeVectorTransformer(
        AbsentFunctionMapper(lp.columnFilters, RangeParams(lp.startMs / 1000, lp.stepMs / 1000, lp.endMs / 1000),
        dsOptions.metricColumn))
      aggregatePlanResult
    }, {case  lp: ApplyAbsentFunction  => lp.vectors})

  private def materializeApplyMiscellaneousFunction(lp: ApplyMiscellaneousFunction, qContext: QueryContext,
                                                    isRoot: Boolean) =
    materializeApplyGenericFunction(lp, qContext, isRoot, x => {
      if (lp.function == MiscellaneousFunctionId.HistToPromVectors)
        x.addRangeVectorTransformer(HistToPromSeriesMapper(schemas.part))
      else
        x.addRangeVectorTransformer(MiscellaneousFunctionMapper(lp.function, lp.stringArgs))
      x
    }, {case  lp: ApplyMiscellaneousFunction  => lp.vectors})


  private def materializeApplySortFunction(lp: ApplySortFunction, qContext: QueryContext, isRoot: Boolean) =
    materializeApplyGenericFunction(lp, qContext, isRoot, x => {
      x.addRangeVectorTransformer(SortFunctionMapper(lp.function))
      x
    }, {case  lp: ApplySortFunction  => lp.vectors})

  private def materializeVectorPlan(lp: VectorPlan, qContext: QueryContext,
                                                    isRoot: Boolean) =
    materializeApplyGenericFunction(lp, qContext, isRoot, x => {
        x.addRangeVectorTransformer(VectorFunctionMapper())
        x
      }, {case  lp: VectorPlan  => lp.scalars})

  private def materializeScalarPlan(lp: ScalarVaryingDoublePlan, qContext: QueryContext, isRoot: Boolean) =
    materializeApplyGenericFunction(lp, qContext, isRoot, x => {
      x.addRangeVectorTransformer(ScalarFunctionMapper(lp.function, RangeParams(lp.startMs, lp.stepMs, lp.endMs)))
      x
    }, {case  lp: ScalarVaryingDoublePlan  => lp.vectors})

  private def materializeScalarVectorBinOp(lp: ScalarVectorBinaryOperation, qContext: QueryContext, isRoot: Boolean) =
    materializeApplyGenericFunction(lp, qContext, isRoot, x => {
      val funcArg = materializeFunctionArgs(Seq(lp.scalarArg), qContext)
      x.addRangeVectorTransformer(ScalarOperationMapper(lp.operator, lp.scalarIsLhs, funcArg))
      x
    }, {case  lp: ScalarVectorBinaryOperation  => lp.vector})

  private def materializeApplyInstantFunctionRaw(lp: ApplyInstantFunctionRaw, qContext: QueryContext, isRoot: Boolean) =
      materializeApplyGenericFunction(lp, qContext, isRoot, x => {
        x.addRangeVectorTransformer(
            InstantVectorFunctionMapper(lp.function, materializeFunctionArgs(lp.functionArgs, qContext))
        )
        x
      }, {case  lp: ApplyInstantFunctionRaw  => lp.vectors})

  private def materializeApplyInstantFunction(lp: ApplyInstantFunction, qContext: QueryContext, isRoot: Boolean) =
    materializeApplyGenericFunction(lp, qContext, isRoot, x => {
      x.addRangeVectorTransformer(
        InstantVectorFunctionMapper(lp.function, materializeFunctionArgs(lp.functionArgs, qContext))
      )
      x
    }, {case  lp: ApplyInstantFunction  => lp.vectors})

  /**
   * All operations apart from joins, aggregates and raw data retrieval do not need a new ExecPlan but just apply
   * RangeVectorTransformers. Applying these transformers does not generate a new ExecPlan but passes the retrieved
   * data through a transformation pipeline. This is synonymous to data.map(f1).map(f2).. where f1, f2 are
   * transformations to be applied in a particular order
   *
   * @param lp LogicalPlan for the Query
   * @param qContext QueryContext
   * @param isRoot boolean flag indicating whether the passed LogicalPlan is a root node
   * @param fn The mapping function which accepts an ExecPlan, adds the RangeVectorTransformation and returns the
   *           updated ExecPlan. In majority of cases the ExecPlan does not change and the mapping functions invokes
   *           the mutable addRangeVectorTransformer method on the ExecPlan, however in cases like absent, we possibly
   *           can return a completely new ExecPlan on top of the one passed.
   * @param childNodes A partial function that given a LogicalPlan returns the LogicalPlan for the child vector/scalar
   *                   nodes. The reason this is a partial function is because the function is not necessarily defined
   *                   for all possible values LogicalPlan types in a given scenario. For example, the function may
   *                   just be defined for a specific type ApplyInstantFunction of the LogicalPlan as input and not
   *                   for all possible subtypes of LogicalPlan
   * @return An Option None if it is not necessary to materialize this function and let the parent node materialize
   *         the query or Some(ExecPlan) if the a child node in the tree is already materialized or this node is root
   */
  private def materializeApplyGenericFunction(lp: LogicalPlan,
                                              qContext: QueryContext,
                                              isRoot: Boolean,
                                              fn: ExecPlan => ExecPlan,
                                              childNodes: PartialFunction[LogicalPlan, LogicalPlan])
  : Option[ExecPlan] = walkLogicalPlanTreeInternal(childNodes(lp), qContext, isRoot = false) match {
      case None =>
        // Because we got None, none of the child nodes are materialized, we now have two choices, either, materialize
        // now, or let one of the parent materialize the tree. Also because we have got None, it also means all
        // operations starting from this node are not spanning multi-partition
        if (isRoot) {
          // materialize now
          // guaranteed to generate only one ExecPlan
          Some(materializeOthers(lp, qContext).plans.head)
        } else
        // Let one of the parent node materialize itself and its subtree
          None
      case plan@Some(_: DistConcatExec) =>
        // The child is Raw partition selection, Local/Multi partition. Only if this node is the root
        // for e.g ln(test1{...}), then add that instant function to transformer, else let the parent materialize
        // e.g. sum(ln(test1{})), in this case let the parent aggregation be materialized
        if (isRoot) plan.map(fn) else None
      case plan@_ =>
        // Some node in the tree at a lower level is already materialized, simply add the instant Function and
        // return the plan with the Instant function wrapped in current process
        plan.map(fn)
    }


  private def materializeAggregate(aggregate: Aggregate, qContext: QueryContext, isRoot: Boolean) = {
    val childPlan = walkLogicalPlanTreeInternal(aggregate.vectors, qContext, isRoot = false)
    childPlan match {
      case None | Some(_: DistConcatExec) =>
        // No other operation under this aggregate is materialized, or the underlying operation is simply concatenating
        // results. If the underlying plan is DistConcatExec, then we can push the plan materialization up the tree
        // Consider the case sum(count by(foo)(test1{_ws_ = "demo", _ns_ =~ "App-.*"})), here test1{...} will return
        // a MultiPartitionDistConcatExec, however, we can push the count aggregate to a partition
        val nonMetricShardKeyFilters = getNonMetricShardKeyFilters(aggregate, dataset.options.nonMetricShardColumns)
        val queriesSingleShard = hasSingleShardKeyMatch(nonMetricShardKeyFilters)
        if (!isRoot && (hasShardKeyEqualsOnly(aggregate, dataset.options.nonMetricShardColumns) || queriesSingleShard))
          None  // Do not materialize yet, perhaps a parent node can be materialized on the same partition
        else {
          // The aggregate needs to spawn different partitions or this is the root node,
          // we therefore will materialize now
          if (queriesSingleShard) {
            // guaranteed to generate only one ExecPlan
              Some(generateExecWithoutRegex(aggregate, nonMetricShardKeyFilters.head, qContext).head)
          } else {
              // We will query multiple partitions and aggregate locally. The child plans will not have
              // aggregate presenter

              val execPlans = generateExecWithoutRegex(aggregate, nonMetricShardKeyFilters.head, qContext)
              val reducer = MultiPartitionReduceAggregateExec(qContext, inProcessPlanDispatcher,
                execPlans.sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec]), aggregate.operator, aggregate.params)
              val promQlQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
              reducer.addRangeVectorTransformer(AggregatePresenter(aggregate.operator, aggregate.params,
                RangeParams(promQlQueryParams.startSecs, promQlQueryParams.stepSecs, promQlQueryParams.endSecs)))

            Some(reducer)
          }
        }
      case Some(plan) =>
        // The child plan is already materialized, we have to simply do this aggregation in process
        val reducer = LocalPartitionReduceAggregateExec(qContext, inProcessPlanDispatcher,
          (plan :: Nil).sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec]),
          aggregate.operator, aggregate.params)
        val promQlQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
        reducer.addRangeVectorTransformer(AggregatePresenter(aggregate.operator, aggregate.params,
          RangeParams(promQlQueryParams.startSecs, promQlQueryParams.stepSecs, promQlQueryParams.endSecs)))

        Some(reducer)
    }
  }

  /**
   * Materialize the Binary Join if necessary else let one of the parent node if one exists materialize the plan.
   * The necessity is driven by two factors, a) are LHS & RHS combines access data across multiple partitions?
   * b) If any of LHS or RHS already materialized. c) Is this a root node?
   * In case a) None of the LHS and RHS individually access data across multiple partitions but as for this Binary Join
   * is concerned, we have to materialize both LHS and RHS and perform the join in process
   * In case b) it shows some node a lower level in the tree is already accessing data across multiple partitions and
   * we have no choice but to materialize the BinaryJoin operation in memory
   * In Case c) None of LHS/RHS accesses the data across partitions but since this is the root node, we need to
   * materialize the tree
   *
   * @param qContext The QueryContext instance
   * @param bin The LogicalPlan for BinaryJoin
   * @param isRoot boolean flag indicating if the node is the root of the query plan
   * @return An Option None if it is not necessary to materialize this BinaryJoin and let the parent node materialize
   *         the query or Some(ExecPlan) if the Binary node or any of is child is already materialized
   *
   */
  private def materializeBinaryJoin(qContext: QueryContext, bin: BinaryJoin, isRoot: Boolean) = {
    val lhsQueryContext = qContext.copy(origQueryParams =
      qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
        copy(promQl = LogicalPlanParser.convertToQuery(bin.lhs)),
        plannerParams = qContext.plannerParams.copy(skipAggregatePresent = true))
    val rhsQueryContext = qContext.copy(origQueryParams =
      qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
        copy(promQl = LogicalPlanParser.convertToQuery(bin.rhs)),
        plannerParams = qContext.plannerParams.copy(skipAggregatePresent = true))


    val lhsPlan = walkLogicalPlanTreeInternal(bin.lhs, lhsQueryContext, isRoot = false)
    val rhsPlan = walkLogicalPlanTreeInternal(bin.rhs, rhsQueryContext, isRoot = false)
    (lhsPlan, rhsPlan) match {
      case (None, None) | (None, Some(_: DistConcatExec)) | (Some(_: DistConcatExec), None)
           | (Some(_: DistConcatExec), Some(_: DistConcatExec)) =>
        // Case 1: No child is materialized or is a DistConcatExec,
        // however if lhs and rhs both belong to same partition, we will
        // pass the let the parent of this join be materialized by one planner.
        val nonMetricShardKeyFilters = getNonMetricShardKeyFilters(bin.lhs, dataset.options.nonMetricShardColumns) ++
                                        getNonMetricShardKeyFilters(bin.rhs, dataset.options.nonMetricShardColumns)
        val queriesSingleShard = hasSingleShardKeyMatch(nonMetricShardKeyFilters)
        // For Binary Joins, If LHS and RHS has Equals only that doesn't mean we can materialize the entire join
        // using the underlying planner, eg  metric1{_ns="ns1", _ws_="ws"} + metric1{_ns="ns2", _ws_="ws"}
        if (isRoot &&  queriesSingleShard) {
            // This is the root node and all the data is in the same partition, use the queryPlanner to materialize
            // entire tree
            Some(generateExecWithoutRegex(bin,
              getNonMetricShardKeyFilters(bin, dataset.options.nonMetricShardColumns).head,
              qContext).head)
        } else if (!isRoot && queriesSingleShard)
            // Not a root but all data in same partition, do not materialize yet
            // perhaps a parent node can be materialized on the same partition
            None
        else
           // Perform the join in process
            performInProcessBinaryJoin(qContext, bin, lhsQueryContext, rhsQueryContext, lhsPlan, rhsPlan)

      case _ =>
        // Case 2, At least Left or right child is already materialized.
        // In this case, we cannot materialize this binary join, nor any of if parent using the query planner.
        // We therefore have to materialize the other child of this join and do the join in process
        performInProcessBinaryJoin(qContext, bin, lhsQueryContext, rhsQueryContext, lhsPlan, rhsPlan)
    }
  }



  private def performInProcessBinaryJoin(qContext: QueryContext, bin: BinaryJoin,
                                        lhsQueryContext: QueryContext, rhsQueryContext: QueryContext,
                                        lhsPlan: Option[ExecPlan], rhsPlan: Option[ExecPlan]) = {
    val lhsMaterialized = lhsPlan match {
      case None => this.materialize(bin.lhs, lhsQueryContext)
      case Some(plan) => plan
    }

    val rhsMaterialized = rhsPlan match {
      case None => this.materialize(bin.rhs, rhsQueryContext)
      case Some(plan) => plan
    }
    val onKeysReal = ExtraOnByKeysUtil.getRealOnLabels(bin, queryConfig.addExtraOnByKeysTimeRanges)
    bin.operator match {
      case _: SetOperator =>
        Some(SetOperatorExec(qContext, inProcessPlanDispatcher, Seq(lhsMaterialized),
          Seq(rhsMaterialized), bin.operator,
          LogicalPlanUtils.renameLabels(onKeysReal, datasetMetricColumn),
          LogicalPlanUtils.renameLabels(bin.ignoring, datasetMetricColumn), datasetMetricColumn))
      case _ =>
        Some(BinaryJoinExec(qContext, inProcessPlanDispatcher, Seq(lhsMaterialized),
          Seq(rhsMaterialized), bin.operator,
          bin.cardinality, LogicalPlanUtils.renameLabels(onKeysReal, datasetMetricColumn),
          LogicalPlanUtils.renameLabels(bin.ignoring, datasetMetricColumn),
          LogicalPlanUtils.renameLabels(bin.include, datasetMetricColumn), datasetMetricColumn))
    }
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
        execPlans.sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec]))
    }
    PlanResult(Seq(exec))
  }
}
