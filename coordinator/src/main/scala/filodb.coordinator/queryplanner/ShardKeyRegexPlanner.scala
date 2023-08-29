package filodb.coordinator.queryplanner

import scala.collection.{mutable, Seq}

import filodb.core.{StaticTargetSchemaProvider, TargetSchemaProvider}
import filodb.core.metadata.{Dataset, DatasetOptions, Schemas}
import filodb.core.query.{ColumnFilter, Filter, PromQlQueryParams, QueryConfig, QueryContext, RangeParams}
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
                           partitionLocationProvider: PartitionLocationProvider,
                           config: QueryConfig,
                           _targetSchemaProvider: TargetSchemaProvider = StaticTargetSchemaProvider())
  extends PartitionLocationPlanner(dataset, partitionLocationProvider) {

  override def queryConfig: QueryConfig = config
  override val schemas: Schemas = Schemas(dataset.schema)
  override val dsOptions: DatasetOptions = schemas.part.options
  private val datasetMetricColumn = dataset.options.metricColumn

  private val nonMetricShardKeyColToIndex = dataset.options.shardKeyColumns
    .filterNot(_ == dataset.options.metricColumn)
    .zipWithIndex
    .toMap

  private def targetSchemaProvider(qContext: QueryContext): TargetSchemaProvider = {
    qContext.plannerParams.targetSchemaProviderOverride.getOrElse(_targetSchemaProvider)
  }

  private def getShardKeys(plan: LogicalPlan): Seq[Seq[ColumnFilter]] = {
    LogicalPlan.getNonMetricShardKeyFilters(plan, dataset.options.shardKeyColumns)
      .flatMap(shardKeyMatcher(_))
  }

  /**
   * Returns a set of shard-key sets iff a plan can be pushed-down to each.
   * See [[LogicalPlanUtils.getPushdownKeys]] for more info.
   */
  private def getPushdownKeys(qContext: QueryContext,
                              plan: LogicalPlan): Option[Set[Set[ColumnFilter]]] = {
    val getRawShardKeys = (rs: RawSeries) => getShardKeys(rs).map(_.toSet).toSet
    LogicalPlanUtils.getPushdownKeys(
      plan,
      targetSchemaProvider(qContext),
      dataset.options.nonMetricShardColumns,
      getRawShardKeys,
      rs => getShardKeys(rs))
  }

  /**
   * Attempts a pushdown optimization.
   * Returns an occupied Optional iff the optimization was successful.
   */
  private def attemptPushdown(logicalPlan: LogicalPlan, qContext: QueryContext): Option[PlanResult] = {
    val pushdownKeys = getPushdownKeys(qContext, logicalPlan)
    if (pushdownKeys.isDefined) {
      val plans = generateExec(logicalPlan, pushdownKeys.get.map(_.toSeq).toSeq, qContext)
        .sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec])
      Some(PlanResult(plans))
    } else None
  }

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
    } else {
      val result = walkLogicalPlanTree(logicalPlan, qContext)
      if (result.plans.size > 1) {
        val dispatcher = PlannerUtil.pickDispatcher(result.plans)
        MultiPartitionDistConcatExec(qContext, dispatcher, result.plans)
      } else result.plans.head
    }
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
    // Materialize for each key with the inner planner if:
    //   - all plan data lies on one partition
    //   - all RawSeries filters are identical
    val qParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val filterGroups = LogicalPlan.getRawSeriesFilters(logicalPlan)
      .map(_.filter(cf => dataset.options.nonMetricShardColumns.contains(cf.column)))
    val headFilters = filterGroups.headOption.map(_.toSet)
    // Note: unchecked .get is OK here since it will only be called for each tail element.
    val sameFilters = filterGroups.tail.forall(_.toSet == headFilters.get)
    val partitions = getShardKeys(logicalPlan)
      .flatMap(filters => getPartitions(logicalPlan.replaceFilters(filters), qParams))
    if (partitions.isEmpty) {
      return PlanResult(Seq(queryPlanner.materialize(logicalPlan, qContext)))
    } else if (sameFilters && isSinglePartition(partitions)) {
      val plans = generateExec(logicalPlan, getShardKeys(logicalPlan), qContext)
      return PlanResult(plans)
    }
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
    val shardKeyMatches = shardKeyMatcher(nonMetricShardKeyFilters)
    generateExec(logicalPlan, shardKeyMatches, qContext)
  }

  /**
   * Updates the time params and query of the the argument PromQlQueryParams according to the argument LogicalPlan.
   */
  private def updateQueryParams(logicalPlan: LogicalPlan,
                                queryParams: PromQlQueryParams): PromQlQueryParams = {
    logicalPlan match {
      case tls: TopLevelSubquery => {
        val instantTime = queryParams.startSecs
        queryParams.copy(
          promQl = LogicalPlanParser.convertToQuery(logicalPlan),
          startSecs = instantTime,
          endSecs = instantTime
        )
      }
      case psp: PeriodicSeriesPlan => {
        queryParams.copy(
          promQl = LogicalPlanParser.convertToQuery(logicalPlan),
          startSecs = psp.startMs / 1000,
          endSecs = psp.endMs / 1000,
          stepSecs = psp.stepMs / 1000
        )
      }
      case _ => queryParams.copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan))
    }
  }

  // scalastyle:off method.length
  /**
   * Group shard keys by partition and generate an ExecPlan for each.
   * Plans will match shard-keys by pipe-separated regex filters. For example, Suppose the following keys are provided:
   *     keys = {{a=1, b=1}, {a=1, b=2}, {a=2, b=3}}
   *   These will be grouped according to the partitions they occupy:
   *     part1 -> {{a=1, b=1}, {a=1, b=2}}
   *     part2 -> {{a=2, b=3}}
   *   Then a plan will be generated for each partition:
   *     plan1 -> {a=1, b=~"1|2"}
   *     plan2 -> {a=2, b=3}
   * Additional plans are materialized per partition when multiple regex filters would otherwise be required.
   *   This prevents scenarios such as:
   *     keys = {{a=1, b=2}, {a=3, b=4}, {a=5, b=6}}
   *   These will be grouped according to the partitions they occupy:
   *     part1 -> {{a=1, b=2}, {a=3, b=4}}
   *     part2 -> {{a=5, b=6}}
   *   Then a plan will be generated for each partition:
   *     plan1 -> {a=~"1|3", b=~"2|4"}
   *     plan2 -> {a=5, b=6}
   *   This might erroneously read {a=1, b=4}, which was not included in the original key set.
   */
  private def generateExecForEachPartition(logicalPlan: LogicalPlan,
                                           keys: Seq[Seq[ColumnFilter]],
                                           qContext: QueryContext): Seq[ExecPlan] = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]

    // maps individual partitions to the set of shard-keys they contain.
    val partitionsToKeys = new mutable.HashMap[String, mutable.Buffer[Seq[ColumnFilter]]]()
    keys.foreach { key =>
      val newLogicalPlan = logicalPlan.replaceFilters(key)
      // Querycontext should just have the part of query which has regex
      // For example for exp(sum(test{_ws_ = "demo", _ns_ =~ "App.*"})), sub queries should be
      // sum(test{_ws_ = "demo", _ns_ = "App-1"}), sum(test{_ws_ = "demo", _ns_ = "App-2"}) etc
      val newQueryParams = updateQueryParams(newLogicalPlan, queryParams)
      getPartitions(newLogicalPlan, newQueryParams)
        .map(_.partitionName)
        .distinct
        .foreach(part => partitionsToKeys.getOrElseUpdate(part, new mutable.ArrayBuffer).append(key))
    }

    // Sort each key into the same order as nonMetricShardKeys, then group keys with the same prefix.
    // A plan will be created for each group; this prevents the scenario mentioned in the javadoc.
    val partitionToKeyGroups = partitionsToKeys.map{ case (partition, keys) =>
      val sortedPrefixes = keys.map(key => key.sortBy(filter => nonMetricShardKeyColToIndex(filter.column)))
        .groupBy(_.dropRight(1))
        .values
      (partition, sortedPrefixes)
    }

    // Skip the aggregate presentation if there are more than one plans to materialize.
    val skipAggregatePresentValue = partitionToKeyGroups.size > 1 ||
                                    partitionToKeyGroups.values.headOption.map(_.size).getOrElse(0) > 1

    // Create one plan per key group (i.e. one per partition,prefix pair).
    partitionToKeyGroups.flatMap{ case (partition, keyGroups) =>
      keyGroups.map{ keys =>
        // Create a map of key->values, then create a ColumnFilter for each key.
        val keyToValues = new mutable.HashMap[String, mutable.Set[String]]()
        keys.flatten.foreach { filter =>
          // Find the key's list of values in the map (or create it), then add the filter's values.
          val values = keyToValues.getOrElseUpdate(filter.column, new mutable.HashSet[String]())
          filter.filter.valuesStrings.map(_.toString).foreach(values.add)
        }
        val newFilters = keyToValues.map { case (key, values) =>
          val filter = if (values.size == 1) {
            Filter.Equals(values.head)
          } else {
            // Concatenate values with "|" for multi-valued keys.
            Filter.EqualsRegex(values.toSeq.sorted.mkString("|"))
          }
          ColumnFilter(key, filter)
        }.toSeq
        // Update the LogicalPlan with the new partition-specific filters, then materialize.
        val newLogicalPlan = logicalPlan.replaceFilters(newFilters)
        val newQueryParams = updateQueryParams(newLogicalPlan, queryParams)
        val newQueryContext = qContext.copy(origQueryParams = newQueryParams, plannerParams = qContext.plannerParams.
          copy(skipAggregatePresent = skipAggregatePresentValue))
        queryPlanner.materialize(newLogicalPlan, newQueryContext)
      }
    }.toSeq
  }
  // scalastyle:on method.length

  // FIXME: This will eventually be replaced with generateExecForEachPartition.
  @Deprecated
  private def generateExecForEachKey(logicalPlan: LogicalPlan,
                                     keys: Seq[Seq[ColumnFilter]],
                                     qContext: QueryContext): Seq[ExecPlan] = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val skipAggregatePresentValue = keys.length > 1
    keys.map { result =>
      val newLogicalPlan = logicalPlan.replaceFilters(result)
      // Querycontext should just have the part of query which has regex
      // For example for exp(sum(test{_ws_ = "demo", _ns_ =~ "App.*"})), sub queries should be
      // sum(test{_ws_ = "demo", _ns_ = "App-1"}), sum(test{_ws_ = "demo", _ns_ = "App-2"}) etc
      val newQueryParams = logicalPlan match {
        case tls: TopLevelSubquery => {
          val instantTime = queryParams.startSecs
          queryParams.copy(
            promQl = LogicalPlanParser.convertToQuery(newLogicalPlan),
            startSecs = instantTime,
            endSecs = instantTime
          )
        }
        case psp: PeriodicSeriesPlan => {
          queryParams.copy(
            promQl = LogicalPlanParser.convertToQuery(newLogicalPlan),
            startSecs = psp.startMs / 1000,
            endSecs = psp.endMs / 1000,
            stepSecs = psp.stepMs / 1000
          )
        }
        case _ => queryParams.copy(promQl = LogicalPlanParser.convertToQuery(newLogicalPlan))
      }
      val newQueryContext = qContext.copy(origQueryParams = newQueryParams, plannerParams = qContext.plannerParams.
        copy(skipAggregatePresent = skipAggregatePresentValue))
      queryPlanner.materialize(newLogicalPlan, newQueryContext)
    }
  }

  private def generateExec(logicalPlan: LogicalPlan,
                           keys: Seq[Seq[ColumnFilter]],
                           qContext: QueryContext): Seq[ExecPlan] =
    if (qContext.plannerParams.reduceShardKeyRegexFanout) {
      generateExecForEachPartition(logicalPlan, keys, qContext)
    } else {
      generateExecForEachKey(logicalPlan, keys, qContext)
    }

  /**
   * For binary join queries like test1{_ws_ = "demo", _ns_ =~ "App.*"} + test2{_ws_ = "demo", _ns_ =~ "App.*"})
   * LHS and RHS could be across multiple partitions
   */
  private def materializeBinaryJoin(logicalPlan: BinaryJoin, qContext: QueryContext): PlanResult = {
    val pushdownKeys = attemptPushdown(logicalPlan, qContext)
    if (pushdownKeys.isDefined) {
      return pushdownKeys.get
    }

    optimizeOrVectorDouble(qContext, logicalPlan).getOrElse {
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
    val pushdownKeys = attemptPushdown(aggregate, queryContext)
    if (pushdownKeys.isDefined) {
      return pushdownKeys.get
    }

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
