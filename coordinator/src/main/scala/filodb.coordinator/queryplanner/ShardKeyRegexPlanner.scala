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
      val plans = generateExec(logicalPlan, pushdownKeys.get.map(_.toSeq).toSeq, qContext, pushdownPlan = true)
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
    // Materialize with the inner planner if both of:
    //   - all plan data lies on one partition
    //   - all RawSeries filters are identical
    val qParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val shardKeyFilterGroups = LogicalPlan.getRawSeriesFilters(logicalPlan)
      .map(_.filter(cf => dataset.options.nonMetricShardColumns.contains(cf.column)))
    // Unchecked .get is OK here since it will only be called for each tail element.
    val hasSameShardKeyFilters = shardKeyFilterGroups.tail.forall(_.toSet == shardKeyFilterGroups.head.toSet)
    val shardKeys = getShardKeys(logicalPlan)
    val partitions = shardKeys
      .flatMap(filters => getPartitions(logicalPlan.replaceFilters(filters), qParams))
      .flatMap(_.proportionMap.keys)
      .distinct
    // NOTE: don't use partitions.size < 2. When partitions == 0, generateExec will not
    //   materialize any plans because there are no partitions against which it should materialize.
    //   That is a problem for e.g. scalars or absent().
    if (hasSameShardKeyFilters && partitions.size == 1) {
      val plans = generateExec(logicalPlan, shardKeys, qContext)
      // If !=1 plans were produced, additional high-level coordination may be needed
      //   (e.g. joins / aggregations). In that case, proceed through the logic below.
      if (plans.size == 1) {
        return PlanResult(plans)
      }
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

  // scalastyle:off method.length
  /**
   * Group shard keys by partition, then generate an ExecPlan for each group.
   * For example, suppose the following shard keys are provided (by either
   *   Equals filters or pipe-concatenated EqualsRegex):
   *       keys = {{a=1, b=1}, {a=1, b=2}, {a=2, b=3}}
   *   These will be grouped according to the partitions they occupy. Suppose the
   *     value of `a` indicates the host partition:
   *       part1 -> {{a=1, b=1}, {a=1, b=2}}
   *       part2 -> {{a=2, b=3}}
   *   Then a plan will be generated for each partition:
   *       plan1 -> {a=1, b=~"1|2"}
   *       plan2 -> {a=2, b=3}
   * Additional plans are materialized per partition when multiple regex filters would otherwise be required.
   *   This prevents scenarios such as:
   *       keys = {{a=1, b=1, c=1}, {a=1, b=2, c=2}, {a=2, b=1, c=1}}
   *   These will be grouped according to the partitions they occupy (again, suppose `a` determines the partition):
   *       part1 -> {{a=1, b=1, c=1}, {a=1, b=2, c=1}, {a=1, b=2, c=2}}
   *       part2 -> {{a=2, b=1, c=2}}
   *   Then a plan will be generated for each partition:
   *       plan1 -> {a=1, b=~"1|2", c=~"1|2"}
   *       plan2 -> {a=2, b=1, c=1}
   *   This might erroneously read data for {a=1, b=1, c=2}, which was not included in the original key set.
   *   To prevent this, all keys on the same partition are again grouped by "prefix" -- all columns except
   *   the last in a dataset's non-metric shard key-- and plans are materialized for each group.
   *   For example, given the above keys, the result plans will instead be:
   *       plan1a -> {{a=1, b=1, c=1}}}
   *       plan2a -> {{a=1, b=2, c=~"1|2"}}
   *       plan2 -> {{a=2, b=1, c=2}}
   */
  private def generateExecForEachPartition(logicalPlan: LogicalPlan,
                                           keys: Seq[Seq[ColumnFilter]],
                                           qContext: QueryContext,
                                           pushdownPlan: Boolean = false): Seq[ExecPlan] = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]

    if (keys.isEmpty) {
      // most likely a scalar -- just materialize for the local partition.
      return Seq(queryPlanner.materialize(logicalPlan, qContext))
    }

    // Map partitions to the set of *non*-partition-split keys they house.
    val partitionsToNonSplitKeys = new mutable.HashMap[String, mutable.Buffer[Seq[ColumnFilter]]]()
    // Set of all *partition-split* key groups. These will be materialized
    //   individually to take advantage of exiting split-key stitching infrastructure.
    val partitionSplitKeys = new mutable.ArrayBuffer[Seq[ColumnFilter]]
    keys.foreach { key =>
      val newLogicalPlan = logicalPlan.replaceFilters(key)
      val newQueryParams = queryParams.copy(promQl = LogicalPlanParser.convertToQuery(newLogicalPlan))
      val partitions = getPartitions(newLogicalPlan, newQueryParams)
        .flatMap(_.proportionMap.keys)
        .distinct
      if (partitions.size > 1) {
        partitionSplitKeys.append(key)
      } else {
        partitions.foreach(part => partitionsToNonSplitKeys.getOrElseUpdate(part, new mutable.ArrayBuffer).append(key))
      }
    }

    // Sort each key into the same order as nonMetricShardKeys, then group keys with the same prefix.
    // A plan will be created for each group; this prevents the scenario mentioned in the javadoc.
    // NOTE: this solution is not optimal, but it is simple and guarantees that the regex filters
    //   in the result plans don't span unintentional shard keys.

    // Map column -> index, then use this to sort each shard key.
    // NOTE: this means the order in which shard-key columns are defined will affect query results.
    //   Columns with higher cardinalities should be defined last.
    val shardKeyCols = dataset.options.shardKeyColumns.filterNot(_ == dataset.options.metricColumn)
    val nonMetricShardKeyColToIndex = shardKeyCols.zipWithIndex.toMap
    val partitionToKeyGroups = partitionsToNonSplitKeys.map{ case (partition, keys) =>
      val prefixGroups = keys
        .map(key => key.sortBy(filter => nonMetricShardKeyColToIndex.get(filter.column)))
        .groupBy(_.dropRight(1))
        .values
        .flatMap(group => group.grouped(qContext.plannerParams.maxShardKeyRegexFanoutBatchSize))
      (partition, prefixGroups)
    }

    // Skip lower-level aggregate presentation if there is more than one plan to materialize.
    // In that case, the presentation step will be applied by this planner.
    val skipAggregatePresentValue = !pushdownPlan &&
                                      partitionToKeyGroups.values.map(_.size).sum + partitionSplitKeys.size > 1

    // Materialize a plan for each group of non-split of keys.
    // Each group will be encoded into a set of Equals/EqualsRegex filters, where regex filters
    //   contain only shard-key values concatenated by pipes.
    val nonSplitPlans = partitionToKeyGroups.flatMap{ case (partition, keyGroups) =>
      // NOTE: partition is intentionally unused; the inner planner will again determine which partitions own the data.
      keyGroups.map{ keys =>
        // Create a map of key->values, then create a ColumnFilter for each key.
        val keyToValues = keys.flatten
          .foldLeft(new mutable.HashMap[String, mutable.Set[String]]()) { case (acc, filter) =>
            val values = acc.getOrElseUpdate(filter.column, new mutable.HashSet[String]())
            filter.filter.valuesStrings.map(_.toString).foreach(values.add)
            acc
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
        // Update the LogicalPlan with the new single-partition filters, then materialize.
        val newLogicalPlan = logicalPlan.replaceFilters(newFilters)
        val newQueryParams = queryParams.copy(promQl = LogicalPlanParser.convertToQuery(newLogicalPlan))
        val newQueryContext = qContext.copy(
          origQueryParams = newQueryParams,
          plannerParams = qContext.plannerParams.copy(skipAggregatePresent = skipAggregatePresentValue)
        )
        queryPlanner.materialize(newLogicalPlan, newQueryContext)
      }
    }.toSeq

    // Just materialize each of these partition-split plans individually -- let the
    //   lower-level planners handle stitching.
    val splitPlans = partitionSplitKeys.map{ key =>
      val newLogicalPlan = logicalPlan.replaceFilters(key)
      val newQueryParams = queryParams.copy(promQl = LogicalPlanParser.convertToQuery(newLogicalPlan))
      val newQueryContext = qContext.copy(
        origQueryParams = newQueryParams,
        plannerParams = qContext.plannerParams.copy(skipAggregatePresent = skipAggregatePresentValue)
      )
      queryPlanner.materialize(newLogicalPlan, newQueryContext)
    }

    val res = nonSplitPlans ++ splitPlans
    if (res.nonEmpty) res else Seq(EmptyResultExec(qContext, dataset.ref, inProcessPlanDispatcher))
  }
  // scalastyle:on method.length

  // FIXME: This will eventually be replaced with generateExecForEachPartition.
  private def generateExecForEachKey(logicalPlan: LogicalPlan,
                                     keys: Seq[Seq[ColumnFilter]],
                                     qContext: QueryContext,
                                     pushdownPlan: Boolean = false): Seq[ExecPlan] = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val skipAggregatePresentValue = !pushdownPlan && keys.length > 1
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

  /**
   * Materializes the argument LogicalPlan.
   * This method should be preferred to {@link generateExecForEachPartition}
   *   or {@link generateExecForEachKey}; one of those will be called internally
   *   according to the PlannerParams attribute reduceShardKeyRegexFanout.
   */
  private def generateExec(logicalPlan: LogicalPlan,
                           keys: Seq[Seq[ColumnFilter]],
                           qContext: QueryContext,
                           pushdownPlan: Boolean = false): Seq[ExecPlan] =
    if (qContext.plannerParams.reduceShardKeyRegexFanout) {
      generateExecForEachPartition(logicalPlan, keys, qContext, pushdownPlan)
    } else {
      generateExecForEachKey(logicalPlan, keys, qContext, pushdownPlan)
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

      val lhsPlanRes = walkLogicalPlanTree(logicalPlan.lhs, lhsQueryContext)
      val rhsPlanRes = walkLogicalPlanTree(logicalPlan.rhs, rhsQueryContext)

      val execPlan = if (logicalPlan.operator.isInstanceOf[SetOperator])
        SetOperatorExec(qContext, inProcessPlanDispatcher, lhsPlanRes.plans, rhsPlanRes.plans, logicalPlan.operator,
          logicalPlan.on.map(LogicalPlanUtils.renameLabels(_, datasetMetricColumn)),
          LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn), datasetMetricColumn,
          rvRangeFromPlan(logicalPlan))
      else
        BinaryJoinExec(qContext, inProcessPlanDispatcher, lhsPlanRes.plans, rhsPlanRes.plans, logicalPlan.operator,
          logicalPlan.cardinality,
          logicalPlan.on.map(LogicalPlanUtils.renameLabels(_, datasetMetricColumn)),
          LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn),
          LogicalPlanUtils.renameLabels(logicalPlan.include, datasetMetricColumn), datasetMetricColumn,
          rvRangeFromPlan(logicalPlan))

      PlanResult(Seq(execPlan))
    }
  }

  /**
   * Retuns an occupied Option iff the plan can be pushed-down according to the set of labels.
   */
  private def getTschemaLabelsIfCanPushdown(lp: LogicalPlan, qContext: QueryContext): Option[Seq[String]] = {
    val canTschemaPushdown = getPushdownKeys(qContext, lp).isDefined
    if (canTschemaPushdown) {
        LogicalPlanUtils.sameRawSeriesTargetSchemaColumns(lp, targetSchemaProvider(qContext), getShardKeys)
    } else None
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

    val tschemaLabels = getTschemaLabelsIfCanPushdown(aggregate.vectors, queryContext)
    val canPushdown = canPushdownAggregate(aggregate, tschemaLabels, queryContext)
    val plan = if (!canPushdown) {
      val childPlanRes = walkLogicalPlanTree(aggregate.vectors, queryContext)
      // We are here because we cannot pushdown the aggregation, if that was multi-partition, the dispatcher will
      // be InProcessPlanDispatcher and adding the current aggregate using addAggregate will use the same dispatcher
      // If the underlying plan however is not multi partition, adding the aggregator using addAggregator will
      // use the same dispatcher
      addAggregator(aggregate, queryContext, childPlanRes)
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
    val execPlans = generateExecWithoutRegex(logicalPlan, nonMetricShardKeyFilters.head, queryContext)
    val exec = if (execPlans.size == 1) {
      execPlans.head
    } else if (execPlans.size > 1) {
      // TODO
      // here we essentially do not allow to optimize the physical plan for subqueries
      // as we concat the results with MultiPartitionDisConcatExec,
      // below queries:
      // max_over_time(rate(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }[10m])[1h:1m])
      // sum_over_time(foo{_ws_ = "demo", _ns_ =~ ".*Ns", instance = "Inst-1" }[5d:300s])
      // would have suboptimal performance. See subquery tests in PlannerHierarchySpec
      MultiPartitionDistConcatExec(
        queryContext, inProcessPlanDispatcher,
        execPlans.sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec])
      )
    } else {
      EmptyResultExec(queryContext, dataset.ref, inProcessPlanDispatcher)
    }
    PlanResult(Seq(exec))
  }
}
