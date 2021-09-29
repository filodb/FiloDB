package filodb.coordinator.queryplanner

import filodb.core.metadata.Dataset
import filodb.core.query.{ColumnFilter, PromQlQueryParams, QueryConfig, QueryContext}
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

class ShardKeyRegexPlanner(val dataset: Dataset,
                           queryPlanner: QueryPlanner,
                           shardKeyMatcher: Seq[ColumnFilter] => Seq[Seq[ColumnFilter]],
                           config: QueryConfig)
  extends QueryPlanner with PlannerHelper {

  override def queryConfig: QueryConfig = config
  val datasetMetricColumn = dataset.options.metricColumn

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
      case lp: Aggregate                   => materializeAggregate(qContext, lp)
      case lp: BinaryJoin                  => materializeBinaryJoin(qContext, lp)
      case lp: LabelValues                 => PlanResult(Seq(queryPlanner.materialize(lp, qContext)))
      case lp: LabelNames                  => PlanResult(Seq(queryPlanner.materialize(lp, qContext)))
      case lp: SeriesKeysByFilters         => PlanResult(Seq(queryPlanner.materialize(lp, qContext)))
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
}
