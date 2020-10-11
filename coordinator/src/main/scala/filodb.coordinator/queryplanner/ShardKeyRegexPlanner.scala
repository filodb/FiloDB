package filodb.coordinator.queryplanner

import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.query.{ColumnFilter, PromQlQueryParams, QueryContext}
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
                           shardKeyMatcher: Seq[ColumnFilter] => Seq[Seq[ColumnFilter]])
  extends QueryPlanner with PlannerMaterializer {

  override val schemas = Schemas(dataset.schema)
  /**
    * Converts a logical plan to execution plan.
    *
    * @param logicalPlan Logical plan after converting PromQL -> AST -> LogicalPlan
    * @param qContext    holder for additional query parameters
    * @return materialized Execution Plan which can be dispatched
    */
  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    if (LogicalPlan.hasShardKeyEqualsOnly(logicalPlan, dataset.options.nonMetricShardColumns))
      queryPlanner.materialize(logicalPlan, qContext)
    else walkLogicalPlanTree(logicalPlan, qContext).plans.head
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
      case _                               => materializeOthers(logicalPlan, qContext)
    }
  }

  private def generateExecWithoutRegex(logicalPlan: LogicalPlan, nonMetricShardKeyFilters: Seq[ColumnFilter],
                                       qContext: QueryContext): Seq[ExecPlan] = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    shardKeyMatcher(nonMetricShardKeyFilters).map { result =>
        val newLogicalPlan = logicalPlan.replaceFilters(result)
        // Querycontext should just have the part of query which has regex
        // For example for exp(sum(test{_ws_ = "demo", _ns_ =~ "App.*"})), sub queries should be
        // sum(test{_ws_ = "demo", _ns_ = "App-1"}), sum(test{_ws_ = "demo", _ns_ = "App-2"}) etc
        val newQueryParams = queryParams.copy(promQl = LogicalPlanParser.convertToQuery(newLogicalPlan))
        val newQueryContext = qContext.copy(origQueryParams = newQueryParams)
        queryPlanner.materialize(logicalPlan.replaceFilters(result), newQueryContext)
      }
  }

  /**
    * For binary join queries like test1{_ws_ = "demo", _ns_ =~ "App.*"} + test2{_ws_ = "demo", _ns_ =~ "App.*"})
    */
  private def materializeBinaryJoin(binaryJoin: BinaryJoin, qContext: QueryContext): PlanResult = {
   if (LogicalPlan.hasShardKeyEqualsOnly(binaryJoin, dataset.options.nonMetricShardColumns))
     PlanResult(Seq(queryPlanner.materialize(binaryJoin, qContext)))
   else throw new UnsupportedOperationException("Regex not supported for Binary Join")
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
      val reducer = MultiPartitionReduceAggregateExec(queryContext, InProcessPlanDispatcher,
        execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlRemoteExec]), aggregate.operator, aggregate.params)
      reducer.addRangeVectorTransformer(AggregatePresenter(aggregate.operator, aggregate.params))
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
      if (execPlans.size == 1) execPlans.head else MultiPartitionDistConcatExec(queryContext, InProcessPlanDispatcher,
        execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlRemoteExec]))
    }
    PlanResult(Seq(exec))
  }
}
