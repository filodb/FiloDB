package filodb.coordinator.queryplanner

import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.query.{ColumnFilter, PromQlQueryParams, QueryContext}
import filodb.core.query.Filter.{EqualsRegex, NotEqualsRegex}
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
     walkLogicalPlanTree(logicalPlan, qContext).plans.head
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

  private def getNonMetricShardKeyFilters(logicalPlan: LogicalPlan): Seq[Seq[ColumnFilter]] =
    LogicalPlan.getRawSeriesFilters(logicalPlan)
      .map { s => s.filter(f => dataset.options.nonMetricShardColumns.contains(f.column))}

  private def generateExec(logicalPlan: LogicalPlan, nonMetricShardKeyFilters: Seq[ColumnFilter],
                           qContext: QueryContext): Seq[ExecPlan] = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    shardKeyMatcher(nonMetricShardKeyFilters).map { result =>
        val newLogicalPlan = logicalPlan.replaceFilters(result)
        val newQueryParams = queryParams.copy(promQl = LogicalPlanUtils.logicalPlanToQuery(newLogicalPlan))
        val newQueryContext = qContext.copy(origQueryParams = newQueryParams)
        queryPlanner.materialize(logicalPlan.replaceFilters(result), newQueryContext)
      }
  }

  private def materializeBinaryJoin(binaryJoin: BinaryJoin, qContext: QueryContext): PlanResult = {
   if (getNonMetricShardKeyFilters(binaryJoin).forall(_.forall(f => !f.filter.isInstanceOf[EqualsRegex] &&
     !f.filter.isInstanceOf[NotEqualsRegex]))) PlanResult(Seq(queryPlanner.materialize(binaryJoin, qContext)))
   else throw new UnsupportedOperationException("Regex not supported for Binary Join")
  }

  private def materializeAggregate(aggregate: Aggregate, queryContext: QueryContext): PlanResult = {
    val execPlans = generateExec(aggregate, getNonMetricShardKeyFilters(aggregate).head, queryContext)
    val exec = if (execPlans.size == 1) execPlans.head
    else {
      val reducer = MultiPartitionReduceAggregateExec(queryContext, InProcessPlanDispatcher,
        execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlRemoteExec]), aggregate.operator, aggregate.params)
      reducer.addRangeVectorTransformer(AggregatePresenter(aggregate.operator, aggregate.params))
      reducer
    }
    PlanResult(Seq(exec))
  }

  private def materializeOthers(logicalPlan: LogicalPlan, queryContext: QueryContext): PlanResult = {
    val nonMetricShardKeyFilters = getNonMetricShardKeyFilters(logicalPlan)
    // For queries which don't have RawSeries filters like metadata and fixed scalar queries
    val exec = if (nonMetricShardKeyFilters.head.isEmpty) queryPlanner.materialize(logicalPlan, queryContext)
    else {
      val execPlans = generateExec(logicalPlan, nonMetricShardKeyFilters.head, queryContext)
      if (execPlans.size == 1) execPlans.head else MultiPartitionDistConcatExec(queryContext, InProcessPlanDispatcher,
        execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlRemoteExec]))
    }
    PlanResult(Seq(exec))
  }
}
