package filodb.coordinator.queryplanner

import filodb.core.metadata.Dataset
import filodb.core.query.{ColumnFilter, PromQlQueryParams, QueryContext}
import filodb.core.query.Filter.{EqualsRegex, NotEqualsRegex}
import filodb.query.{Aggregate, BinaryJoin, LogicalPlan}
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
  * @param shardKeyMatcherFn used to get values for regex shard keys. Each inner sequence corresponds to matching regex
  * value. For example: Seq(ColumnFilter(ws, Equals(demo)), ColumnFilter(ns, EqualsRegex(App.*)) with following query
  * metric1{ws="demo", ns=~"App.*} returns,
  * Seq(ShardKeyMatcherResult(Seq(ColumnFilter(ws, Equals(demo)),
  *   ColumnFilter(ns, Equals(App1)), metric1{ws="demo", ns="App1"}),
  *     ShardKeyMatcherResult(Seq(ColumnFilter(ws, Equals(demo)),
  *       ColumnFilter(ns, Equals(App2)), metric1{ws="demo", ns="App2"}))
  */
class ShardKeyRegexPlanner(dataset: Dataset,
                           queryPlanner: QueryPlanner,
                           shardKeyMatcherFn: ShardKeyMatcher => Seq[ShardKeyMatcher])
  extends QueryPlanner {

  /**
    * Converts a logical plan to execution plan.
    *
    * @param logicalPlan Logical plan after converting PromQL -> AST -> LogicalPlan
    * @param qContext    holder for additional query parameters
    * @return materialized Execution Plan which can be dispatched
    */
  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    logicalPlan match {
           case a: Aggregate  => materializeAggregate(a, qContext)
           case b: BinaryJoin => materializeBinaryJoin(b, qContext)
           case _             => materializeOthers(logicalPlan, qContext)
    }
  }

  private def getNonMetricShardKeyFilters(logicalPlan: LogicalPlan): Seq[Seq[ColumnFilter]] =
    LogicalPlan.getRawSeriesFilters(logicalPlan)
      .map { s => s.filter(f => dataset.options.nonMetricShardColumns.contains(f.column))}

  private def generateExec(logicalPlan: LogicalPlan, nonMetricShardKeyFilters: Seq[ColumnFilter],
                           qContext: QueryContext): Seq[ExecPlan] = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    shardKeyMatcherFn(ShardKeyMatcher(nonMetricShardKeyFilters, queryParams.promQl)).map { result =>
        val newQueryParams = queryParams.copy(promQl = result.query)
        val newQueryContext = qContext.copy(origQueryParams = newQueryParams)
        queryPlanner.materialize(logicalPlan.replaceFilters(result.columnFilters), newQueryContext)
      }
  }

  private def materializeBinaryJoin(binaryJoin: BinaryJoin, qContext: QueryContext): ExecPlan = {
   if (getNonMetricShardKeyFilters(binaryJoin).forall(_.forall(f => !f.filter.isInstanceOf[EqualsRegex] &&
     !f.filter.isInstanceOf[NotEqualsRegex]))) queryPlanner.materialize(binaryJoin, qContext)
   else throw new UnsupportedOperationException("Regex not supported for Binary Join")
  }

  private def materializeAggregate(aggregate: Aggregate, queryContext: QueryContext): ExecPlan = {
    val execPlans = generateExec(aggregate, getNonMetricShardKeyFilters(aggregate).head, queryContext)
    if (execPlans.size == 1) execPlans.head
    else {
      val reducer = MultiPartitionReduceAggregateExec(queryContext, InProcessPlanDispatcher,
        execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlRemoteExec]), aggregate.operator, aggregate.params)
      reducer.addRangeVectorTransformer(AggregatePresenter(aggregate.operator, aggregate.params))
      reducer
    }
  }

  private def materializeOthers(logicalPlan: LogicalPlan, queryContext: QueryContext): ExecPlan = {
    val nonMetricShardKeyFilters = getNonMetricShardKeyFilters(logicalPlan)
    // For queries which don't have RawSeries filters like metadata and fixed scalar queries
    if (nonMetricShardKeyFilters.head.isEmpty) queryPlanner.materialize(logicalPlan, queryContext)
    else {
      val execPlans = generateExec(logicalPlan, nonMetricShardKeyFilters.head, queryContext)
      if (execPlans.size == 1) execPlans.head else MultiPartitionDistConcatExec(queryContext, InProcessPlanDispatcher,
        execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlRemoteExec]))
    }
  }
}
