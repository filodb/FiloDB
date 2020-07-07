package filodb.coordinator.queryplanner

import filodb.core.metadata.Dataset
import filodb.core.query.{ColumnFilter, QueryContext}
import filodb.core.query.Filter.{EqualsRegex, NotEqualsRegex}
import filodb.query.{Aggregate, BinaryJoin, LogicalPlan}
import filodb.query.exec.{DistConcatExec, ExecPlan, InProcessPlanDispatcher, ReduceAggregateExec}

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
                           shardKeyMatcher: Seq[ColumnFilter] => Seq[Seq[ColumnFilter]]) extends QueryPlanner {
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

  private def getNonMetricShardKeyFilters(logicalPlan: LogicalPlan) = LogicalPlan.
    getRawSeriesFilters(logicalPlan).map { s => s.filter(f => dataset.options.nonMetricShardColumns.contains(f.column))}

  private def generateExec(logicalPlan: LogicalPlan, nonMetricShardKeyFilters: Seq[ColumnFilter],
                           qContext: QueryContext) = shardKeyMatcher(nonMetricShardKeyFilters).map(f =>
                           queryPlanner.materialize(logicalPlan.replaceFilters(f), qContext))

  private def materializeBinaryJoin(binaryJoin: BinaryJoin, qContext: QueryContext): ExecPlan = {
   if (getNonMetricShardKeyFilters(binaryJoin).forall(_.forall(f => !f.filter.isInstanceOf[EqualsRegex] &&
     !f.filter.isInstanceOf[NotEqualsRegex]))) queryPlanner.materialize(binaryJoin, qContext)
   else throw new UnsupportedOperationException("Regex not supported for Binary Join")
  }

  private def materializeAggregate(aggregate: Aggregate, queryContext: QueryContext): ExecPlan = {
    val execPlans = generateExec(aggregate, getNonMetricShardKeyFilters(aggregate).head, queryContext)
    if (execPlans.size == 1) execPlans.head
    else ReduceAggregateExec(queryContext, InProcessPlanDispatcher, execPlans, aggregate.operator, aggregate.params)
  }

  private def materializeOthers(logicalPlan: LogicalPlan, queryContext: QueryContext): ExecPlan = {
    val nonMetricShardKeyFilters = getNonMetricShardKeyFilters(logicalPlan)
    // For queries which don't have RawSeries filters like metadata and fixed scalar queries
    if (nonMetricShardKeyFilters.head.isEmpty) queryPlanner.materialize(logicalPlan, queryContext)
    else {
      val execPlans = generateExec(logicalPlan, nonMetricShardKeyFilters.head, queryContext)
      if (execPlans.size == 1) execPlans.head else DistConcatExec(queryContext, InProcessPlanDispatcher, execPlans)
    }
  }
}
