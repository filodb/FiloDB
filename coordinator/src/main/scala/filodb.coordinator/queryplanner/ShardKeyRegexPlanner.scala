package filodb.coordinator.queryplanner

import filodb.core.metadata.Dataset
import filodb.core.query.{ColumnFilter, QueryContext}
import filodb.query.{Aggregate, BinaryJoin, LogicalPlan}
import filodb.query.exec.{DistConcatExec, ExecPlan, InProcessPlanDispatcher, ReduceAggregateExec}

case class ShardColumnValues(regexColumn: String, regexValue: String, nonRegexColumn: String, nonRegexValue: String)

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

    val nonMetricShardKeyFilters = LogicalPlan.getRawSeriesFilters(logicalPlan).filter(f => dataset.options.
      nonMetricShardColumns.contains(f.column))
    // Fixed scalar, metadata query etc will be executed by multiPartitionPlanner directly
    if (nonMetricShardKeyFilters.isEmpty) queryPlanner.materialize(logicalPlan, qContext)
    else {
      val execPlans = shardKeyMatcher(nonMetricShardKeyFilters).map { f =>
        queryPlanner.materialize(logicalPlan.replaceFilters(f), qContext)
      }
      if (execPlans.size == 1) execPlans.head
      else {
         logicalPlan match {
           case a: Aggregate  => ReduceAggregateExec(qContext, InProcessPlanDispatcher, execPlans, a.operator, a.params)
           case b: BinaryJoin => throw new UnsupportedOperationException("Regex not supported for Binary Join")
           case _             => DistConcatExec(qContext, InProcessPlanDispatcher, execPlans)
         }
      }
    }
  }
}
