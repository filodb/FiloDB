package filodb.coordinator.queryplanner

import filodb.core.metadata.Dataset
import filodb.core.query.{Filter, QueryContext}
import filodb.query.{Aggregate, BinaryJoin, LogicalPlan}
import filodb.query.exec.{DistConcatExec, ExecPlan, InProcessPlanDispatcher, ReduceAggregateExec}

case class ShardColumnValues(regexColumn: String, regexValue: String, nonRegexColumn: String, nonRegexValue: String)

/**
  * Responsible for query planning for queries having regex in shard column
  *
  * @param nonRegexShardColumn shard column which is mandatory and cannot be a regex
  * @param dataset dataset
  * @param multiPartitionPlanner multiPartition query planner
  * @param regexFieldMatcher used to get values for nonRegexShardColumn matching shard key column regex.
  */

class ShardKeyRegexPlanner(nonRegexShardColumn: String,
                           dataset: Dataset,
                           multiPartitionPlanner: QueryPlanner,
                           regexFieldMatcher: ShardColumnValues => Seq[String]) extends QueryPlanner {
  /**
    * Converts a logical plan to execution plan.
    *
    * @param logicalPlan Logical plan after converting PromQL -> AST -> LogicalPlan
    * @param qContext    holder for additional query parameters
    * @return materialized Execution Plan which can be dispatched
    */
  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val regexShardKeyColumn = dataset.options.nonMetricShardColumns.filterNot(_.equals(nonRegexShardColumn))
    val regexShardKeyValue = LogicalPlan.getRawSeriesRegex(logicalPlan, regexShardKeyColumn.head)
    val nonRegexShardKeyValue = LogicalPlan.getColumnValues(logicalPlan, nonRegexShardColumn)
    // Fixed scalar, metadata query etc will be executed by multiPartitionPlanner directly
    if (regexShardKeyValue.isEmpty) multiPartitionPlanner.materialize(logicalPlan, qContext)
    else {
      val regexValues = regexFieldMatcher(ShardColumnValues(regexShardKeyColumn.head, regexShardKeyValue.get,
        nonRegexShardColumn, nonRegexShardKeyValue.head))
      val execPlans = regexValues.map { r =>
      multiPartitionPlanner.materialize(logicalPlan.updateFilter(regexShardKeyColumn.head,
        Filter.Equals(r)), qContext)
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
