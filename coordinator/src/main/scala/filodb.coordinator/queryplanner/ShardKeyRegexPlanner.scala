package filodb.coordinator.queryplanner

import filodb.core.metadata.Dataset
import filodb.core.query.{Filter, QueryContext}
import filodb.query.{Aggregate, LogicalPlan}
import filodb.query.exec.{DistConcatExec, ExecPlan, InProcessPlanDispatcher, ReduceAggregateExec}


case class RegexColumn(column: String, regex: String)
class ShardKeyRegexPlanner(mandatoryColumn: String,
                           dataset: Dataset,
                           multiPartitionPlanner: QueryPlanner,
                           regexFieldMatcher: RegexColumn => Seq[String]) extends QueryPlanner {
  /**
    * Converts a logical plan to execution plan.
    *
    * @param logicalPlan Logical plan after converting PromQL -> AST -> LogicalPlan
    * @param qContext    holder for additional query parameters
    * @return materialized Execution Plan which can be dispatched
    */
  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val regexShardKeyColumn = dataset.options.nonMetricShardColumns.filterNot(_.equals(mandatoryColumn))
    val regexShardKeyValue = LogicalPlan.getRawSeriesRegex(logicalPlan, regexShardKeyColumn.head)
    // Fixed scalar, metadata query etc will be executed by multiPartitionPlanner directly
    if (regexShardKeyValue.isEmpty) multiPartitionPlanner.materialize(logicalPlan, qContext)
    else {
      val execPlans = regexFieldMatcher(RegexColumn(regexShardKeyColumn.head, regexShardKeyValue.get)).map { r =>
      multiPartitionPlanner.materialize(LogicalPlan.updateFilter(logicalPlan, regexShardKeyColumn.head,
        Filter.Equals(r)), qContext)
      }
       if (execPlans.size == 1) execPlans.head
       else {
         logicalPlan match {
           case a: Aggregate => ReduceAggregateExec(qContext, InProcessPlanDispatcher, execPlans, a.operator, a.params)
           case _            => DistConcatExec(qContext, InProcessPlanDispatcher, execPlans)
         }
       }

    }
  }
}
