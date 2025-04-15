package filodb.coordinator.queryplanner

import filodb.coordinator.queryplanner.LogicalPlanUtils.getLookBackMillis
import filodb.core.metadata.Dataset
import filodb.core.query.{PromQlQueryParams, QueryContext, QueryUtils}
import filodb.core.query.Filter.{Equals, EqualsRegex}
import filodb.query.{Aggregate, LogicalPlan}
import filodb.query.exec.{ExecPlan, PromQlRemoteExec}

/**
 * Abstract class for planners that need getPartitions functionality.
 *
 * FIXME: the ShardKeyRegexPlanner and MultiPartitionPlanner share purpose/responsibility
 *   and should eventually be merged. Currently, the SKRP needs getPartitions to group
 *   resolved shard-keys by partition before it individually materializes each of these
 *   groups with the MPP. The MPP will again find the corresponding partition for each group
 *   and materialize accordingly.
 */
abstract class PartitionLocationPlanner(dataset: Dataset,
                                        partitionLocationProvider: PartitionLocationProvider)
  extends QueryPlanner with DefaultPlanner {

  // scalastyle:off method.length
  /**
   * Gets the partition Assignment for the given plan
   */
  protected def getPartitions(logicalPlan: LogicalPlan,
                              queryParams: PromQlQueryParams,
                              infiniteTimeRange: Boolean = false) : Seq[PartitionAssignmentTrait] = {

    //1.  Get a Seq of all Leaf node filters
    val leafFilters = LogicalPlan.getColumnFilterGroup(logicalPlan)
    val nonMetricColumnSet = dataset.options.nonMetricShardColumns.toSet
    //2. Filter from each leaf node filters to keep only nonMetricShardKeyColumns and convert them to key value map
    val routingKeyMap: Seq[Map[String, String]] = leafFilters
      .filter(_.nonEmpty)
      .map(_.filter(col => nonMetricColumnSet.contains(col.column)))
      .map{ filters =>
        filters.map { filter =>
          val values = filter.filter match {
            case Equals(value) => Seq(value.toString)
            case EqualsRegex(value: String) if QueryUtils.containsPipeOnlyRegex(value) =>
              QueryUtils.splitAtUnescapedPipes(value)
            case _ => throw new IllegalArgumentException(
              s"""shard keys must be filtered by equality or "|"-only regex. filter=${filter}""")
          }
          (filter.column, values)
        }
      }
      .flatMap(keyToVals => QueryUtils.makeAllKeyValueCombos(keyToVals.toMap))
      .distinct

    // 3. Determine the query time range
    val queryTimeRange = if (infiniteTimeRange) {
      TimeRange(0, Long.MaxValue)
    } else {
      // 3a. Get the start and end time is ms based on the lookback, offset and the user provided start and end time
      val (maxOffsetMs, minOffsetMs) = LogicalPlanUtils.getOffsetMillis(logicalPlan)
        .foldLeft((Long.MinValue, Long.MaxValue)) {
          case ((accMax, accMin), currValue) => (accMax.max(currValue), accMin.min(currValue))
        }

      val periodicSeriesTimeWithOffset = TimeRange((queryParams.startSecs * 1000) - maxOffsetMs,
        (queryParams.endSecs * 1000) - minOffsetMs)
      val lookBackMs = getLookBackMillis(logicalPlan).max

      //3b Get the Query time range based on user provided range, offsets in previous steps and lookback
      TimeRange(periodicSeriesTimeWithOffset.startMs - lookBackMs,
        periodicSeriesTimeWithOffset.endMs)
    }

    //4. Based on the map in 2 and time range in 5, get the partitions to query
    routingKeyMap.flatMap(metricMap =>
      partitionLocationProvider.getPartitionsTrait(metricMap, queryTimeRange))
  }
  // scalastyle:on method.length

  /**
   * Checks if all the PartitionAssignments belong to same partition
   */
  protected def isSinglePartition(partitions: Seq[PartitionAssignmentTrait]) : Boolean = {
    if (partitions.isEmpty)
      true
    else {
      val pSet = partitions.flatMap(p => p.proportionMap.keys)
      pSet.forall(p => p.equals(pSet.head))
    }
  }

  protected def canSupportMultiPartitionCalls(execPlans: Seq[ExecPlan]): Boolean =
    execPlans.forall {
      case _: PromQlRemoteExec => false
      case _ => true
    }

  /**
   * Returns true iff the argument Aggregation can be pushed down.
   * More specifically, this means that it is correct to materialize the entire plan
   * with a lower-level planner and aggregate again across all returned plans.
   *
   * @param innerTschemaLabels occupied iff the inner plan can be pushed-down according to the set of labels.
   */
  protected def canPushdownAggregate(agg: Aggregate,
                                     innerTschemaLabels: Option[Seq[String]],
                                     qContext: QueryContext): Boolean = {
    // We can pushdown when shard-key labels are preserved throughout the entire inner subtree.
    // For example, consider:
    //   sum(max(foo{shardKeyLabel=~".*"}) by (label1))
    // If this data lives on two partitions, this might have its pushdown-optimized execution planned as:
    //   Sum                   // 6
    //     Sum                 // 5
    //       Max(local_data)   // label1=A -> 3, labelB -> 2
    //     Sum                 // 1
    //       Max(remote_data)  // label1=A -> 1
    // If this data had instead lived on one partition, the materialized plan might have been:
    //   Sum                 // 5
    //     Max(local_data)   // label1=A -> 3, labelB -> 2
    // This discrepancy happens because aggregation occurs across values that account for the same
    //   series (in this case: label1=A). To prevent this, we can pushdown only when the inner vector
    //   tree preserves shard-key labels.

    // Always safe to pushdown if no nested joins/aggregations.
    if (!LogicalPlanUtils.hasDescendantAggregateOrJoin(agg.vectors)) {
      return true
    }

    // Prevent further pushdown according to the PlannerParams.
    if (!qContext.plannerParams.allowNestedAggregatePushdown) {
      return false
    }

    // If a tschema applies, instead require only that non-shard-key target-schema labels
    //   are present, since shard-key target-schema labels are implicit in each clause.
    val labels = if (innerTschemaLabels.isDefined) {
      innerTschemaLabels.get.filter(!dataset.options.nonMetricShardColumns.contains(_))
    } else {
      dataset.options.nonMetricShardColumns
    }
    LogicalPlanUtils.treePreservesLabels(agg.vectors, labels)
  }
}
