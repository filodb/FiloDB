package filodb.coordinator.queryplanner

import filodb.coordinator.queryplanner.LogicalPlanUtils.getLookBackMillis
import filodb.core.metadata.Dataset
import filodb.core.query.{PromQlQueryParams, QueryUtils}
import filodb.core.query.Filter.{Equals, EqualsRegex}
import filodb.query.LogicalPlan

/**
 * Abstract class for planners that need getPartitions functionality.
 *
 * FIXME: the ShardKeyRegexPlanner and MultiPartitionPlanner share purpose/responsibility
 *   and should eventually be merged. Currently, the SKRP needs getPartitions to group
 *   resolved shard-keys by partition before it individually materializes each of these
 *   groups with the MPP. The MPP will again find the corresponding partition for each group
 *   and materialize accordingly, then the SKRP will handle any higher-level concatenation/aggregation/joins
 *   for each of these groups.
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
                              infiniteTimeRange: Boolean = false) : Seq[PartitionAssignment] = {

    //1.  Get a Seq of all Leaf node filters
    val leafFilters = LogicalPlan.getColumnFilterGroup(logicalPlan)
    val nonMetricColumnSet = dataset.options.nonMetricShardColumns.toSet
    //2. Filter from each leaf node filters to keep only nonShardKeyColumns and convert them to key value map
    val routingKeyMap: Seq[Map[String, String]] = leafFilters
      .filter(_.nonEmpty)
      .map(_.filter(col => nonMetricColumnSet.contains(col.column)))
      .map{ filters =>
        filters.map { filter =>
          val values = filter.filter match {
            case Equals(value) => Seq(value.toString)
            // Split '|'-joined values if pipes are the only regex chars used.
            case EqualsRegex(value: String) if QueryUtils.isPipeOnlyRegex(value) =>
              value.split('|').toSeq
            case _ => throw new IllegalArgumentException(
              s"""shard keys must be filtered by equality or "|"-only regex. filter=${filter}""")
          }
          (filter.column, values)
        }
      }
      .flatMap{ keyValuesPairs =>
        // Get all possible value combos, then create a key->value map for each combo.
        // Ordering the pairs first since the output of combinations() is also ordered.
        val orderedPairs = keyValuesPairs.toSeq
        val keys: Seq[String] = orderedPairs.map(_._1)
        val values: Seq[Seq[String]] = orderedPairs.map(_._2)
        QueryUtils.combinations(values).map(keys.zip(_).toMap)
      }

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
      partitionLocationProvider.getPartitions(metricMap, queryTimeRange))
  }
  // scalastyle:on method.length

  /**
   * Checks if all the PartitionAssignments belong to same partition
   */
  protected def isSinglePartition(partitions: Seq[PartitionAssignment]) : Boolean = {
    if (partitions.isEmpty)
      true
    else {
      val partName = partitions.head.partitionName
      partitions.forall(_.partitionName.equals(partName))
    }
  }
}
