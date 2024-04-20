package filodb.coordinator.queryplanner

import filodb.coordinator.queryplanner.LogicalPlanUtils.{canPushdownAggregate, getLookBackMillis}
import filodb.core.metadata.Dataset
import filodb.core.query.{ColumnFilter, PromQlQueryParams, QueryUtils}
import filodb.core.query.Filter.{Equals, EqualsRegex}
import filodb.query.AggregateClause.ClauseType
import filodb.query.{Aggregate, AggregateClause, LogicalPlan, NonLeafLogicalPlan, RawSeries}
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
                                        partitionLocationProvider: PartitionLocationProvider,
                                        shardKeyMatcher: Seq[ColumnFilter] => Seq[Seq[ColumnFilter]])
  extends QueryPlanner with DefaultPlanner {

  private def treePreservesLabels(lp: LogicalPlan, labels: Seq[String]): Boolean = lp match {
    case agg: Aggregate =>
      val clausePreservesLabels = agg.clauseOpt match {
        case Some(AggregateClause(ClauseType.By, clauseLabels)) =>
          labels.forall(clauseLabels.contains(_))
        case Some(AggregateClause(ClauseType.Without, clauseLabels)) =>
          labels.forall(!clauseLabels.contains(_))
        case _ => labels.isEmpty
      }
      if (!clausePreservesLabels) {
        return false
      }
      treePreservesLabels(agg.vectors, labels)
    case nl: NonLeafLogicalPlan => nl.children.forall(treePreservesLabels(_, labels))
  }

  private def getShardKeyLabelsIfTODO(lp: LogicalPlan): Option[Seq[Map[String, String]]] = lp match {
    case rs: RawSeries =>
      val res = LogicalPlan.getNonMetricShardKeyFilters(rs, dataset.options.nonMetricShardColumns)
        .flatMap(shardKeyMatcher(_))
        .map { colFilters =>
          colFilters.map { colFilter =>
            val value = colFilter.filter match {
              case Equals(value) => value.toString
              case _ => throw new IllegalArgumentException("TODO")
            }
            colFilter.column -> value
          }.toMap
        }.distinct
      Some(res)
    case agg: Aggregate =>
      val inner = getShardKeyLabelsIfTODO(agg.vectors)
      if (inner.isEmpty) {
        return None
      }
      val keyLabels = dataset.options.nonMetricShardColumns
      val clausePreservesLabels = agg.clauseOpt match {
        case Some(AggregateClause(ClauseType.By, clauseLabels)) =>
          keyLabels.forall(clauseLabels.contains(_))
        case Some(AggregateClause(ClauseType.Without, clauseLabels)) =>
          keyLabels.forall(!clauseLabels.contains(_))
        case _ => false
      }
      if (clausePreservesLabels) inner else None
    case np: NonLeafLogicalPlan =>


  }
  protected def canPushdownAggregate(agg: Aggregate): Boolean = {
    if (LogicalPlanUtils.shouldAlwaysPushdownAggregate(agg)) {
      return true
    }
    getShardKeyLabelsIfTODO(agg).nonEmpty
  }

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

  protected def canSupportMultiPartitionCalls(execPlans: Seq[ExecPlan]): Boolean =
    execPlans.forall {
      case _: PromQlRemoteExec => false
      case _ => true
    }
}
