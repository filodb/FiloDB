package filodb.query.util

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import scala.jdk.CollectionConverters.asScalaBufferConverter

import filodb.core.GlobalConfig
import filodb.core.query.ColumnFilter
import filodb.core.query.Filter.Equals
import filodb.query.{AggregateClause, AggregationOperator, LogicalPlan, TsCardinalities}

/**
 * Aggregation rule definition. Contains the following information:
 * 1. aggregation metric regex to be matched
 * 2. metric suffix for the given aggregation rule
 * 3. include/exclude tags for the given aggregation rule
 */
sealed trait AggRule {

  val metricRegex: String

  val metricSuffix: String

  val tags: Set[String]
  def isHigherLevelAggregationApplicable(shardKeyColumns: Set[String], filterTags: Seq[String]): Boolean
}

case class IncludeAggRule(metricRegex: String, metricSuffix: String, tags: Set[String]) extends AggRule {

  /**
   * Checks if the higher level aggregation is applicable with IncludeTags.
   *
   * @param shardKeyColumns - Seq[String] - List of shard key columns. These columns are not part of check. This
   *                        include tags which are compulsory for the query like _metric_, _ws_, _ns_.
   * @param filterTags      - Seq[String] - List of filter tags/labels in the query or in the aggregation clause
   * @param includeTags     - Set[String] - Include tags as specified in the aggregation rule
   * @return - Boolean
   */
  override def isHigherLevelAggregationApplicable(shardKeyColumns: Set[String], filterTags: Seq[String]): Boolean = {
    filterTags.forall( tag => shardKeyColumns.contains(tag) || tags.contains(tag))
  }
}

case class ExcludeAggRule(metricRegex: String, metricSuffix: String, tags: Set[String]) extends AggRule {

  /**
   * Checks if the higher level aggregation is applicable with ExcludeTags. Here we need to check if the column filter
   * tags present in query or aggregation clause, should not be a part of ExcludeTags.
   *
   * @param shardKeyColumns - Seq[String] - List of shard key columns. These columns are not part of check. This
   *                        include tags which are compulsory for the query like _metric_, _ws_, _ns_.
   * @param filterTags      - Seq[String] - List of filter tags/labels in the query or in the aggregation clause
   * @param excludeTags     - Set[String] - Exclude tags as specified in the aggregation rule
   * @return - Boolean
   */
  override def isHigherLevelAggregationApplicable(shardKeyColumns: Set[String], filterTags: Seq[String]): Boolean = {
    filterTags.forall { tag => shardKeyColumns.contains(tag) || (!tags.contains(tag)) }
  }
}

object HierarchicalQueryExperience extends StrictLogging {

  val hierarchicalQueryOptimizedCounter = Kamon.counter("hierarchical-query-plans-optimized")

  // Get the shard key columns from the dataset options along with all the metric labels used
  lazy val shardKeyColumnsOption: Option[Set[String]] = GlobalConfig.datasetOptions match {
    case Some(datasetOptions) =>
      Some((datasetOptions.shardKeyColumns ++ Seq( datasetOptions.metricColumn, GlobalConfig.PromMetricLabel)).toSet)
    case None => None
  }

  // Get the allowed aggregation operators from the hierarchical config
  lazy val allowedAggregationOperators: Option[Set[String]] = GlobalConfig.hierarchicalConfig match {
    case Some(hierarchicalConfig) =>
      Some(hierarchicalConfig.getStringList("allowed-aggregation-operators").asScala.toSet)
    case None => None
  }

  // Get the allowed range functions from the hierarchical config
  lazy val allowedRangeFunctions: Option[Set[String]] = GlobalConfig.hierarchicalConfig match {
    case Some(hierarchicalConfig) =>
      Some(hierarchicalConfig.getStringList("allowed-range-functions").asScala.toSet)
    case None => None
  }

  // Get the allowed periodic series plans which have access to RawSeries from the hierarchical config
  lazy val allowedPeriodicSeriesPlansWithRawSeries: Option[Set[String]] = GlobalConfig.hierarchicalConfig match {
    case Some(hierarchicalConfig) =>
      Some(hierarchicalConfig.getStringList("allowed-periodic-series-plans-with-raw-series").asScala.toSet)
    case None => None
  }

  // Get the allowed parent logical plans for optimization from the hierarchical config
  lazy val allowedLogicalPlansForOptimization: Option[Set[String]] = GlobalConfig.hierarchicalConfig match {
    case Some(hierarchicalConfig) =>
      Some(hierarchicalConfig.getStringList("allowed-parent-logical-plans").asScala.toSet)
    case None => None
  }

  /**
   * Helper function to get the ColumnFilter tag/label for the metric. This is needed to correctly update the filter.
   * @param filterTags - Seq[String] - List of ColumnFilter tags/labels
   * @param datasetMetricColumn - String - Metric ColumnFilter tag/label from the configured dataset options
   * @return - String - ColumnFilter tag/label for the metric
   */
  def getMetricColumnFilterTag(filterTags: Seq[String], datasetMetricColumn: String): String = {
    // get metric name filter i.e either datasetOptions.get.metricColumn or PromMetricLabel - We need to support both
    // the cases
    filterTags.find( tag => tag == datasetMetricColumn || tag == GlobalConfig.PromMetricLabel)
      .getOrElse(datasetMetricColumn)
  }

  /**
   * Helper function to update the filters with new filters. Example:
   * filters = Seq(
   *      ColumnFilter("tag1", Equals("value1")),
   *      ColumnFilter("tag2", Equals("value2")))
   *
   * newFilters = Seq(
   *      ColumnFilter("tag2", Equals("value2Updated")),
   *      ColumnFilter("tag4", Equals("value4")))
   *
   * Updated filters = Seq(
   *      ColumnFilter("tag1", Equals("value1")),
   *      ColumnFilter("tag2", Equals("value2Updated")),
   *      ColumnFilter("tag4", Equals("value4")))
   *
   * @param filters - Seq[ColumnFilter] - Existing filters
   * @param newFilters - Seq[ColumnFilter] - New filters to be added/updated
   * @return - Seq[ColumnFilter] - Updated filters
   */
  private def upsertFilters(filters: Seq[ColumnFilter], newFilters: Seq[ColumnFilter]): Seq[ColumnFilter] = {
    val filterColumns = newFilters.map(_.column)
    val updatedFilters = filters.filterNot(f => filterColumns.contains(f.column)) ++ newFilters
    updatedFilters
  }

  /** Checks if the higher level aggregation is applicable for the given Include/Exclude tags.
   * @param isInclude - Boolean
   * @param filterTags - Seq[String] - List of filter tags/labels in the query or in the aggregation clause
   * @param tags - Set[String] - Include or Exclude tags as specified in the aggregation rule
   * @return - Boolean
   */
  def isHigherLevelAggregationApplicable(params: AggRule,
                                         filterTags: Seq[String]): Boolean = {
    shardKeyColumnsOption match {
      case None =>
        logger.info("[HierarchicalQueryExperience] Dataset options config not found. Skipping optimization !")
        false
      case Some(shardKeyColumns) =>
        params.isHigherLevelAggregationApplicable(shardKeyColumns, filterTags)
    }
  }

  /** Returns the next level aggregated metric name. Example
   *  metricRegex = :::
   *  metricSuffix = agg_2
   *  Existing metric name - metric1:::agg
   *  After update - metric1:::agg -> metric1:::agg_2
   * @param metricColumnFilter - String - Metric ColumnFilter tag/label
   * @param params - HierarchicalQueryExperience - Contains
   * @param filters - Seq[ColumnFilter] - label filters of the query/lp
   * @return - Option[String] - Next level aggregated metric name
   */
  def getNextLevelAggregatedMetricName(metricColumnFilter: String, params: AggRule,
                                       filters: Seq[ColumnFilter]): Option[String] = {
    // Get the metric name from the filters
    val metricNameSeq = LogicalPlan.getColumnValues(filters, metricColumnFilter)
    metricNameSeq match {
      case Seq() => None
      case _ => Some(metricNameSeq.head.replaceFirst(
        params.metricRegex + ".*",
        params.metricRegex + params.metricSuffix))
    }
  }

  /**
   * Checks if the given aggregation operator is allowed for next level aggregated metric.
   * @param operatorName - String - Aggregation operator name. Ex - sum, avg, min, max, count.
   * @return - Boolean
   */
  def isAggregationOperatorAllowed(operatorName: String): Boolean = allowedAggregationOperators match {
    case Some(allowedAggregationOperatorsSet) => allowedAggregationOperatorsSet.contains(operatorName)
    case None => false
  }

  /**
   * Checks if the query with the given range function is allowed for next level aggregated metric.
   * @param rangeFunctionEntryName - String - Range function name. Ex - rate, increase etc.
   * @return - Boolean
   */
  def isRangeFunctionAllowed(rangeFunctionEntryName: String): Boolean = allowedRangeFunctions match {
    case Some(allowedRangeFunctionsSet) => allowedRangeFunctionsSet.contains(rangeFunctionEntryName)
    case None => false
  }

  /**
   * Checks if at least one of the parent LogicalPlan(s) is allowed to update/optimize the metric name. If there are
   * No parent LogicalPlans, then it returns false
   * @param parentLogicalPlans - Seq[String] - List of parent LogicalPlans. Ex - Seq("BinaryJoin", "Aggregate")
   * @return - Boolean
   */
  def isParentPeriodicSeriesPlanAllowed(parentLogicalPlans: Seq[String]): Boolean =
    allowedLogicalPlansForOptimization match {
      case Some(allowedLogicalPlans) => parentLogicalPlans.exists(allowedLogicalPlans.contains)
      case None => false
    }

  /**
   * Checks if the PeriodicSeriesPlan which has access to RawSeries is allowed to update/optimize the metric name
   * @param logicalPlanName - PeriodicSeriesPlan name. Ex - PeriodicSeriesWithWindowing, PeriodicSeries
   * @return - Boolean
   */
  def isLeafPeriodicSeriesPlanAllowedForRawSeriesUpdate(logicalPlanName: String): Boolean =
    allowedPeriodicSeriesPlansWithRawSeries match {
      case Some(allowedPeriodSeriesPlans) => allowedPeriodSeriesPlans.contains(logicalPlanName)
      case None => false
    }

  /**
   * Updates the metric column filter if higher level aggregation is applicable
   *
   * @param isInclude - Boolean - Tells if the given tags are IncludeTags or ExcludeTags
   * @param metricSuffix - String - Metric Suffix of the next aggregation level
   * @param filters - Seq[ColumnFilter] - label filters of the query/lp
   * @param tags - Include or Exclude tags as specified in the aggregation rule
   * @return - Seq[ColumnFilter] - Updated filters
   */
  def upsertMetricColumnFilterIfHigherLevelAggregationApplicable(params: AggRule,
                                                                 filters: Seq[ColumnFilter]): Seq[ColumnFilter] = {
    val filterTags = filters.map(x => x.column)
    if (isHigherLevelAggregationApplicable(params, filterTags)) {
      val metricColumnFilter = getMetricColumnFilterTag(filterTags, GlobalConfig.datasetOptions.get.metricColumn)
      val updatedMetricName = getNextLevelAggregatedMetricName(metricColumnFilter, params, filters)
      updatedMetricName match {
        case Some(metricName) =>
          // Checking if the metric actually ends with the next level aggregation metricSuffix.
          // If so, update the filters and emit metric
          // else, return the filters as is
          metricName.endsWith(params.metricSuffix) match {
            case true =>
              val updatedFilters = upsertFilters(filters, Seq(ColumnFilter(metricColumnFilter, Equals(metricName))))
              logger.info(s"[HierarchicalQueryExperience] Query optimized with filters: ${updatedFilters.toString()}")
              incrementHierarcicalQueryOptimizedCounter(updatedFilters)
              updatedFilters
            case false => filters
          }
        case None => filters
      }
    } else {
      filters
    }
  }

  /**
   * Track the queries optimized by workspace and namespace
   * @param filters
   */
  private def incrementHierarcicalQueryOptimizedCounter(filters: Seq[ColumnFilter]): Unit = {
    // track query optimized per workspace and namespace in the counter
    val metric_ws = LogicalPlan.getColumnValues(filters, TsCardinalities.LABEL_WORKSPACE) match {
      case Seq() => ""
      case ws => ws.head
    }
    val metric_ns = LogicalPlan.getColumnValues(filters, TsCardinalities.LABEL_NAMESPACE) match {
      case Seq() => ""
      case ns => ns.head
    }
    hierarchicalQueryOptimizedCounter
      .withTag("metric_ws", metric_ws)
      .withTag("metric_ns", metric_ns)
      .increment()
  }

  /**
   * Helper function to check the following:
   * Check 1: Check if the aggregation operator is enabled
   * Check 2: Check if the `by` and `without` clause labels satisfy the include/exclude tag constraints
   *
   * @param isInclude true if includeTags are specified, false if excludeTags are specified
   * @param tags      higher aggregation rule tags/labels as defined in aggregation rule
   * @return true if the current aggregate query can be optimized, false otherwise
   */
  def checkAggregateQueryEligibleForHigherLevelAggregatedMetric(params: AggRule,
                                                                operator: AggregationOperator,
                                                                clauseOpt: Option[AggregateClause]): Boolean = {
    HierarchicalQueryExperience.isAggregationOperatorAllowed(operator.entryName) match {
      case true =>
        clauseOpt match {
          case Some(clause) =>
            clause.clauseType match {
              case AggregateClause.ClauseType.By =>
                // Check if ALL the tags present in the by clause are part of includeTags or are not part of excludeTags
                if (HierarchicalQueryExperience.isHigherLevelAggregationApplicable(params, clause.labels)) {
                  true
                }
                else {
                  // can't be optimized further as by labels not present in the higher level metric's include tags
                  false
                }
              case AggregateClause.ClauseType.Without =>
                // This is a slightly more tricky case than the by clause. Following are the scenarios:
                // 1. For excludeTags scenario:
                // - We need to check if ALL the excludeTags is subset of the without clause's tags/labels
                // - This ensures that we are not using a tag/label which is not part of the higher level metric
                // 2. For includeTags scenario:
                // - We need to check if all tags/labels, which are NOT present in the without clause, must be part of
                //   the includeTags. This requires the knowledge of all the tags/labels which are being published
                //   for a metric. This info is not available during planning and hence we can't optimize this scenario.
                params match {
                  case IncludeAggRule(_, _, _) =>
                    // can't optimize this scenario as we don't have the required info at the planning stage
                    false
                  case ExcludeAggRule(_, _, excludeTags) =>
                    if (excludeTags.subsetOf(clause.labels.toSet)) { true }
                    else { false }
                }
            }
          case None =>
            // No aggregation clause present. Check raw-series plan to see if we can use the next aggregation level
            true
        }
      case _ => false
    }
  }
}
