package filodb.query.util

import com.typesafe.scalalogging.StrictLogging
import scala.jdk.CollectionConverters.asScalaBufferConverter

import filodb.core.GlobalConfig
import filodb.core.query.ColumnFilter
import filodb.core.query.Filter.Equals
import filodb.query.LogicalPlan

final case class HierarchicalQueryExperience(isInclude: Boolean,
                                             metricRegex: String,
                                             metricSuffix: String,
                                             tags: Set[String])
object HierarchicalQueryExperience extends StrictLogging {

  // Get the shard key columns from the dataset options along with all the metric labels used
  lazy val shardKeyColumns: Option[Set[String]] = GlobalConfig.datasetOptions match {
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
  def upsertFilters(filters: Seq[ColumnFilter], newFilters: Seq[ColumnFilter]): Seq[ColumnFilter] = {
    val filterColumns = newFilters.map(_.column)
    val updatedFilters = filters.filterNot(f => filterColumns.contains(f.column)) ++ newFilters
    updatedFilters
  }

  /**
   * Checks if the higher level aggregation is applicable with IncludeTags.
   * @param tagsToIgnoreForCheck - Seq[String] - List of shard key columns. These columns are not part of check. This
   *                             include tags which are compulsory for the query like _metric_, _ws_, _ns_.
   * @param filterTags - Seq[String] - List of filter tags/labels in the query or in the aggregation clause
   * @param includeTags - Set[String] - Include tags as specified in the aggregation rule
   * @return - Boolean
   */
  def isHigherLevelAggregationApplicableWithIncludeTags(tagsToIgnoreForCheck: Set[String],
                                                        filterTags: Seq[String], includeTags: Set[String]): Boolean = {
    filterTags.forall( tag => tagsToIgnoreForCheck.contains(tag) || includeTags.contains(tag))
  }

  /**
   * Checks if the higher level aggregation is applicable with ExcludeTags. Here we need to check if the column filter
   * tags present in query or aggregation clause, should not be a part of ExcludeTags.
   *
   * @param tagsToIgnoreForCheck - Seq[String] - List of shard key columns. These columns are not part of check. This
   *                             include tags which are compulsory for the query like _metric_, _ws_, _ns_.
   * @param filterTags - Seq[String] - List of filter tags/labels in the query or in the aggregation clause
   * @param excludeTags - Set[String] - Exclude tags as specified in the aggregation rule
   * @return - Boolean
   */
  def isHigherLevelAggregationApplicableWithExcludeTags(tagsToIgnoreForCheck: Set[String],
                                                        filterTags: Seq[String], excludeTags: Set[String]): Boolean = {
    val columnFilterTagsPresentInExcludeTags = filterTags.filterNot {
      tag => tagsToIgnoreForCheck.contains(tag) || (!excludeTags.contains(tag))
    }.isEmpty
    columnFilterTagsPresentInExcludeTags
  }

  /** Checks if the higher level aggregation is applicable for the given Include/Exclude tags.
   * @param isInclude - Boolean
   * @param filterTags - Seq[String] - List of filter tags/labels in the query or in the aggregation clause
   * @param tags - Set[String] - Include or Exclude tags as specified in the aggregation rule
   * @return - Boolean
   */
  def isHigherLevelAggregationApplicable(isInclude: Boolean,
                                         filterTags: Seq[String], tags: Set[String]): Boolean = {
    shardKeyColumns match {
      case None =>
        logger.info("[HierarchicalQueryExperience] Dataset options config not found. Skipping optimization !")
        false
      case Some(tagsToIgnoreForCheck) =>
        if (isInclude) {
          isHigherLevelAggregationApplicableWithIncludeTags(tagsToIgnoreForCheck, filterTags, tags)
        }
        else {
          isHigherLevelAggregationApplicableWithExcludeTags(tagsToIgnoreForCheck, filterTags, tags)
        }
    }
  }

  /** Returns the next level aggregated metric name. Example
   *  metricRegex = :::
   *  metricSuffix = agg_2
   *  Exiting metric name - metric1:::agg
   *  After update - metric1:::agg -> metric1:::agg_2
   * @param metricColumnFilter - String - Metric ColumnFilter tag/label
   * @param params - HierarchicalQueryExperience - Contains
   * @param filters - Seq[ColumnFilter] - label filters of the query/lp
   * @return - Option[String] - Next level aggregated metric name
   */
  def getNextLevelAggregatedMetricName(metricColumnFilter: String, params: HierarchicalQueryExperience,
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
  def isParentPeriodicSeriesPlanAllowedForRawSeriesUpdateForHigherLevelAggregatedMetric(
                                       parentLogicalPlans: Seq[String]): Boolean = {
    GlobalConfig.hierarchicalConfig match {
      case Some(hierarchicalConfig) =>
        parentLogicalPlans.exists(hierarchicalConfig
          .getStringList("allowed-parent-logical-plans").contains)
      case None => false
    }
  }

  /**
   * Checks if the PeriodicSeriesPlan which has access to RawSeries is allowed to update/optimize the metric name
   * @param logicalPlanName - PeriodicSeriesPlan name. Ex - PeriodicSeriesWithWindowing, PeriodicSeries
   * @return - Boolean
   */
  def isPeriodicSeriesPlanAllowedForRawSeriesUpdateForHigherLevelAggregatedMetric(
                                                 logicalPlanName: String): Boolean = {
    GlobalConfig.hierarchicalConfig match {
      case Some(hierarchicalConfig) =>
        hierarchicalConfig
          .getStringList("allowed-periodic-series-plans-with-raw-series")
          .contains(logicalPlanName)
      case None => false
    }
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
  def upsertMetricColumnFilterIfHigherLevelAggregationApplicable(params: HierarchicalQueryExperience,
                                                                 filters: Seq[ColumnFilter]): Seq[ColumnFilter] = {
    val filterTags = filters.map(x => x.column)
    if (isHigherLevelAggregationApplicable(params.isInclude, filterTags, params.tags)) {
      val metricColumnFilter = getMetricColumnFilterTag(filterTags, GlobalConfig.datasetOptions.get.metricColumn)
      val updatedMetricName = getNextLevelAggregatedMetricName(metricColumnFilter, params, filters)
      updatedMetricName match {
        case Some(metricName) =>
          val updatedFilters = upsertFilters(filters, Seq(ColumnFilter(metricColumnFilter, Equals(metricName))))
          logger.info(s"[HierarchicalQueryExperience] Query optimized with filters: ${updatedFilters.toString()}")
          updatedFilters
        case None => filters
      }
    } else {
      filters
    }
  }
}
