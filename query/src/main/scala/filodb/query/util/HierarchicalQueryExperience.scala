package filodb.query.util

import com.typesafe.scalalogging.StrictLogging

import filodb.core.GlobalConfig
import filodb.core.query.ColumnFilter
import filodb.core.query.Filter.Equals
import filodb.query.LogicalPlan

final case class HierarchicalQueryExperience(isInclude: Boolean,
                                             metricRegex: String,
                                             metricSuffix: String,
                                             tags: Set[String],
                                             parentLogicalPlans: Seq[String])
object HierarchicalQueryExperience extends StrictLogging {

  /**
   * @param filterTags
   * @param datasetMetricColumn
   * @return
   */
  def getMetricColumnFilterTag(filterTags: Seq[String], datasetMetricColumn: String): String = {
    filterTags.find(_ == datasetMetricColumn).getOrElse(
      filterTags.find(_ == GlobalConfig.PromMetricLabel).getOrElse(datasetMetricColumn)
    )
  }

  /**
   * @param filters
   * @param newFilters
   * @return
   */
  def upsertFilters(filters: Seq[ColumnFilter], newFilters: Seq[ColumnFilter]): Seq[ColumnFilter] = {
    val filterColumns = newFilters.map(_.column)
    val updatedFilters = filters.filterNot(f => filterColumns.contains(f.column)) ++ newFilters
    updatedFilters
  }

  /**
   * @param shardKeyColumnTags
   * @param filterTags
   * @param includeTags
   * @return
   */
  def isHigherLevelAggregationApplicableWithIncludeTags(shardKeyColumnTags: Seq[String],
                                                        filterTags: Seq[String], includeTags: Set[String]): Boolean = {
    // filter out shard key columns and make a set of all columns in filters
    val allTagsInFiltersSet = filterTags.filterNot {
      tag => shardKeyColumnTags.contains(tag)
    }.toSet
    allTagsInFiltersSet.subsetOf(includeTags)
  }

  /**
   * @param shardKeyColumnTags
   * @param filterTags
   * @param excludeTags
   * @return
   */
  def isHigherLevelAggregationApplicableWithExcludeTags(shardKeyColumnTags: Seq[String],
                                                        filterTags: Seq[String], excludeTags: Set[String]): Boolean = {
    val tagsNotPresentInExcludeTagsSet = filterTags.filterNot {
      tag => shardKeyColumnTags.contains(tag) || excludeTags.contains(tag)
    }.toSet
    tagsNotPresentInExcludeTagsSet.isEmpty
  }

  /**
   * @param isInclude
   * @param filterTags
   * @param tags
   * @return
   */
  def isHigherLevelAggregationApplicable(isInclude: Boolean,
                                         filterTags: Seq[String], tags: Set[String]): Boolean = {
    GlobalConfig.datasetOptions match {
      case None =>
        logger.info("Dataset options config not found. Skipping optimization !")
        false
      case Some(datasetOptions) =>
        val updatedShardKeyColumns = datasetOptions.shardKeyColumns :+ getMetricColumnFilterTag(
          filterTags,
          datasetOptions.metricColumn)
        if (isInclude) {
          isHigherLevelAggregationApplicableWithIncludeTags(updatedShardKeyColumns, filterTags, tags)
        }
        else {
          isHigherLevelAggregationApplicableWithExcludeTags(updatedShardKeyColumns, filterTags, tags)
        }
    }
  }

  /**
   * @param metricColumnFilter
   * @param metricSuffix
   * @param filters
   * @return
   */
  def getNextLevelAggregatedMetricName(metricColumnFilter: String, metricSuffix: String,
                                       filters: Seq[ColumnFilter]): Option[String] = {
    val metricNameSeq = LogicalPlan.getColumnValues(filters, metricColumnFilter)
    metricNameSeq match {
      case Seq() => None
      // TODO: make it a config
      case _ => Some(metricNameSeq.head.replaceFirst(":::.*", ":::" + metricSuffix))
    }
  }

  /**
   * Checks if the given aggregation operator is allowed for next level aggregated metric.
   * @param operatorName - String - Aggregation operator name. Ex - sum, avg, min, max, count.
   * @return - Boolean
   */
  def isAggregationOperatorAllowed(operatorName: String): Boolean = GlobalConfig.hierarchicalConfig match {
    case Some(hierarchicalConfig) => hierarchicalConfig
      .getStringList("allowed-aggregation-operators")
      .contains(operatorName)
    case None => false
  }

  /**
   * Checks if the query with the given range function is allowed for next level aggregated metric.
   * @param rangeFunctionEntryName - String - Range function name. Ex - rate, increase etc.
   * @return - Boolean
   */
  def isRangeFunctionAllowed(rangeFunctionEntryName: String): Boolean = GlobalConfig.hierarchicalConfig match {
    case Some(hierarchicalConfig) =>
      hierarchicalConfig
        .getStringList("allowed-range-functions")
        .contains(rangeFunctionEntryName)
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
  def upsertMetricColumnFilterIfHigherLevelAggregationApplicable(isInclude: Boolean,
                                                                 metricSuffix: String,
                                                                 filters: Seq[ColumnFilter],
                                                                 tags: Set[String]): Seq[ColumnFilter] = {
    // get metric name filter i.e either datasetOptions.get.metricColumn or PromMetricLabel - We need to support both
    // the cases
    val filterTags = filters.map(x => x.column)
    if (isHigherLevelAggregationApplicable(isInclude, filterTags, tags)) {
      val metricColumnFilter = getMetricColumnFilterTag(filterTags, GlobalConfig.datasetOptions.get.metricColumn)
      val updatedMetricName = getNextLevelAggregatedMetricName(metricColumnFilter, metricSuffix, filters)
      updatedMetricName match {
        case Some(metricName) => upsertFilters(filters, Seq(ColumnFilter(metricColumnFilter, Equals(metricName))))
        case None => filters
      }
    } else {
      filters
    }
  }
}
