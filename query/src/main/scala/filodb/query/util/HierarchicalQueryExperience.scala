package filodb.query.util

import com.typesafe.scalalogging.StrictLogging

import filodb.core.GlobalConfig

/**
 * Aggregation rule definition. Contains the following information:
 * @param metricSuffix suffix for the given aggregation rule
 * @param level  level of the given aggregation rule
 * @param tags include/exclude tags for the given aggregation rule
 */
sealed trait AggRule {
  def ruleId: String
  def metricSuffix: String
  def level: String
  def tags: Set[String]
  def versionEffectiveTime: Long
  def active: Boolean
  def isHigherLevelAggregationApplicable(shardKeyColumns: Set[String], filterTags: Seq[String]): Boolean
  def numExcludedLabels: Int
  def numIncludedLabels: Int
}

/**
 * @param metricSuffix - String - Metric suffix for the given aggregation rule
 * @param tags - Set[String] - Include tags as specified in the aggregation rule
 * @param level - String - level of the aggregation rule
 */
case class IncludeAggRule(ruleId: String, metricSuffix: String, tags: Set[String], versionEffectiveTime: Long,
                          active: Boolean, level: String = "1") extends AggRule {

  /**
   * Checks if the higher level aggregation is applicable with IncludeTags.
   *
   * @param shardKeyColumns - Seq[String] - List of shard key columns. These columns are not part of check. This
   *                        includes tags which are compulsory for the query like _metric_, _ws_, _ns_.
   * @param filterTags      - Seq[String] - List of filter tags/labels in the query or in the aggregation clause
   * @return - Boolean
   */
  override def isHigherLevelAggregationApplicable(shardKeyColumns: Set[String], filterTags: Seq[String]): Boolean = {
    filterTags.forall( tag => shardKeyColumns.contains(tag) || tags.contains(tag))
  }

  override def numIncludedLabels: Int = tags.size
  override def numExcludedLabels: Int = 0
}

/**
 * @param metricSuffix - String - Metric suffix for the given aggregation rule
 * @param tags - Set[String] - Exclude tags as specified in the aggregation rule
 * @param level - String - level of the aggregation rule
 */
case class ExcludeAggRule(ruleId: String, metricSuffix: String, tags: Set[String], versionEffectiveTime: Long,
                          active: Boolean, level: String = "1") extends AggRule {

  /**
   * Checks if the higher level aggregation is applicable with ExcludeTags. Here we need to check if the column filter
   * tags present in query or aggregation clause, should not be a part of ExcludeTags.
   *
   * @param shardKeyColumns - Seq[String] - List of shard key columns. These columns are not part of check. This
   *                        include tags which are compulsory for the query like _metric_, _ws_, _ns_.
   * @param filterTags      - Seq[String] - List of filter tags/labels in the query or in the aggregation clause
   * @return - Boolean
   */
  override def isHigherLevelAggregationApplicable(shardKeyColumns: Set[String], filterTags: Seq[String]): Boolean = {
    filterTags.forall { tag => shardKeyColumns.contains(tag) || (!tags.contains(tag)) }
  }
  override def numIncludedLabels: Int = 0
  override def numExcludedLabels: Int = tags.size
}

object HierarchicalQueryExperience extends StrictLogging {

  // Get the shard key columns from the dataset options along with all the metric labels used
  private[query] lazy val shardKeyColumnsOption: Option[Set[String]] = GlobalConfig.datasetOptions match {
    case Some(datasetOptions) =>
      Some((datasetOptions.shardKeyColumns ++ Seq( datasetOptions.metricColumn, GlobalConfig.PromMetricLabel)).toSet)
    case None => None
  }
}
