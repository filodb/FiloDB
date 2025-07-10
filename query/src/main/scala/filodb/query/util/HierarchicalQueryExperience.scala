package filodb.query.util

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import scala.jdk.CollectionConverters.asScalaBufferConverter

import filodb.core.GlobalConfig
import filodb.core.query.ColumnFilter
import filodb.core.query.Filter.{Equals, EqualsRegex}
import filodb.query.{AggregateClause, AggregationOperator, LogicalPlan, TsCardinalities}
import filodb.query.MiscellaneousFunctionId.OptimizeWithAgg

/**
 * Aggregation rule definition. Contains the following information:
 *
 * @param metricDelimiter metricDelimiter to be matched and used to create the raw to agg metric
 * @param aggRulesByAggregationSuffix map of:
 *                 Current aggregation metric suffix -> Set of "NEXT" level aggregation's rules that can be matched
 *                 Example - {
 *                                "agg1": Set(
 *                                      AggRule{ metricSuffix = agg_2, tags = Set("tag1", "tag2")},
 *                                      AggRule{ metricSuffix = agg_3, tags = Set("tag2")},
 *                                  )
 *                           }
 * @param aggRulesByRawMetricName map of:
 *                 metric name -> Set of aggregation rules to be tested against the raw metric label filters.
 *                 WHY is this separate map needed and why are we grouping by metric name ?
 *                    A single promql query can have several metrics. Not all of them would qualify for raw to agg
 *                    translation. Hence, we store the metrics which are eligible for the same in this map to avoid
 *                    accidental updates.
 *                 Example - {
 *                               "metric_name1": Set(
 *                                     AggRule{ metricSuffix = agg,   tags = Set("tag1", "tag2", "tag3")},
 *                                     AggRule{ metricSuffix = agg_2, tags = Set("tag1", "tag2")},
 *                                     AggRule{ metricSuffix = agg_3, tags = Set("tag2")},
 *                                 )
 *                          }
 */
case class HierarchicalQueryExperienceParams(metricDelimiter: String,
                                             aggRulesByAggregationSuffix: Map[String, Set[AggRule]],
                                             aggRulesByRawMetricName: Map[String, Set[AggRule]] = Map.empty) { }

/**
 * Aggregation rule definition. Contains the following information:
 * @param metricSuffix suffix for the given aggregation rule
 * @param level  level of the given aggregation rule
 * @param tags include/exclude tags for the given aggregation rule
 */
sealed trait AggRule {
  val ruleId: String
  val metricSuffix: String
  val level: String
  val tags: Set[String]
  val versionEffectiveTime: Long
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
                          level: String = "1") extends AggRule {

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
                          level: String = "1") extends AggRule {

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

  private val hierarchicalQueryOptimizedCounter = Kamon.counter("hierarchical-query-plans-optimized")

  // Get the shard key columns from the dataset options along with all the metric labels used
  private lazy val shardKeyColumnsOption: Option[Set[String]] = GlobalConfig.datasetOptions match {
    case Some(datasetOptions) =>
      Some((datasetOptions.shardKeyColumns ++ Seq( datasetOptions.metricColumn, GlobalConfig.PromMetricLabel)).toSet)
    case None => None
  }

  // Get the allowed aggregation operators from the hierarchical config
  private lazy val allowedAggregationOperators: Option[Set[String]] = GlobalConfig.hierarchicalConfig match {
    case Some(hierarchicalConfig) =>
      Some(hierarchicalConfig.getStringList("allowed-aggregation-operators").asScala.toSet)
    case None => None
  }

  // Get the allowed range functions from the hierarchical config
  private lazy val allowedRangeFunctions: Option[Set[String]] = GlobalConfig.hierarchicalConfig match {
    case Some(hierarchicalConfig) =>
      Some(hierarchicalConfig.getStringList("allowed-range-functions").asScala.toSet)
    case None => None
  }

  // Get the allowed periodic series plans which have access to RawSeries from the hierarchical config
  private lazy val allowedPeriodicSeriesPlansWithRawSeries: Option[Set[String]] =
    GlobalConfig.hierarchicalConfig match {
    case Some(hierarchicalConfig) =>
      Some(hierarchicalConfig.getStringList("allowed-periodic-series-plans-with-raw-series").asScala.toSet)
    case None => None
  }

  // Get the allowed parent logical plans for optimization from the hierarchical config
  private lazy val allowedLogicalPlansForOptimization: Option[Set[String]] = GlobalConfig.hierarchicalConfig match {
    case Some(hierarchicalConfig) =>
      Some(hierarchicalConfig.getStringList("allowed-parent-logical-plans").asScala.toSet)
    case None => None
  }

  lazy val logicalPlanForRawToAggMetric = "ApplyMiscellaneousFunction-" + OptimizeWithAgg.entryName

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
   * @param aggRule - AggRule - Include or Exclude AggRule
   * @param filterTags - Seq[String] - List of filter tags/labels in the query or in the aggregation clause
   * @return - Boolean
   */
  def isHigherLevelAggregationApplicable(aggRule: AggRule,
                                         filterTags: Seq[String]): Boolean = {
    shardKeyColumnsOption match {
      case None =>
        logger.info("[HierarchicalQueryExperience] Dataset options config not found. Skipping optimization !")
        false
      case Some(shardKeyColumns) =>
        aggRule.isHigherLevelAggregationApplicable(shardKeyColumns, filterTags)
    }
  }

  /** Returns the next level aggregated metric name. Example
   *  metricDelimiter = :::
   *  metricSuffix = agg_2
   *  Existing metric name - metric1:::agg
   *  After update - metric1:::agg -> metric1:::agg_2
   * @param metricName - String - Metric ColumnFilter tag/label
   * @param metricDelimiter - HierarchicalQueryExperience - Contains
   * @param metricSuffix - Seq[ColumnFilter] - label filters of the query/lp
   * @return - Option[String] - Next level aggregated metric name
   */
  def getNextLevelAggregatedMetricName(metricName : String, metricDelimiter: String, metricSuffix: String): String = {
    metricName.replaceFirst(metricDelimiter + ".*", metricDelimiter + metricSuffix)
  }

  /**
   * @param metricName raw metric name
   * @param metricDelimiter metric delimiter pattern for aggregated metric
   * @param metricSuffix suffix for the given aggregation rule
   * @return the updated agregated metric name
   */
  def getAggMetricNameForRawMetric(metricName : String, metricDelimiter: String, metricSuffix: String): String = {
    metricName + metricDelimiter + metricSuffix
  }

  /** Gets the current metric name from the given metricColumnFilter and filters
   *
   * @param metricColumnFilter - String - Metric ColumnFilter tag/label
   * @param filters            - Seq[ColumnFilter] - label filters of the query/lp
   * @return - Option[String] - Next level aggregated metric name
   */
  def getMetricName(metricColumnFilter: String, filters: Seq[ColumnFilter]): Option[String] = {
    // Get the metric name from the filters
    val metricNameSeq = LogicalPlan.getColumnValues(filters, metricColumnFilter)
    metricNameSeq match {
      case Seq() => None
      case _ => Some(metricNameSeq.head)
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
   * @param filters - Seq[ColumnFilter] - label filters of the query/lp
   * @return - Seq[String] - List of filter tags/labels after filtering out the .* regex. We want to optimize the
   *         aggregation queries which have .* regex in the filters as they are selecting all the relevant time-series
   *         and hence not applicable for filter check in the aggregation rules.
   */
  def getColumnsAfterFilteringOutDotStarRegexFilters(filters: Seq[ColumnFilter]): Seq[String] = {
    filters.filter {
      case ColumnFilter(_, EqualsRegex(value)) if value.toString == ".*" => false
      case _ => true
    }.map(x => x.column)
  }

  /**
   * Updates the metric column filter if higher level aggregation is applicable. Two scenarios:
   *  1. If the metric is aggregated metric - uses HierarchicalQueryExperienceParams.aggRulesByAggregationSuffix
   *  2. If the metric is raw metric - uses HierarchicalQueryExperienceParams.aggRulesByRawMetricName map
   * @param params - HierarchicalQueryExperienceParams
   *               Contains metricDelimiter, aggRulesByAggregationSuffix, and aggRulesByRawMetricName
   * @param filters - Seq[ColumnFilter] - label filters of the query/lp
   * @param isOptimizeWithAggLp - Boolean - Flag to check if the query is using `optimize_with_agg` function
   * @return - Seq[ColumnFilter] - Updated filters
   */
  def upsertMetricColumnFilterIfHigherLevelAggregationApplicable(params: HierarchicalQueryExperienceParams,
                                                                 filters: Seq[ColumnFilter],
                                                                 isOptimizeWithAggLp: Boolean): Seq[ColumnFilter] = {
    val filterTags = getColumnsAfterFilteringOutDotStarRegexFilters(filters)
    val metricColumnFilter = getMetricColumnFilterTag(filterTags, GlobalConfig.datasetOptions.get.metricColumn)
    val currentMetricName = getMetricName(metricColumnFilter, filters)
    if (currentMetricName.isDefined) {
      // Check if the metric name is part of the params.aggRulesByRawMetricName ( i.e. check if it is raw metric)
      if (isOptimizeWithAggLp && params.aggRulesByRawMetricName.contains(currentMetricName.get)) {
        // CASE 1: Check if the given raw metric can be optimized using an aggregated rule
        val matchingRules = params.aggRulesByRawMetricName(currentMetricName.get)
          .filter(x => isHigherLevelAggregationApplicable(x, filterTags))
        if (matchingRules.nonEmpty) {
          val highestLevelAggRule = matchingRules.maxBy(x => x.level)
          val updatedMetricName = getAggMetricNameForRawMetric(
            currentMetricName.get, params.metricDelimiter, highestLevelAggRule.metricSuffix)
          val updatedFilters = upsertFilters(filters, Seq(ColumnFilter(metricColumnFilter, Equals(updatedMetricName))))
          logger.info(s"[HierarchicalQueryExperience] Query optimized with filters: ${updatedFilters.toString()}")
          incrementHierarchicalQueryOptimizedCounter(updatedFilters, true)
          updatedFilters
        } else {
          filters
        }
      } else {
        // CASE 2: Check if the given aggregated metric can be optimized using the NEXT level aggregation rules
        params.aggRulesByAggregationSuffix.find(x => currentMetricName.get.endsWith(x._1)) match {
          case Some(aggRulesSet) =>
            val matchingRules = aggRulesSet._2.filter( x => isHigherLevelAggregationApplicable(x, filterTags))
            if (matchingRules.nonEmpty) {
              val highestLevelAggRule = matchingRules.maxBy(x => x.level)
              val updatedMetricName = getNextLevelAggregatedMetricName(
                currentMetricName.get, params.metricDelimiter, highestLevelAggRule.metricSuffix)
              val updatedFilters = upsertFilters(
                filters, Seq(ColumnFilter(metricColumnFilter, Equals(updatedMetricName))))
              logger.info(s"[HierarchicalQueryExperience] Query optimized with filters: ${updatedFilters.toString()}")
              incrementHierarchicalQueryOptimizedCounter(updatedFilters, false)
              updatedFilters
            } else {
              filters
            }
          case None => filters
        }
      }
    } else {
      filters
    }
  }

  /**
   * Track the queries optimized by workspace and namespace
   * @param filters - Seq[ColumnFilter] - label filters of the query/lp
   * @param optimizingRawMetric - Boolean - flag signifying if the metric getting optimized is a raw or agg metric
   */
  private def incrementHierarchicalQueryOptimizedCounter(filters: Seq[ColumnFilter],
                                                         optimizingRawMetric: Boolean): Unit = {
    // track query optimized per workspace and namespace in the counter
    val metric_ws = LogicalPlan.getColumnValues(filters, TsCardinalities.LABEL_WORKSPACE) match {
      case Seq() => ""
      case ws => ws.head
    }
    val metric_ns = LogicalPlan.getColumnValues(filters, TsCardinalities.LABEL_NAMESPACE) match {
      case Seq() => ""
      case ns => ns.head
    }
    val metric_type = optimizingRawMetric match {
      case true => "raw"
      case false => "agg"
    }
    hierarchicalQueryOptimizedCounter
      .withTag("metric_ws", metric_ws)
      .withTag("metric_ns", metric_ns)
      .withTag("metric_type", metric_type)
      .increment()
  }

  /**
   * Helper function to check the following:
   * Check 1: Check if the aggregation operator is enabled
   * Check 2: Check if the `by` and `without` clause labels satisfy the include/exclude tag constraints
   *
   * @param params   higher level aggregation rule
   * @param operator Aggregation operator like sum, avg, min, max, count
   * @param clauseOpt AggregateClause - by or without clause
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
                  // can't be optimized further as by labels not present in the higher level metric include tags
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
                  case IncludeAggRule( _, _, _, _, _) =>
                    // can't optimize this scenario as we don't have the required info at the planning stage
                    false
                  case ExcludeAggRule(_, _, excludeTags, _, _) =>
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
