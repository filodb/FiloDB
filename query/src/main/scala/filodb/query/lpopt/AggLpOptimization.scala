package filodb.query.lpopt

import scala.concurrent.duration.DurationInt

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon

import filodb.core.GlobalConfig
import filodb.core.metadata.Schemas
import filodb.core.query.ColumnFilter
import filodb.core.query.Filter.Equals
import filodb.query._
import filodb.query.RangeFunctionId.SumOverTime
import filodb.query.util.{AggRule, ExcludeAggRule, HierarchicalQueryExperience, IncludeAggRule}

/**
 * This object contains the logic to optimize Aggregate logical plans to use pre-aggregated metrics
 * if available.
 */
object AggLpOptimization extends StrictLogging{

  // GOTCHA for LP Optimization:
  // If raw data is 10-secondly, and pre-aggregated data is 1-minutely, then query results won't match exactly.
  // We optimize the query anyway since we do not have pre-agg use cases that publish 10s data yet.
  // To support query equivalence, we would need to have pre-aggregated data at same frequency as raw data.
  // If there is a use case that should not optimize for such scenarios prior to pre-agg supporting 10s data,
  // instruct users to the no_optimize promql function to bypass the optimization.

  // configure if needed later
  private val aggDelay  = 1.minutes.toMillis // pre-aggregated data is delayed by 1m

  private val numAggLpOptimized = Kamon.counter(s"num_agg_lps_optimized").withoutTags()
  private val numAggLpNotOptimized = Kamon.counter(s"num_agg_lps_not_optimized")

  /**
   * Facade method for this utility. Optimizes the given Aggregate logical plan to use a pre-aggregated metric if
   * possible. If pre-aggregated data is being queried, it will try to find a higher level pre-aggregated metric
   * if present.
   *
   * @return Some(optimizedLogicalPlan) if the optimization was possible, None if it cannot be optimized.
   */
  def optimizeWithPreaggregatedDataset(agg: Aggregate, aggRuleProvider: AggRuleProvider): Option[Aggregate] = {
    val canTranslateResult = canTranslateQueryToPreagg(agg)
    if (canTranslateResult.isDefined) {
      val rules: List[AggRule] = aggRuleProvider.getAggRuleVersions(
                                              canTranslateResult.get.rawSeriesFilters,
                                              canTranslateResult.get.timeInterval)
      logger.debug(s"Matching agg rules for optimizing query $agg were determined to be $rules")
      // grouping by suffix results in all rule and versions for given suffix
      val rulesBySuffix = rules.groupBy(r => r.metricSuffix)
      var chosenRule: Option[AggRule] = None

      for { rule <- rulesBySuffix } { // iterate to see which suffix is the best to use
        val ruleRetainsLabels = rule._2.forall { r =>
          ruleRetainsNeededLabels(r, canTranslateResult.get.rawSeriesFilters.map(_.column).toSet, agg.clauseOpt)
        }
        val ruleWasNotInactiveDuringQueryRange = rule._2.forall(_.active)
        val rulePresentDuringEntireQueryRange =
          rule._2.map(_.versionEffectiveTime).min <= canTranslateResult.get.timeInterval.from

        if (ruleRetainsLabels && rulePresentDuringEntireQueryRange && ruleWasNotInactiveDuringQueryRange &&
          (chosenRule.isEmpty || firstRuleIsBetterThanSecond(rule._2.last, chosenRule.get))) {
          chosenRule = Some(rule._2.last)
          logger.debug(s"Chose better rule for optimizing query $agg : $chosenRule")
        } else {
          logger.debug(s"Did not choose rule for optimizing query $agg : ${rule._2} " +
            s"ruleRetainsLabels=$ruleRetainsLabels " +
            s"rulePresentDuringEntireQueryRange=$rulePresentDuringEntireQueryRange " +
            s"ruleWasNotInactiveDuringQueryRange=$ruleWasNotInactiveDuringQueryRange")
        }
      }

      if (chosenRule.isEmpty) {
        numAggLpNotOptimized.withTag("reason", "noRules").increment()
        // no rule was chosen, return this plan as is
        None
      } else {
        // rule was chosen, rewrite the logical plan to use the pre-aggregated metric
        val aggRuleSuffix = chosenRule.get.metricSuffix
        val aggMetricName = metricNameString(metricNameWithoutSuffix(canTranslateResult.get.rawSeriesMetricName),
          Some(aggRuleSuffix), None)
        numAggLpOptimized.increment()
        Some(replaceMetricNameInQuery(agg, aggMetricName, canTranslateResult.get.aggColumnToUse.toSeq,
                                      canTranslateResult.get.rangeFunctionToUse))
      }
    } else {
      numAggLpNotOptimized.withTag("reason", "cannotOptimize").increment()
      None
    }
  }

  /**
   * Result of canTranslateQueryToPreagg method. It contains data from the underlying RawSeriesLikePlan
   * plan and data needed to formulate the new aggregated query.
   *
   * @param rawSeriesMetricName The metric name in the underlying RawSeriesLikePlan. This is needed to formulate
   *                            the new aggregated metric name. It may contain suffixes and column specifiers.
   * @param rawSeriesFilters The filters in the underlying RawSeriesLikePlan. This is needed to find the relevant rules
   * @param rawSeriesColumn The column in the underlying RawSeriesLikePlan. This is the column that is being queried
   *                        in the original query issued by user.
   * @param timeInterval return timeInterval from the underlying RawSeriesLikePlan. This is needed to find the relevant
   *                     rules.
   * @param aggColumnToUse Some means replace the column in optimized query; None means use the same column as in query
   * @param rangeFunctionToUse Some means replace the range function in optimized query; None means use the same
   *                           column as in query
   */
  private case class CanTranslateResult(rawSeriesMetricName: String,
                                        rawSeriesFilters: Seq[ColumnFilter],
                                        rawSeriesColumn: Option[String],
                                        timeInterval: IntervalSelector,
                                        aggColumnToUse: Option[String],
                                        rangeFunctionToUse: Option[RangeFunctionId])

  /**
   * Used to determine if the given Aggregate logical plan can be translated to a pre-aggregated metric.
   *
   * Returns Option(CanTranslateResult) if the query can be translated to a pre-aggregated metric. It contains data
   * from the underlying RawSeriesLikePlan and the column to use for aggregation. Method returns None if it cannot be
   * translated / optimized.
   */
  // scalastyle:off method.length cyclomatic.complexity
  private def canTranslateQueryToPreagg(agg: Aggregate): Option[CanTranslateResult] = {

    // If this is an instant query for latest minute, do not optimize since pre-aggregated data may not be ready yet
    val nowMinus1m = System.currentTimeMillis() - aggDelay
    if (agg.endMs > nowMinus1m && agg.startMs > nowMinus1m) {
      return None
    }

    def makeResult(rs: RawSeriesLikePlan, colOpt: Option[String],
                   rangeFunctionId: Option[RangeFunctionId]): Option[CanTranslateResult] = {
      val metricName = rs.metricName()
      rs.rangeSelector() match {
        case is: IntervalSelector => Some(CanTranslateResult(metricName, rs.rawSeriesFilters(),
          rs.columns().headOption, is, colOpt, rangeFunctionId))
        case _ => None
      }
    }

    val ret = agg match {
      // sum(rate(foo))
      // sum(rate(foo::sum))
      // sum(rate(foo::count))
      // sum(rate(foo:::agg::count))
      // sum(rate(foo:::agg::sum))
      case Aggregate(AggregationOperator.Sum,
                    PeriodicSeriesWithWindowing(rs, _, _, _, window, RangeFunctionId.Rate, _, _, _, _),
                    _,
                    _) if rs.columns().toSet.subsetOf(Set("sum", "count"))
          => makeResult(rs, rs.columns().headOption, None)

      // sum(increase(foo))
      // sum(increase(foo::sum))
      // sum(increase(foo::count))
      // sum(increase(foo:::agg::count))
      // sum(increase(foo:::agg::sum))
      case Aggregate(AggregationOperator.Sum,
                    PeriodicSeriesWithWindowing(rs, _, _, _, _, RangeFunctionId.Increase, _, _, _, _),
                    _,
                    _) if rs.columns().toSet.subsetOf(Set("sum", "count"))
          => makeResult(rs, rs.columns().headOption, None)

      // sum(sum_over_time(foo))
      // sum(sum_over_time(foo:::agg::sum))
      // sum(sum_over_time(foo:::agg::count))
      case Aggregate(AggregationOperator.Sum,
                    PeriodicSeriesWithWindowing(rs, _, _, _, _, RangeFunctionId.SumOverTime, _, _, _, _),
                    _,
                    _) if rs.columns().toSet.subsetOf(Set("sum", "count"))
          => makeResult(rs, rs.columns().headOption.orElse(Some("sum")), None)

      // sum(count_over_time(foo))
      case Aggregate(AggregationOperator.Sum,
                    PeriodicSeriesWithWindowing(rs, _, _, _, _, RangeFunctionId.CountOverTime, _, _, _, _),
                    _,
                    _) if rs.columns().isEmpty
          => makeResult(rs, Some("count"), Some(SumOverTime))

      // min(min_over_time(foo))
      // min(min_over_time(foo:::agg::min))
      case Aggregate(AggregationOperator.Min,
                    PeriodicSeriesWithWindowing(rs, _, _, _, _, RangeFunctionId.MinOverTime, _, _, _, _),
                    _,
                    _) if rs.columns().toSet.subsetOf(Set("min"))
          => makeResult(rs, Some("min"), None)

      // max(max_over_time(foo))
      // max(max_over_time(foo:::agg::max))
      case Aggregate(AggregationOperator.Max,
                    PeriodicSeriesWithWindowing(rs, _, _, _, _, RangeFunctionId.MaxOverTime, _, _, _, _),
                    _,
                    _) if rs.columns().toSet.subsetOf(Set("max"))
          => makeResult(rs, Some("max"), None)

      /*
         We could optimize queries like sum(foo), max(foo), min(foo), count(foo) by looking back [1m] and optimizing
         with sum(last_over_time(foo:::agg::sum[1m])) etc with aggregated time series. However, aggregation would
         happen for all samples arriving in the 1m window, and not strictly using the last sample of each series.
         The time series could be publishing at a higher frequency than 1m, and thus the aggregation would be over more
         samples. Thus, the query results may be incorrect and not match with raw if we optimize queries this way.

         In such cases where publish interval is 1m, and optimization is a must-have, users can use optimized queries
         that approximate. Know that this is not 100% reliable because publish interval is not exactly 1m, and there
         could be jitter. But it should be close enough.

         Examples of such queries that can be approximately optimized when publish interval is 1m:
              sum(sum_over_time(foo[1m])) is an optimizable approximation of sum(foo)
              max(max_over_time(foo[1m])) is an optimizable approximation of max(foo)
              min(min_over_time(foo[1m])) is an optimizable approximation of min(foo)
              sum(count_over_time(foo[1m])) is an optimizable approximation of count(foo)
       */

      case _ => None
    }
    if (ret.isEmpty) {
      None
    } else {
      // if the query column is not the same as the suggested aggregation column, it is a wierd case, so don't optimize
      val queryColumnOpt = ret.get.rawSeriesColumn
      val typeFilterCanBeTranslated = ret.get.rawSeriesFilters.find(_.column == Schemas.TypeLabel) match {
        case Some(ColumnFilter(_, Equals(value))) =>
          // only optimize if the type filter is querying pre-aggregated schema
          Schemas.preAggSchema.contains(value.toString)
        case Some(_) => false // if there are other operators on type filter, cannot optimize
        case None => true // can optimize if there is no type filter
      }
      if ((queryColumnOpt.isEmpty || queryColumnOpt == ret.get.aggColumnToUse) && typeFilterCanBeTranslated) ret
      else None
    }
  }

  private def replaceMetricNameInQuery(agg: Aggregate, aggMetricName: String,
                                       col: Seq[String], rf: Option[RangeFunctionId]): Aggregate = {
    agg.copy(vectors = agg.vectors.asInstanceOf[PeriodicSeriesWithWindowing]
       .updateRawSeriesForAggOptimize(Seq(ColumnFilter(GlobalConfig.PromMetricLabel, Equals(aggMetricName))), col, rf))
  }

  private def metricNameWithoutSuffix(metricNameStr: String): String = metricNameStr.split(":::").head

  private def metricNameString(metricName: String, ruleSuffix: Option[String], col: Option[String]): String = {
    s"$metricName${ruleSuffix.map(s => s":::$s").getOrElse("")}${col.map(c => s":$c").getOrElse("")}"
  }

  private lazy val shardKeys = HierarchicalQueryExperience.shardKeyColumnsOption.toSeq.flatten
  /**
   * Checks if the given AggRule retains labels needed for the given filter tags and aggregate clause.
   *
   * {{{
   * -------------------------------------------------------------------------------------
   * | RuleType |   Group By                      | Group Without                        |
   * -------------------------------------------------------------------------------------
   * | Include  | byTags subset of includeTags    |  false                               |
   * | Exclude  | byTags disjointWith excludeTags |  withoutTags subsetOf excludeTags    |
   * -------------------------------------------------------------------------------------
   * }}}
   */
  private def ruleRetainsNeededLabels(rule: AggRule, filterTags: Set[String],
                                      aggClause: Option[AggregateClause]): Boolean = {
    // Note: We assume that the rule is relevant since it is already filtered by the column filters in the query.
    // and we just need to check if the filter tags and aggregate clause match the rule.
    rule match {
      case in: IncludeAggRule =>
        (filterTags -- shardKeys).subsetOf(in.tags) && // filter tags should be included in the rule
          (aggClause.isEmpty || // no group by or without clause
            (aggClause.get.clauseType == AggregateClause.ClauseType.By &&
              aggClause.get.labels.toSet.subsetOf(in.tags))  // by clause labels are all included in rule
          )
        // Include rules do not work with without clause since we do not know all the labels that will be dropped
      case ex: ExcludeAggRule =>
        filterTags.intersect(ex.tags).isEmpty && // none of the filter tags should be excluded
          (aggClause.isEmpty || // no group by or without clause
            (aggClause.get.clauseType == AggregateClause.ClauseType.By &&
              aggClause.get.labels.toSet.intersect(ex.tags).isEmpty) || // none of by clause labels are excluded in rule
            (aggClause.get.clauseType == AggregateClause.ClauseType.Without &&
              aggClause.get.labels.toSet.subsetOf(ex.tags)) // all without labels are excluded in rule
            )
    }
  }

  /**
   * Compares two AggRules and returns true if the first rule is better than the second.
   * A rule is considered better if it has more excluded labels or less included labels.
   */
  private def firstRuleIsBetterThanSecond(first: AggRule, second: AggRule): Boolean = {
    hasMoreExcludedLabels(first, second) || hasFewerIncludedLabels(first, second)
  }

  /**
   * Checks if the first rule has more excluded labels than the second rule.
   * Both rules must have excluded labels for this comparison to be valid.
   */
  private def hasMoreExcludedLabels(first: AggRule, second: AggRule): Boolean = {
    first.numExcludedLabels > 0 && second.numExcludedLabels > 0 &&
      first.numExcludedLabels > second.numExcludedLabels
  }

  /**
   * Checks if the first rule has fewer included labels than the second rule.
   * Both rules must have included labels for this comparison to be valid.
   */
  private def hasFewerIncludedLabels(first: AggRule, second: AggRule): Boolean = {
    first.numIncludedLabels > 0 && second.numIncludedLabels > 0 &&
      first.numIncludedLabels < second.numIncludedLabels
  }
}
