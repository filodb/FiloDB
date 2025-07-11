package filodb.query.lpopt

import filodb.core.GlobalConfig
import filodb.core.query.ColumnFilter
import filodb.core.query.Filter.Equals
import filodb.query._
import filodb.query.util.{AggRule, ExcludeAggRule, IncludeAggRule}

/**
 * This object contains the logic to optimize Aggregate logical plans to use pre-aggregated metrics
 * if available.
 */
object AggLpOptimization {

  /**
   * Facade method for this utility. Optimizes the given Aggregate logical plan to use a pre-aggregated metric if
   * possible. If pre-aggregated data is being queried, it will try to find a higher level pre-aggregated metric
   * if present.
   *
   * @return Some(optimizedLogicalPlan) if the optimization was possible, None if it cannot be optimized.
   */
  def optimizeWithPreaggregatedDataset(agg: Aggregate, aggRuleProvider: AggRuleProvider): Option[Aggregate] = {
    require(aggRuleProvider.aggRuleOptimizationEnabled,
      "AggRuleProvider must have aggRuleOptimizationEnabled=true to use this optimization")
    val canTranslateResult = canTranslateQueryToPreagg(agg)
    if (canTranslateResult.isDefined) {
      val rules: List[AggRule] = aggRuleProvider.getAggRuleVersions(
                                                      canTranslateResult.get.rs.rawSeriesFilters(),
                                                      canTranslateResult.get.rs.rangeSelector())
      // grouping by level+id results in all versions of the rule as value
      val rulesGroupedByIdLevel = rules.groupBy(r => (r.ruleId, r.level))
      var chosenRule: Option[AggRule] = None

      for {rule <- rulesGroupedByIdLevel} { // iterate to see which id/level the best rule to use
        val ruleIsEligible = rule._2.forall { r =>
          canUseRule(r, canTranslateResult.get.rs.rawSeriesFilters().map(_.column).toSet, agg.clauseOpt)
        }
        if (ruleIsEligible &&
          (chosenRule.isEmpty || firstRuleIsBetterThanSecond(rule._2.head, chosenRule.get))) {
          chosenRule = Some(rule._2.head)
        }
      }

      if (chosenRule.isEmpty) {
        // no rule was chosen, return this plan as is
        None
      } else {
        // rule was chosen, rewrite the logical plan to use the pre-aggregated metric
        val aggRuleSuffix = chosenRule.get.metricSuffix
        val aggMetricName = metricNameString(metricNameWithoutSuffix(canTranslateResult.get.rs.metricName()),
          Some(aggRuleSuffix), None)
        Some(replaceMetricNameInQuery(agg, aggMetricName, canTranslateResult.get.aggColumnToUse.toSeq))
      }
    } else {
      None
    }
  }

  private case class CanTranslateResult(rs: RawSeriesLikePlan, aggColumnToUse: Option[String])

  /**
   * Used to determine if the given Aggregate logical plan can be translated to a pre-aggregated metric.
   *
   * Returns Option(CanTranslateResult) if the query can be translated to a pre-aggregated metric. It contains
   * the underlying RawSeriesLikePlan and the column to use for aggregation. Method returns None if it cannot be
   * translated / optimized.
   */
  private def canTranslateQueryToPreagg(agg: Aggregate): Option[CanTranslateResult] = {
    val ret = agg match {
      case Aggregate(AggregationOperator.Sum,
                    PeriodicSeriesWithWindowing(rs, _, _, _, _, RangeFunctionId.Rate, _, _, _, _),
                    _,
                    _) if rs.columns().toSet.subsetOf(Set("sum", "count")) // rate only on sum and count columns
          => Some(CanTranslateResult(rs, rs.columns().headOption))

      case Aggregate(AggregationOperator.Sum,
                    PeriodicSeriesWithWindowing(rs, _, _, _, _, RangeFunctionId.Increase, _, _, _, _),
                    _,
                    _) if rs.columns().toSet.subsetOf(Set("sum", "count")) // increase only on sum and count columns
          => Some(CanTranslateResult(rs, rs.columns().headOption))

      case Aggregate(AggregationOperator.Sum,
                    PeriodicSeriesWithWindowing(rs, _, _, _, _, RangeFunctionId.SumOverTime, _, _, _, _),
                    _,
                    _) if rs.columns().toSet.subsetOf(Set("sum")) // SumOverTime only on sum column
          => Some(CanTranslateResult(rs, Some("sum")))

// FIXME optimization of query to count number of samples still does not work since it requires replacement of
// range function. Leaving it out for now since it is not a common query. Cross the bridge later when really needed.
//      case Aggregate(AggregationOperator.Sum,
//                    PeriodicSeriesWithWindowing(rs, _, _, _, _, RangeFunctionId.CountOverTime, _, _, _, _),
//                    _,
//                    _) => Some(CanTranslateResult(rs, Some("count")))

      case Aggregate(AggregationOperator.Min,
                    PeriodicSeriesWithWindowing(rs, _, _, _, _, RangeFunctionId.MinOverTime, _, _, _, _),
                    _,
                    _) if rs.columns().toSet.subsetOf(Set("min"))
          => Some(CanTranslateResult(rs, Some("min")))

      case Aggregate(AggregationOperator.Max,
                    PeriodicSeriesWithWindowing(rs, _, _, _, _, RangeFunctionId.MaxOverTime, _, _, _, _),
                    _,
                    _) if rs.columns().toSet.subsetOf(Set("max"))
          => Some(CanTranslateResult(rs, Some("max")))

      case _ => None
    }
    if (ret.isEmpty) {
      None
    } else {
      // if the query column is not the same as the suggested aggregation column, it is a wierd case, so don't optimize
      val queryColumnOpt = ret.get.rs.columns().headOption
      if (queryColumnOpt.isEmpty || queryColumnOpt == ret.get.aggColumnToUse) ret else None
    }
  }

  private def replaceMetricNameInQuery(agg: Aggregate, aggMetricName: String, col: Seq[String]): Aggregate = {
    agg.copy(vectors = agg.vectors.asInstanceOf[PeriodicSeriesWithWindowing].copy(
      series = agg.vectors.asInstanceOf[PeriodicSeriesWithWindowing].series.replaceRawSeriesFiltersAndColumn(
          Seq(ColumnFilter(GlobalConfig.PromMetricLabel, Equals(aggMetricName))), col)))
  }

  private def metricNameWithoutSuffix(metricNameStr: String): String = metricNameStr.split(":::").head

  private def metricNameString(metricName: String, ruleSuffix: Option[String], col: Option[String]): String = {
    s"$metricName${ruleSuffix.map(s => s":::$s").getOrElse("")}${col.map(c => s":$c").getOrElse("")}"
  }

  /**
   * Checks if the given AggRule can be used for the given filter tags and aggregate clause.
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
  private def canUseRule(rule: AggRule, filterTags: Set[String], aggClause: Option[AggregateClause]): Boolean = {
    // Note: We assume that the rule is relevant since it is already filtered by the column filters in the query.
    // and we just need to check if the filter tags and aggregate clause match the rule.
    // TODO do we need to shard key columns ? They will never be dropped by aggregation rules.
    rule match {
      case in: IncludeAggRule =>
        filterTags.subsetOf(in.tags) && // filter tags should be included in the rule
          (aggClause.isEmpty || // no group by or without clause
            (aggClause.get.clauseType == AggregateClause.ClauseType.By &&
              aggClause.get.labels.toSet.subsetOf(in.tags))  // by clause labels are all included in rule
          )
        // Include rules do not work with without clause since wew do not know all the labels that will be dropped
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
