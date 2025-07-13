package filodb.query.lpopt

import filodb.core.query.ColumnFilter
import filodb.query.IntervalSelector
import filodb.query.util.AggRule

trait AggRuleProvider {
  /**
   * Returns all the Aggregation Rules along with all versions that match the given filters and time range selector.
   *
   * Rule's versions should be returned only if all available rule versions' filters match the query column filters.
   *
   * Assumptions client will make:
   * * a namespace is associated with only one suffix at a given time (multiple levels are okay). Rules are
   *   maintained in a way where same suffix is not used for different rules for same time series at the same time
   * * Rules are never hard-deleted, but only deactivated, and can be re-activated later. Deactivation results in
   *   a new version
   *
   * @param filters the filters in the query
   * @param rs the range selector in the query. Typically this will be an IntervalSelector
   * @return list of AggRules that match the filters and range selector
   */
  def getAggRuleVersions(filters: Seq[ColumnFilter], rs: IntervalSelector): List[AggRule]

  def aggRuleOptimizationEnabled: Boolean
}


