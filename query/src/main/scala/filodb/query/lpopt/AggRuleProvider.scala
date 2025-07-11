package filodb.query.lpopt

import filodb.core.query.ColumnFilter
import filodb.query.RangeSelector
import filodb.query.util.AggRule

trait AggRuleProvider {
  /**
   * Returns all the Aggregation Rules along with all versions that match the given filters and time range selector.
   *
   * Rule's versions should be returned only if all available rule versions' filters match the query column filters.
   *
   * @param filters the filters in the query
   * @param rs the range selector in the query
   * @return list of AggRules that match the filters and range selector
   */
  def getAggRuleVersions(filters: Seq[ColumnFilter], rs: RangeSelector): List[AggRule]

  def aggRuleOptimizationEnabled: Boolean
}


