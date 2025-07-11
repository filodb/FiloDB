package filodb.coordinator.queryplanner

import filodb.core.query.ColumnFilter
import filodb.prometheus.parse.Parser
import filodb.prometheus.parse.Parser.Antlr
import filodb.query.RangeSelector
import filodb.query.lpopt.AggRuleProvider
import filodb.query.util.{AggRule, ExcludeAggRule}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class AggLpOptimizationSpec extends AnyFunSpec with Matchers {

  private val now = 1634777330000L
  private val endSeconds = now / 1000
  private val step = 300

  def newProvider(aggRules: List[AggRule]): AggRuleProvider = new AggRuleProvider {
    override def getAggRuleVersions(filters: Seq[ColumnFilter], rs: RangeSelector): List[AggRule] = aggRules
    override def aggRuleOptimizationEnabled: Boolean = true
  }

  // Placeholder for unit tests on plan optimization
  // @Sandeep, we can move all the tests from LogicalPlanParser to here to avoid bloating of that spec?

  it("should not optimize when no rules are found") {
    val query = """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s]))"""
    val lp = Parser.queryToLogicalPlan(query, endSeconds, step, Antlr)
    val arp = newProvider(Nil)
    val optimized = lp.useHigherLevelAggregatedMetric(arp)
    LogicalPlanParser.convertToQuery(optimized) shouldEqual query
  }

  private val rules1 = List(
    // Rule1 Level1
    ExcludeAggRule("1", "agg1_1", Set("instance", "pod"), 10, level="1"),

    // Rule1 Level2 and its versions
    ExcludeAggRule("1", "agg1_2", Set("instance", "pod", "container"), 10, level="2"),
    ExcludeAggRule("1", "agg1_2", Set("instance", "pod", "container", "guid"), 12, level="2"),
    ExcludeAggRule("1", "agg1_2", Set("instance", "pod", "container", "fixed_guid"), 13, level="2"),
  )

  private val lpToOptimizedWithExcludes = Seq(
    // picking rule that has necessary labels: should use rule 1 level 1 since container is needed
    """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
      -> """sum(rate(foo:::agg1_1{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",
    """sum(increase(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
      -> """sum(increase(foo:::agg1_1{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",

    //  Gauge queries that need to choose an aggregated column
    // FIXME fails right now
//    """sum(count_over_time(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
//      -> """sum(sum(foo:::agg1_1::count{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",
//    """min(min_over_time(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
//      -> """min(min_over_time(foo:::agg1_1::min{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",
//    """max(max_over_time(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
//      -> """max(max_over_time(foo:::agg1_1::max{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",

    // picking rule with excludes more labels: should use rule 1 level 2 since it excludes more labels
    """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s]))"""
      -> """sum(rate(foo:::agg1_2{_ws_="demo",_ns_="localNs"}[300s]))""",

    // binary join of above 2 test cases
    """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container) + sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s]))"""
      -> """(sum(rate(foo:::agg1_1{_ws_="demo",_ns_="localNs"}[300s])) by (container) + sum(rate(foo:::agg1_2{_ws_="demo",_ns_="localNs"}[300s])))""",

    // selection of column as in average latency on histograms: should use rule 1 level 1 since container is needed
    """sum(rate(foo::sum{_ws_="demo",_ns_="localNs"}[300s])) by (container) / sum(rate(foo::count{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
      -> """(sum(rate(foo:::agg1_1::sum{_ws_="demo",_ns_="localNs"}[300s])) by (container) / sum(rate(foo:::agg1_1::count{_ws_="demo",_ns_="localNs"}[300s])) by (container))""",

    // choose higher level aggregation which excludes more labels: should use higher level aggregation rule 1 level 2 since it excludes more labels
    """sum(rate(foo:::agg1_1{_ws_="demo",_ns_="localNs"}[300s]))"""
      -> """sum(rate(foo:::agg1_2{_ws_="demo",_ns_="localNs"}[300s]))""",

    // choose higher level aggregation which excludes more labels when asking a column: should use rule 1 level 1 since container is needed
    """sum(rate(foo:::agg1_1::sum{_ws_="demo",_ns_="localNs"}[300s])) / sum(rate(foo:::agg1_1::count{_ws_="demo",_ns_="localNs"}[300s]))"""
      -> """(sum(rate(foo:::agg1_2::sum{_ws_="demo",_ns_="localNs"}[300s])) / sum(rate(foo:::agg1_2::count{_ws_="demo",_ns_="localNs"}[300s])))""",

    // nested aggregation: should use rule 1 level 1 for inner aggregate since container is needed
    """min(sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container))"""
      -> """min(sum(rate(foo:::agg1_1{_ws_="demo",_ns_="localNs"}[300s])) by (container))""",

    // group without: should use rule 1 level 2 since container is excluded and most number of labels excluded
    """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) without (container)"""
      -> """sum(rate(foo:::agg1_2{_ws_="demo",_ns_="localNs"}[300s])) without (container)""",

    // group without label that is not excluded: should not optimize since dc is not excluded in any rule
    """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) without (dc)"""
      -> """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) without (dc)""",

  )

  it("should optimize when include rules are found") {
    val arp = newProvider(rules1)
    for { (query, optimizedExpected) <- lpToOptimizedWithExcludes } {
      val lp = Parser.queryToLogicalPlan(query, endSeconds, step, Antlr)
      val optimized = lp.useHigherLevelAggregatedMetric(arp)
      LogicalPlanParser.convertToQuery(optimized) shouldEqual optimizedExpected
    }
  }

  private val lpNoOptimizWithRules = Seq(
    // same
    """no_optimize(sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container))"""
      -> """no_optimize(sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container))""",

    // same
    """no_optimize(sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])))"""
      -> """no_optimize(sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])))""",

    // util adds parens around binary ops - thats all
    """no_optimize(sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container) + sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])))"""
      -> """no_optimize((sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container) + sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s]))))""",
  )

  it("should not optimize when no_optimize is set") {
    val arp = newProvider(rules1)
    for { (query, optimizedExpected) <- lpNoOptimizWithRules } {
      val lp = Parser.queryToLogicalPlan(query, endSeconds, step, Antlr)
      val optimized = lp.useHigherLevelAggregatedMetric(arp)
      LogicalPlanParser.convertToQuery(optimized) shouldEqual optimizedExpected
    }
  }

}
