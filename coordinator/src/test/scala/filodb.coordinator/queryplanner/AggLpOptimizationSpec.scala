package filodb.coordinator.queryplanner

import filodb.core.query.ColumnFilter
import filodb.prometheus.parse.Parser
import filodb.prometheus.parse.Parser.Antlr
import filodb.query.RangeSelector
import filodb.query.lpopt.AggRuleProvider
import filodb.query.util.{AggRule, ExcludeAggRule, IncludeAggRule}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class AggLpOptimizationSpec extends AnyFunSpec with Matchers {

  // these time values don't matter much. We assume AggRuleProvider (mocked in these tests)
  // will return the right rules and versions for the query time range
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

  private val excludeRules1 = List(
    // Rule1 Level1
    ExcludeAggRule("1", "agg1_1", Set("instance", "pod"), 10, level="1"),

    // Rule1 Level2 and its versions
    ExcludeAggRule("1", "agg1_2", Set("instance", "pod", "container"), 10, level="2"),
    ExcludeAggRule("1", "agg1_2", Set("instance", "pod", "container", "guid"), 12, level="2"),
    ExcludeAggRule("1", "agg1_2", Set("instance", "pod", "container", "port"), 13, level="2"),
  )

  it ("[exclude rules] should pick rule that has necessary labels") {
    val testCases = Seq(
      // should use rule 1 level 1 since container is needed
      """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
        -> """sum(rate(foo:::agg1_1{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",
      """sum(increase(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
        -> """sum(increase(foo:::agg1_1{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",
    )
    testOptimization(excludeRules1, testCases)
  }

  it ("[exclude rules] should optimize by picking rule with excludes more labels") {
    val testCases = Seq(
      // should use rule 1 level 2 since it excludes more labels
      """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s]))"""
        -> """sum(rate(foo:::agg1_2{_ws_="demo",_ns_="localNs"}[300s]))""",
    )
    testOptimization(excludeRules1, testCases)
  }

  it ("[exclude rules] should optimize simple binary join") {
    val testCases = Seq(
      """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container) + sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s]))"""
        -> """(sum(rate(foo:::agg1_1{_ws_="demo",_ns_="localNs"}[300s])) by (container) + sum(rate(foo:::agg1_2{_ws_="demo",_ns_="localNs"}[300s])))""",
    )
    testOptimization(excludeRules1, testCases)
  }

  it ("[exclude rules] should optimize queries with selection of column as in average latency on histograms") {
    val testCases = Seq(
      // should use rule 1 level 1 since container is needed
      """sum(rate(foo::sum{_ws_="demo",_ns_="localNs"}[300s])) by (container) / sum(rate(foo::count{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
        -> """(sum(rate(foo:::agg1_1::sum{_ws_="demo",_ns_="localNs"}[300s])) by (container) / sum(rate(foo:::agg1_1::count{_ws_="demo",_ns_="localNs"}[300s])) by (container))""",
    )
    testOptimization(excludeRules1, testCases)
  }

  it ("[exclude rules] should choose higher level aggregation which excludes more labels") {
    val testCases = Seq(
      //  should use higher level aggregation rule 1 level 2 since it excludes more labels
      """sum(rate(foo:::agg1_1{_ws_="demo",_ns_="localNs"}[300s]))"""
        -> """sum(rate(foo:::agg1_2{_ws_="demo",_ns_="localNs"}[300s]))""",
    )
    testOptimization(excludeRules1, testCases)
  }

  it ("[exclude rules] should choose higher level aggregation which excludes more labels when asking a column") {
    val testCases = Seq(
      // should use rule 1 level 1 since container is needed
      """sum(rate(foo:::agg1_1::sum{_ws_="demo",_ns_="localNs"}[300s])) / sum(rate(foo:::agg1_1::count{_ws_="demo",_ns_="localNs"}[300s]))"""
        -> """(sum(rate(foo:::agg1_2::sum{_ws_="demo",_ns_="localNs"}[300s])) / sum(rate(foo:::agg1_2::count{_ws_="demo",_ns_="localNs"}[300s])))""",
    )
    testOptimization(excludeRules1, testCases)
  }

  it ("[exclude rules] should optimize nested aggregation") {
    val testCases = Seq(
      // should use rule 1 level 1 for inner aggregate since container is needed
      """min(sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container))"""
        -> """min(sum(rate(foo:::agg1_1{_ws_="demo",_ns_="localNs"}[300s])) by (container))""",
    )
    testOptimization(excludeRules1, testCases)
  }

  it ("[exclude rules] should optimize queries with group without") {
    val testCases = Seq(
      // should use rule 1 level 2 since container is excluded and most number of labels excluded
      """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) without (container)"""
        -> """sum(rate(foo:::agg1_2{_ws_="demo",_ns_="localNs"}[300s])) without (container)""",
    )
    testOptimization(excludeRules1, testCases)
  }

  it ("[exclude rules] should not optimize when one of the rule versions does not have the required labels") {
    val rules = excludeRules1.drop(1) // use the second rule only which has versions
    val testCases = Seq(
      // not all the rules have guid
      """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) without (guid)"""
        -> """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) without (guid)""",
      """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (guid)"""
        -> """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (guid)""",
    )
    testOptimization(rules, testCases)
  }

  it("[exclude rules] should not optimize (later feature) gauge queries that need to change the range function and aggregated column") {
    val testCases = Seq(
          // This is a known feature gap to be closed later if needed
          """sum(count_over_time(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
              -> """sum(count_over_time(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
//   correct expected output when feature is done -> """sum(sum(foo:::agg1_1::count{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",
    )
    testOptimization(excludeRules1, testCases)
  }

  it("[exclude rules] should optimize Raw Gauge queries that need to choose an aggregated column") {
    val testCases = Seq(
      """min(min_over_time(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
        -> """min(min_over_time(foo:::agg1_1::min{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",
      """max(max_over_time(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
        -> """max(max_over_time(foo:::agg1_1::max{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",
      """sum(sum_over_time(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
        -> """sum(sum_over_time(foo:::agg1_1::sum{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",
    )
    testOptimization(excludeRules1, testCases)
  }

  it("[exclude rules] should not optimize with group without label that is not excluded") {
    // should not optimize since dc is not excluded in any rule
    val testCases = Seq(
      """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) without (dc)"""
        -> """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) without (dc)""",
    )
    testOptimization(excludeRules1, testCases)
  }

  it("[exclude rules] should optimize agg Gauge queries that can use higher level aggregated rule") {
    val testCases = Seq(
      """min(min_over_time(foo{_ws_="demo",_ns_="localNs"}[300s]))"""
        -> """min(min_over_time(foo:::agg1_2::min{_ws_="demo",_ns_="localNs"}[300s]))""",
      """max(max_over_time(foo{_ws_="demo",_ns_="localNs"}[300s]))"""
        -> """max(max_over_time(foo:::agg1_2::max{_ws_="demo",_ns_="localNs"}[300s]))""",
      """sum(sum_over_time(foo{_ws_="demo",_ns_="localNs"}[300s]))"""
        -> """sum(sum_over_time(foo:::agg1_2::sum{_ws_="demo",_ns_="localNs"}[300s]))""",
    )
    testOptimization(excludeRules1, testCases)
  }

  it("[exclude rules] should not optimize wierd cases where query already has a column that is not the right aggregation column") {
    val testCases = Seq(
      """max(max_over_time(foo::min{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
        -> """max(max_over_time(foo::min{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",
      """sum(rate(foo::min{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
        -> """sum(rate(foo::min{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",
      """sum(rate(foo:::agg1_1::min{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
        -> """sum(rate(foo:::agg1_1::min{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",
      """min(rate(foo:::agg1_1::sum{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
        -> """min(rate(foo:::agg1_1::sum{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",
    )
    testOptimization(excludeRules1, testCases)
  }

  private val includeRules1 = List(
    // Rule1 Level1
    IncludeAggRule("1", "agg1_1", Set("dc", "service"), 10, level="1"),

    // Rule1 Level2 and its versions
    IncludeAggRule("1", "agg1_2", Set("dc", "service", "region"), 10, level="2"),
    IncludeAggRule("1", "agg1_2", Set("dc", "service", "region", "guid"), 12, level="2"),
    IncludeAggRule("1", "agg1_2", Set("dc", "service", "region", "port"), 13, level="2"),
  )

  it ("[include rules] should pick rule that has necessary labels") {
    val testCases = Seq(
      // should use rule 1 level 2 since region is needed
      """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (region)"""
        -> """sum(rate(foo:::agg1_2{_ws_="demo",_ns_="localNs"}[300s])) by (region)""",
      """sum(increase(foo{_ws_="demo",_ns_="localNs"}[300s])) by (region)"""
        -> """sum(increase(foo:::agg1_2{_ws_="demo",_ns_="localNs"}[300s])) by (region)""",
    )
    testOptimization(includeRules1, testCases)
  }

  it ("[include rules] should optimize by picking rule with includes fewer labels") {
    val testCases = Seq(
      // should use rule 1 level 1 since it excludes more labels
      """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s]))"""
        -> """sum(rate(foo:::agg1_1{_ws_="demo",_ns_="localNs"}[300s]))""",
    )
    testOptimization(includeRules1, testCases)
  }

  it ("[include rules] should not optimize when one of the rule versions does not have the required labels") {
    val rules = excludeRules1.drop(1) // use the second rule only which has versions
    val testCases = Seq(
      // not all the rules have guid
      """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (guid)"""
        -> """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (guid)""",
    )
    testOptimization(rules, testCases)
  }

  it ("[include rules] should not optimize when without clause is used") {
    val testCases = Seq(
      """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) without (port)"""
        -> """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) without (port)""",
    )
    testOptimization(includeRules1, testCases)
  }

  private def testOptimization(rules: List[AggRule], testCases: Seq[(String, String)]): Unit = {
    val arp = newProvider(rules)
    for {(query, optimizedExpected) <- testCases} {
      println(s"Testing query $query")
      val lp = Parser.queryToLogicalPlan(query, endSeconds, step, Antlr)
      val optimized = lp.useHigherLevelAggregatedMetric(arp)
      LogicalPlanParser.convertToQuery(optimized) shouldEqual optimizedExpected
    }
  }

  it("should not optimize when no_optimize function is set") {
    val testCases = Seq(
      // same
      """no_optimize(sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container))"""
        -> """no_optimize(sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container))""",

      // same
      """no_optimize(sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])))"""
        -> """no_optimize(sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])))""",

      // same
      """no_optimize(histogram_quantile(0.9,sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s]))))"""
        -> """no_optimize(histogram_quantile(0.9,sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s]))))""",

      // util adds parens around binary ops - thats all
      """no_optimize(sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container) + sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])))"""
        -> """no_optimize((sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container) + sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s]))))""",
    )

    val arp = newProvider(excludeRules1)
    for { (query, optimizedExpected) <- testCases } {
      val lp = Parser.queryToLogicalPlan(query, endSeconds, step, Antlr)
      val optimized = lp.useHigherLevelAggregatedMetric(arp)
      LogicalPlanParser.convertToQuery(optimized) shouldEqual optimizedExpected
    }
  }

}
