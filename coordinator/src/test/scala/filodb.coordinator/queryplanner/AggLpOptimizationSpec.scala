package filodb.coordinator.queryplanner

import filodb.core.query.ColumnFilter
import filodb.prometheus.parse.Parser
import filodb.prometheus.parse.Parser.Antlr
import filodb.query.IntervalSelector
import filodb.query.lpopt.AggRuleProvider
import filodb.query.util.{AggRule, ExcludeAggRule, IncludeAggRule}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.prometheus.ast.TimeStepParams

class AggLpOptimizationSpec extends AnyFunSpec with Matchers {

  // these time values don't matter much. We assume AggRuleProvider (mocked in these tests)
  // will return the right rules and versions for the query time range
  private val endSeconds = 30000L
  private val startSeconds = 15000L
  private val step = 2

  def newProvider(aggRules: List[AggRule], enabled: Boolean = true): AggRuleProvider = new AggRuleProvider {
    override def getAggRuleVersions(filters: Seq[ColumnFilter], rs: IntervalSelector): List[AggRule] = aggRules
    override def aggRuleOptimizationEnabled(filters: Seq[ColumnFilter]): Boolean = enabled
  }

  private val excludeRules1 = List(
    // Rule1 Level1
    ExcludeAggRule("1", "agg1_1", Set("instance", "pod"), 10000000L, active = true, level="1"),

    // Rule1 Level2 and its versions
    ExcludeAggRule("1", "agg1_2", Set("instance", "pod", "container"), 10000000L, active = true, level="2"),
    ExcludeAggRule("1", "agg1_2", Set("instance", "pod", "container", "guid"), 16000000L, active = true, level="2"),
    ExcludeAggRule("1", "agg1_2", Set("instance", "pod", "container", "port"), 17000000L, active = true, level="2"),
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

  it ("[exclude rules] should not optimize if window less than 60s") {
    val testCases = Seq(
      // should use rule 1 level 1 since container is needed
      """sum(rate(foo{_ws_="demo",_ns_="localNs"}[30s])) by (container)"""
        -> """sum(rate(foo{_ws_="demo",_ns_="localNs"}[30s])) by (container)""",
      """sum(increase(foo{_ws_="demo",_ns_="localNs"}[30s])) by (container)"""
        -> """sum(increase(foo{_ws_="demo",_ns_="localNs"}[30s])) by (container)""",
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

      // this one cannot be optimized since one side has window < 60s. Optimize join only if both sides can be optimized
      """(sum(rate(foo{_ws_="demo",_ns_="localNs"}[10s])) by (container) + sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])))"""
        -> """(sum(rate(foo{_ws_="demo",_ns_="localNs"}[10s])) by (container) + sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])))""",
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

  it("[exclude rules] should optimize gauge queries that need to change the range function and aggregated column") {
    val testCases = Seq(
      """sum(count_over_time(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
         -> """sum(sum_over_time(foo:::agg1_1::count{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",
      // average of values in a gauge over time is sum/count
      """sum(sum_over_time(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container) / sum(count_over_time(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
        -> """(sum(sum_over_time(foo:::agg1_1::sum{_ws_="demo",_ns_="localNs"}[300s])) by (container) / sum(sum_over_time(foo:::agg1_1::count{_ws_="demo",_ns_="localNs"}[300s])) by (container))""",
    )
    testOptimization(excludeRules1, testCases)
  }

  it("[exclude rules] should optimize raw gauge queries that need to choose an aggregated column") {
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

  it("[exclude rules] should optimize agg gauge queries to use highest level aggregated rule") {
    val testCases = Seq(
      """min(min_over_time(foo:::agg1_1::min{_ws_="demo",_ns_="localNs"}[300s]))"""
        -> """min(min_over_time(foo:::agg1_2::min{_ws_="demo",_ns_="localNs"}[300s]))""",
      """max(max_over_time(foo:::agg1_1::max{_ws_="demo",_ns_="localNs"}[300s]))"""
        -> """max(max_over_time(foo:::agg1_2::max{_ws_="demo",_ns_="localNs"}[300s]))""",
      """sum(sum_over_time(foo:::agg1_1::sum{_ws_="demo",_ns_="localNs"}[300s]))"""
        -> """sum(sum_over_time(foo:::agg1_2::sum{_ws_="demo",_ns_="localNs"}[300s]))""",
      """sum(sum_over_time(foo:::agg1_1::count{_ws_="demo",_ns_="localNs"}[300s]))"""
        -> """sum(sum_over_time(foo:::agg1_2::count{_ws_="demo",_ns_="localNs"}[300s]))""",
    )
    testOptimization(excludeRules1, testCases)
  }

  it("[exclude rules] should optimize subqueries") {
    val testCases = Seq(
      // top level subquery
      """min(min_over_time(foo:::agg1_1::min{_ws_="demo",_ns_="localNs"}[300s]))[600s:300s]"""
        -> """min(min_over_time(foo:::agg1_2::min{_ws_="demo",_ns_="localNs"}[300s]))[600s:300s]""",
      // subquery with windowing
      """sum_over_time(min(min_over_time(foo:::agg1_1::min{_ws_="demo",_ns_="localNs"}[300s]))[600s:300s])"""
      -> """sum_over_time(min(min_over_time(foo:::agg1_2::min{_ws_="demo",_ns_="localNs"}[300s]))[600s:300s])"""
    )
    testOptimizationInstant(excludeRules1, testCases.take(1)) // top level subquery should be an instant query
    testOptimization(excludeRules1, testCases.drop(1))
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

  it("[exclude rules] should not optimize with group without label that is not excluded") {
    // should not optimize since dc is not excluded in any rule
    val testCases = Seq(
      """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) without (dc)"""
        -> """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) without (dc)""",
    )
    testOptimization(excludeRules1, testCases)
  }

  private val excludeRules2WithInactive = List(
    // Rule1 Level1 and its versions
    ExcludeAggRule("1", "agg1_1", Set("instance", "pod", "container"), 10000000L, active = true, level="1"),
    ExcludeAggRule("1", "agg1_1", Set("instance", "pod", "container", "guid"), 12000000L, active = true, level="1"),
    // this rule1 for multiple namespaces (queried namespace included) is inactive at 13, rule2 is active at 13
    ExcludeAggRule("1", "agg1_1", Set("instance", "pod", "container", "guid"), 13000000L, active = false, level="1"),
    // a rule for multiple namespaces (queried namespace included) is active enabled again at 14, rule2 is disabled
    ExcludeAggRule("1", "agg1_1", Set("instance", "pod", "container", "guid"), 14000000L, active = true, level="1"),

    // Rule2 Level1 and its versions
    // a different rule (with same suffix) for queried namespaces is added and is active at 13
    ExcludeAggRule("2", "agg2_1", Set("instance", "pod", "container", "port"), 13000000L, active = true, level="1"),
    // this rule (with same suffix) for queried namespaces is disabled at 14, and rule1 is active again at 14
    ExcludeAggRule("2", "agg2_1", Set("instance", "pod", "container", "port"), 14000000L, active = false, level="1"),
  )

  it("[exclude rules] should handle when some rules are inactive and there is a different rule from that timestamp") {
    val testCases = Seq(
      """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (dc)"""
        -> """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (dc)""", // not optimized: the rule is inactive at 13
    )
    testOptimization(excludeRules2WithInactive, testCases)
  }

  private val includeRules1 = List(
    // Rule1 Level1
    IncludeAggRule("1", "agg1_1", Set("dc", "service"), 10, active = true, level="1"),

    // Rule1 Level2 and its versions
    IncludeAggRule("1", "agg1_2", Set("dc", "service", "region"), 10, active = true, level="2"),
    IncludeAggRule("1", "agg1_2", Set("dc", "service", "region", "guid"), 12, active = true, level="2"),
    IncludeAggRule("1", "agg1_2", Set("dc", "service", "region", "port"), 13, active = true, level="2"),
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
      val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(startSeconds, step, endSeconds), Antlr)
      val optimized = lp.useHigherLevelAggregatedMetric(arp)
      LogicalPlanParser.convertToQuery(optimized) shouldEqual optimizedExpected
    }
  }

  private def testOptimizationInstant(rules: List[AggRule], testCases: Seq[(String, String)]): Unit = {
    val arp = newProvider(rules)
    for {(query, optimizedExpected) <- testCases} {
      println(s"Testing query $query")
      val lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(endSeconds, step, endSeconds), Antlr)
      val optimized = lp.useHigherLevelAggregatedMetric(arp)
      LogicalPlanParser.convertToQuery(optimized) shouldEqual optimizedExpected
    }
  }

  it("should not optimize instant queries within latest minute") {
    val query = """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s]))"""
    val now = System.currentTimeMillis() / 1000
    val lp = Parser.queryToLogicalPlan(query, now, step, Antlr)
    val arp = newProvider(excludeRules1)
    val optimized = lp.useHigherLevelAggregatedMetric(arp)
    LogicalPlanParser.convertToQuery(optimized) shouldEqual query
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

  it("should not optimize when agg is disabled") {
    val testCases = Seq(
      // same  query
      """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
        -> """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",
    )

    val arp = newProvider(excludeRules1, enabled = false)
    for { (query, optimizedExpected) <- testCases } {
      val lp = Parser.queryToLogicalPlan(query, endSeconds, step, Antlr)
      val optimized = lp.useHigherLevelAggregatedMetric(arp)
      LogicalPlanParser.convertToQuery(optimized) shouldEqual optimizedExpected
    }
  }

  it("should optimize when agg is disabled but optimize_with_agg function is used") {
    val testCases = Seq(
      """optimize_with_agg(sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container))"""
        -> """optimize_with_agg(sum(rate(foo:::agg1_1{_ws_="demo",_ns_="localNs"}[300s])) by (container))""",
    )

    val arp = newProvider(excludeRules1, enabled = false)
    for { (query, optimizedExpected) <- testCases } {
      val lp = Parser.queryToLogicalPlan(query, endSeconds, step, Antlr)
      val optimized = lp.useHigherLevelAggregatedMetric(arp)
      LogicalPlanParser.convertToQuery(optimized) shouldEqual optimizedExpected
    }
  }

  it("should not optimize when no rules are found") {
    val query = """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s]))"""
    val lp = Parser.queryToLogicalPlan(query, endSeconds, step, Antlr)
    val arp = newProvider(Nil)
    val optimized = lp.useHigherLevelAggregatedMetric(arp)
    LogicalPlanParser.convertToQuery(optimized) shouldEqual query
  }

}
