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
    ExcludeAggRule("1", "agg1", Set("instance", "pod"), 10, level="1"),
      ExcludeAggRule("1", "agg2", Set("instance", "pod", "container"), 10, level="2")
  )

  private val lpToOptimizedWithIncludes = Seq(
    """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container)"""
      -> """sum(rate(foo:::agg1{_ws_="demo",_ns_="localNs"}[300s])) by (container)""",


    """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s]))"""
      -> """sum(rate(foo:::agg2{_ws_="demo",_ns_="localNs"}[300s]))""",

    """sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s])) by (container) + sum(rate(foo{_ws_="demo",_ns_="localNs"}[300s]))"""
      -> """(sum(rate(foo:::agg1{_ws_="demo",_ns_="localNs"}[300s])) by (container) + sum(rate(foo:::agg2{_ws_="demo",_ns_="localNs"}[300s])))""",

  )

  it("should optimize when include rules are found") {
    val arp = newProvider(rules1)
    for { (query, optimizedExpected) <- lpToOptimizedWithIncludes } {
      println(s"Testing query: $query")
      val lp = Parser.queryToLogicalPlan(query, endSeconds, step, Antlr)
      val optimized = lp.useHigherLevelAggregatedMetric(arp)
      LogicalPlanParser.convertToQuery(optimized) shouldEqual optimizedExpected
    }
  }

}
