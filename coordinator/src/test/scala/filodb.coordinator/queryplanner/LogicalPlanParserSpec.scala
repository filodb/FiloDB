package filodb.coordinator.queryplanner

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.SeriesKeysByFilters

class LogicalPlanParserSpec extends AnyFunSpec with Matchers {

  private def parseAndAssertResult(query: String)(expectedResult: String = query) = {
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanParser.convertToQuery(lp)
    res shouldEqual expectedResult
  }

  it("should generate query from LogicalPlan") {
    parseAndAssertResult("""http_requests_total{job="app"}""") ()
    parseAndAssertResult("""sum(http_requests_total{job="app"})""") ()
    parseAndAssertResult("""sum(count(http_requests_total{job="app"}))""")()
    parseAndAssertResult("""sum(http_requests_total{job="app",instance="inst-1"}) by (instance)""")()
    parseAndAssertResult("""count(http_requests_total{job="app",instance="inst-1"}) without (instance)""")()
    parseAndAssertResult("""absent(http_requests_total{job="app"})""")()
    parseAndAssertResult("""exp(http_requests_total{job="app"})""")()
    parseAndAssertResult("""clamp_min(http_requests_total{job="app"},1000.0)""")()
    parseAndAssertResult("""histogram_quantile(0.2,sum(http_requests_total{job="app"}))""")()
    parseAndAssertResult("""5.1 > bool 2.2""")("""(5.1 > bool 2.2)""")
    parseAndAssertResult("""http_requests_total1{job="app"} + 2.1 + 3.1""") ("""((http_requests_total1{job="app"} + 2.1) + 3.1)""")
    parseAndAssertResult("""sum_over_time(http_requests_total{job="app"}[5s])""") ()
    parseAndAssertResult("""rate(http_requests_total{job="app"}[5s] offset 200s)""") ()
    parseAndAssertResult("""holt_winters(http_requests_total{job="app"}[5s],0.1,0.6)""") ()
    parseAndAssertResult("""predict_linear(http_requests_total{job="app"}[5s],7200.0)""") ()
    parseAndAssertResult("""rate(http_req_latency{app="foobar",_bucket_="0.5"}[5s])""") ()
    parseAndAssertResult("""sort(http_requests_total{job="app"})""") ()
    parseAndAssertResult("""sort_desc(http_requests_total{job="app"})""") ()
    parseAndAssertResult("""http_requests_total1{job="app"} + http_requests_total2{job="app"}""") ("""(http_requests_total1{job="app"} + http_requests_total2{job="app"})""")
    parseAndAssertResult("""http_requests_total1{job="app",instance="inst-1"} / ignoring(instance) """ +
        """http_requests_total2{job="app",instance="inst-1"}""") ("""(http_requests_total1{job="app",instance="inst-1"} / ignoring(instance) http_requests_total2{job="app",instance="inst-1"})""")
    parseAndAssertResult("""http_requests_total1{job="app",instance="inst-1"} / on(instance) """ +
      """http_requests_total2{job="app",instance="inst-1"}""") ("""(http_requests_total1{job="app",instance="inst-1"} / on(instance) http_requests_total2{job="app",instance="inst-1"})""".stripMargin)
    parseAndAssertResult("""http_requests_total1{job="app",instance="inst-1"} / on(instance) group_left """ +
      """http_requests_total2{job="app",instance="inst-1"}""") ("""(http_requests_total1{job="app",instance="inst-1"} / on(instance) group_left """ +
      """http_requests_total2{job="app",instance="inst-1"})""")
    parseAndAssertResult("""http_requests_total1{job="app",instance="inst-1"} / on(instance) group_left(job) """
      + """http_requests_total2{job="app",instance="inst-1"}""") ("""(http_requests_total1{job="app",instance="inst-1"} / on(instance) group_left(job) """
      + """http_requests_total2{job="app",instance="inst-1"})""")
    parseAndAssertResult("""hist_to_prom_vectors(http_request_latency)""") ("""hist_to_prom_vectors(http_request_latency)""")
    parseAndAssertResult("""label_join(http_requests_total1{job="app",instance="inst-1"},"dst","-"""" +
      ""","instance","job")""") ()
    parseAndAssertResult("""label_replace(http_requests_total{job="app",instance="inst-1"},"$1-new-label-$2","""
      + s""""instance","(.*)-(.*)")""")()
    parseAndAssertResult("""scalar(http_requests_total{job="app",instance="inst-1"})""")()
    parseAndAssertResult("""vector(1.5)""")()
    parseAndAssertResult("""time()""")()
    parseAndAssertResult("""http_requests_total::count{job="app"}""")()
    parseAndAssertResult("""http_requests_total::sum{job="app"}""")()
    parseAndAssertResult("""topk(2.0,http_requests_total{job="app"})""")()
    parseAndAssertResult("""quantile(0.2,http_requests_total{job="app"})""")()
    parseAndAssertResult("""count_values("freq",http_requests_total{job="app"})""")()
    parseAndAssertResult("""timestamp(http_requests_total{job="app"})""")()
    parseAndAssertResult("""absent(http_requests_total{job="app"})""")()
    parseAndAssertResult("""absent(sum(http_requests_total{job="app"}))""")()
    parseAndAssertResult("""absent(sum_over_time(http_requests_total{job="app"}[5s]))""")()
    parseAndAssertResult("""absent(rate(http_requests_total{job="app"}[5s] offset 200s))""")()
    parseAndAssertResult("""avg_over_time(test{_ws_="demo",_ns_=~"App.*",instance="Inst-1"}[5m:1m])""")("""avg_over_time(test{_ws_="demo",_ns_=~"App.*",instance="Inst-1"}[300s:60s])""")
    parseAndAssertResult("""quantile_over_time(0.5,test{_ws_="demo",_ns_=~"App.*",instance="Inst-1"}[5m:1m])""")("""quantile_over_time(0.5,test{_ws_="demo",_ns_=~"App.*",instance="Inst-1"}[300s:60s])""")
    parseAndAssertResult("""foo{_ws_="demo",_ns_="App.*"}[5m:1m]""")("""foo{_ws_="demo",_ns_="App.*"}[300s:60s]""")
    parseAndAssertResult("""max_over_time(avg_over_time(test{_ws_="demo",_ns_=~"App.*",instance="Inst-1"}[5m:1m])[3m:1m])""")("""max_over_time(avg_over_time(test{_ws_="demo",_ns_=~"App.*",instance="Inst-1"}[300s:60s])[180s:60s])""")
    parseAndAssertResult("""test{_ws_="demo",_ns_="App1",instance="Inst-1"}[600s]""")("""test{_ws_="demo",_ns_="App1",instance="Inst-1"}[600s]""")
    parseAndAssertResult("""test{_ws_="demo",_ns_="App1",instance="Inst-1"}[600s] offset 1000s""")("""test{_ws_="demo",_ns_="App1",instance="Inst-1"}[600s] offset 1000s""")
    parseAndAssertResult("""foo[5m:1m]""")("""foo[300s:60s]""")
  }

  it("should generate query from LogicalPlan having offset") {
    val query = "http_requests_total{job=\"app\"} offset 5m"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual("http_requests_total{job=\"app\"} offset 300s")
  }

  it("should generate query from SubqueryWithWindowing having offset") {
    val query = """sum_over_time(http_requests_total{job="app"}[5m:1m] offset 5m)"""
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000, Parser.Antlr)
    val res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual("""sum_over_time(http_requests_total{job="app"}[300s:60s] offset 300s)""")
  }

  it("should generate query from TopLevelSubquery having offset") {
    val query = """http_requests_total{job="app"}[5m:1m] offset 5m"""
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000, Parser.Antlr)
    val res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual("""http_requests_total{job="app"}[300s:60s] offset 300s""")
  }

  it("should generate query from LogicalPlan having escape characters") {
    val query = """sum(rate(my_counter{_ws_="demo",_ns_=~".+",app_identity=~".+\\.test\\.identity",status=~"5.."}[60s]))"""
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanParser.convertToQuery(lp)
    res shouldEqual query
  }

  it("should preserve brackets in Binary join query") {
    val query = """foo / (bar + baz)"""
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanParser.convertToQuery(lp)
    res shouldEqual "(foo / (bar + baz))"
  }

  it("should preserve brackets in scalar binary operation query") {
    val query = """1 / (2 + 3)"""
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanParser.convertToQuery(lp)
    res shouldEqual "(1.0 / (2.0 + 3.0))"
  }

  it("should preserve brackets in scalar vector operation query") {
    val query = """1 / (2 + foo)"""
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanParser.convertToQuery(lp)
    res shouldEqual "(1.0 / (2.0 + foo))"
  }

  it("should convert metadata series match query1") {
    val query = """http_requests_total{job="app"}"""
    val lp = Parser.metadataQueryToLogicalPlan(query, TimeStepParams(1000, 10, 2000))
    val res = LogicalPlanParser.metatadataMatchToQuery(lp.asInstanceOf[SeriesKeysByFilters])
    res shouldEqual "http_requests_total{job=\"app\"}"
  }

  it("should convert metadata series match query2") {
    val query = """{__name__="http_requests_total", job=~"app|job"}"""
    val lp = Parser.metadataQueryToLogicalPlan(query, TimeStepParams(1000, 10, 2000))
    val res = LogicalPlanParser.metatadataMatchToQuery(lp.asInstanceOf[SeriesKeysByFilters])
    res shouldEqual "http_requests_total{job=~\"app|job\"}"
  }

  it("should convert scalar vector operation query") {
    val query = """http_requests_total{job="app"} + 2.1"""
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanParser.convertToQuery(lp)
    res shouldEqual "(http_requests_total{job=\"app\"} + 2.1)"
  }
}
