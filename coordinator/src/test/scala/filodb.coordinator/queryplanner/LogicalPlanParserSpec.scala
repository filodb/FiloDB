package filodb.coordinator.queryplanner

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.prometheus.parse.Parser

class LogicalPlanParserSpec extends AnyFunSpec with Matchers {

  private def parseAndAssertResult(query: String) = {
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanParser.convertToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query from LogicalPlan") {
    parseAndAssertResult("""http_requests_total{job="app"}""")
    parseAndAssertResult("""sum(http_requests_total{job="app"})""")
    parseAndAssertResult("""sum(count(http_requests_total{job="app"}))""")
    parseAndAssertResult("""sum(http_requests_total{job="app",instance="inst-1"}) by (instance)""")
    parseAndAssertResult("""count(http_requests_total{job="app",instance="inst-1"}) without (instance)""")
    parseAndAssertResult("""absent(http_requests_total{job="app"})""")
    parseAndAssertResult("""exp(http_requests_total{job="app"})""")
    parseAndAssertResult("""clamp_min(http_requests_total{job="app"},1000.0)""")
    parseAndAssertResult("""histogram_quantile(0.2,sum(http_requests_total{job="app"}))""")
    parseAndAssertResult("""http_requests_total{job="app"} + 2.1""")
    parseAndAssertResult("""5.1 > bool 2.2""")
    parseAndAssertResult("""http_requests_total1{job="app"} + 2.1 + 3.1""")
    parseAndAssertResult("""sum_over_time(http_requests_total{job="app"}[5s])""")
    parseAndAssertResult("""rate(http_requests_total{job="app"}[5s] offset 200s)""")
    parseAndAssertResult("""holt_winters(http_requests_total{job="app"}[5s],0.1,0.6)""")
    parseAndAssertResult("""predict_linear(http_requests_total{job="app"}[5s],7200.0)""")
    parseAndAssertResult("""rate(http_req_latency{app="foobar",_bucket_="0.5"}[5s])""")
    parseAndAssertResult("""sort(http_requests_total{job="app"})""")
    parseAndAssertResult("""sort_desc(http_requests_total{job="app"})""")
    parseAndAssertResult("""http_requests_total1{job="app"} + http_requests_total2{job="app"}""")
    parseAndAssertResult("""http_requests_total1{job="app",instance="inst-1"} / ignoring(instance) """ +
        """http_requests_total2{job="app",instance="inst-1"}""")
    parseAndAssertResult("""http_requests_total1{job="app",instance="inst-1"} / on(instance) """ +
      """http_requests_total2{job="app",instance="inst-1"}""")
    parseAndAssertResult("""http_requests_total1{job="app",instance="inst-1"} / on(instance) group_left """ +
      """http_requests_total2{job="app",instance="inst-1"}""")
    parseAndAssertResult("""http_requests_total1{job="app",instance="inst-1"} / on(instance) group_left(job) """
      + """http_requests_total2{job="app",instance="inst-1"}""")
    parseAndAssertResult("""hist_to_prom_vectors(http_request_latency)""")
    parseAndAssertResult("""label_join(http_requests_total1{job="app",instance="inst-1"},"dst","-"""" +
      ""","instance","job")""")
    parseAndAssertResult("""label_replace(http_requests_total{job="app",instance="inst-1"},"$1-new-label-$2","""
      + s""""instance","(.*)-(.*)")""")
    parseAndAssertResult("""scalar(http_requests_total{job="app",instance="inst-1"})""")
    parseAndAssertResult("""vector(1.5)""")
    parseAndAssertResult("""time()""")
    parseAndAssertResult("""http_requests_total::count{job="app"}""")
    parseAndAssertResult("""http_requests_total::sum{job="app"}""")
    parseAndAssertResult("""topk(2.0,http_requests_total{job="app"})""")
    parseAndAssertResult("""quantile(0.2,http_requests_total{job="app"})""")
    parseAndAssertResult("""count_values("freq",http_requests_total{job="app"})""")
    parseAndAssertResult("""timestamp(http_requests_total{job="app"})""")
    parseAndAssertResult("""absent(http_requests_total{job="app"})""")
    parseAndAssertResult("""absent(sum(http_requests_total{job="app"}))""")
    parseAndAssertResult("""absent(sum_over_time(http_requests_total{job="app"}[5s]))""")
    parseAndAssertResult("""absent(rate(http_requests_total{job="app"}[5s] offset 200s))""")
  }

  it("should generate query from LogicalPlan having offset") {
    val query = "http_requests_total{job=\"app\"} offset 5m"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual("http_requests_total{job=\"app\"} offset 300s")
  }
}
