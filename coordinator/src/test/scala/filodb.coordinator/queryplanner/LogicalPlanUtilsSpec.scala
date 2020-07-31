package filodb.coordinator.queryplanner

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.prometheus.parse.Parser

class LogicalPlanUtilsSpec extends AnyFunSpec with Matchers {

  it("should generate query from LogicalPlan") {
    val query = "http_requests_total{job=\"app\"}"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query from LogicalPlan having offset") {
    val query = "http_requests_total{job=\"app\"} offset 5m"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual("http_requests_total{job=\"app\"} offset 300s")
  }

  it("should generate query from LogicalPlan for sum") {
    val query = "sum(http_requests_total{job=\"app\"})"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query from LogicalPlan for nested aggregate") {
    val query = "sum(count(http_requests_total{job=\"app\"}))"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query from LogicalPlan for sum with by") {
    val query = "sum(http_requests_total{job=\"app\",instance=\"inst-1\"}) by (instance)"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query from LogicalPlan for count with without") {
    val query = "count(http_requests_total{job=\"app\",instance=\"inst-1\"}) without (instance)"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query from LogicalPlan for absent function sum") {
    val query = "absent(http_requests_total{job=\"app\"})"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res  shouldEqual(query)
  }


  it("should generate query from LogicalPlan for exp function sum") {
    val query = "exp(http_requests_total{job=\"app\"})"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }
  it("should generate query from LogicalPlan for clamp_min function") {
    val query = "clamp_min(http_requests_total{job=\"app\"},1000.0)"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query from LogicalPlan for histogram quantile with sum") {
    val query = "histogram_quantile(0.2,sum(http_requests_total{job=\"app\"}))"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query for scalar vector binary operation") {
    val query = "http_requests_total{job=\"app\"} + 2.1"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query for scalar binary operation") {
    val query = "5.1 > bool 2.2"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query for nested scalar vector binary operation") {
    val query = "http_requests_total1{job=\"app\"} + 2.1 + 3.1"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query for sum over time") {
    val query = "sum_over_time(http_requests_total{job=\"app\"}[5s])"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query for rate with offset") {
    val query = "rate(http_requests_total{job=\"app\"}[5s] offset 200s)"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query for holt winters") {
    val query = "holt_winters(http_requests_total{job=\"app\"}[5s], 0.1, 0.6)"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res  shouldEqual(query.replaceAll("\\s", ""))// remove spaces
  }
  

  it("should generate query for predict linear") {
    val query = "predict_linear(http_requests_total{job=\"app\"}[5s], 7200.0)"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res  shouldEqual(query.replaceAll("\\s", ""))// remove spaces
  }


  it("should generate query for ApplyInstantFunctionRaw") {
    val query = "rate(http_req_latency{app=\"foobar\",_bucket_=\"0.5\"}[5s])"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query from LogicalPlan for sort") {
    val query = "sort(http_requests_total{job=\"app\"})"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query from LogicalPlan for sort_desc") {
    val query = "sort_desc(http_requests_total{job=\"app\"})"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query for binaryJoin") {
    val query = "http_requests_total1{job=\"app\"} + http_requests_total2{job=\"app\"}"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query for binaryJoin with ignoring") {
    val query = "http_requests_total1{job=\"app\",instance=\"inst-1\"} / ignoring(instance) " +
      "http_requests_total2{job=\"app\",instance=\"inst-1\"}"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query for binaryJoin with on") {
    val query = "http_requests_total1{job=\"app\",instance=\"inst-1\"} / on(instance) " +
      "http_requests_total2{job=\"app\",instance=\"inst-1\"}"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query for binaryJoin with group_left") {
    val query = "http_requests_total1{job=\"app\",instance=\"inst-1\"} / on(instance) group_left" +
      " http_requests_total2{job=\"app\",instance=\"inst-1\"}"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query for binaryJoin with group_right with params ") {
    val query = "http_requests_total1{job=\"app\",instance=\"inst-1\"} / on(instance) group_right(job)" +
      " http_requests_total2{job=\"app\",instance=\"inst-1\"}"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query for hist_to_prom_vectors ") {
    val query = "hist_to_prom_vectors(http_request_latency)"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query for label join ") {
    val query = "label_join(http_requests_total1{job=\"app\",instance=\"inst-1\"},\"dst\",\"-\"," +
      "\"instance\",\"job\")"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query for label replace ") {
    val query = "label_replace(http_requests_total{job=\"app\",instance=\"inst-1\"},\"$1-new-label-$2\",\"instance\"," +
      "\"(.*)-(.*)\")"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query for scalar") {
    val query = "scalar(http_requests_total{job=\"app\",instance=\"inst-1\"})"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query for vector") {
    val query = "vector(1.5)"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }

  it("should generate query for time()") {
    val query = "time()"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanUtils.logicalPlanToQuery(lp)
    res shouldEqual(query)
  }
}
