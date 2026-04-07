package filodb.coordinator.queryplanner

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.{IntervalSelector, RawSeries, SeriesKeysByFilters}

class LogicalPlanParserSpec extends AnyFunSpec with Matchers {

  private def parseAndAssertResult(query: String)(expectedResult: String = query) = {
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanParser.convertToQuery(lp)
    res shouldEqual expectedResult
  }

  it("should generate query from LogicalPlan") {
    parseAndAssertResult("""http_requests_total{job="app"}""")("""{job="app",__name__="http_requests_total"}""")
    parseAndAssertResult("""sum(http_requests_total{job="app"})""")("""sum({job="app",__name__="http_requests_total"})""")
    parseAndAssertResult("""sum(count(http_requests_total{job="app"}))""")("""sum(count({job="app",__name__="http_requests_total"}))""")
    parseAndAssertResult("""sum(http_requests_total{job="app",instance="inst-1"}) by (instance)""")("""sum({job="app",instance="inst-1",__name__="http_requests_total"}) by (instance)""")
    parseAndAssertResult("""count(http_requests_total{job="app",instance="inst-1"}) without (instance)""")("""count({job="app",instance="inst-1",__name__="http_requests_total"}) without (instance)""")
    parseAndAssertResult("""absent(http_requests_total{job="app"})""")("""absent({job="app",__name__="http_requests_total"})""")
    parseAndAssertResult("""exp(http_requests_total{job="app"})""")("""exp({job="app",__name__="http_requests_total"})""")
    parseAndAssertResult("""clamp_min(http_requests_total{job="app"},1000.0)""")("""clamp_min({job="app",__name__="http_requests_total"},1000.0)""")
    parseAndAssertResult("""histogram_quantile(0.2,sum(http_requests_total{job="app"}))""")("""histogram_quantile(0.2,sum({job="app",__name__="http_requests_total"}))""")
    parseAndAssertResult("""5.1 > bool 2.2""")("""(5.1 > bool 2.2)""")
    parseAndAssertResult("""http_requests_total1{job="app"} + 2.1 + 3.1""") ("""(({job="app",__name__="http_requests_total1"} + 2.1) + 3.1)""")
    parseAndAssertResult("""sum_over_time(http_requests_total{job="app"}[5s])""")("""sum_over_time({job="app",__name__="http_requests_total"}[5s])""")
    parseAndAssertResult("""rate(http_requests_total{job="app"}[5s] offset 200s)""")("""rate({job="app",__name__="http_requests_total"}[5s] offset 200s)""")
    parseAndAssertResult("""holt_winters(http_requests_total{job="app"}[5s],0.1,0.6)""")("""holt_winters({job="app",__name__="http_requests_total"}[5s],0.1,0.6)""")
    parseAndAssertResult("""predict_linear(http_requests_total{job="app"}[5s],7200.0)""")("""predict_linear({job="app",__name__="http_requests_total"}[5s],7200.0)""")
    parseAndAssertResult("""rate(http_req_latency{app="foobar",_bucket_="0.5"}[5s])""")("""rate({app="foobar",__name__="http_req_latency",_bucket_="0.5"}[5s])""")
    parseAndAssertResult("""sort(http_requests_total{job="app"})""")("""sort({job="app",__name__="http_requests_total"})""")
    parseAndAssertResult("""sort_desc(http_requests_total{job="app"})""")("""sort_desc({job="app",__name__="http_requests_total"})""")
    parseAndAssertResult("""http_requests_total1{job="app"} + http_requests_total2{job="app"}""") ("""({job="app",__name__="http_requests_total1"} + {job="app",__name__="http_requests_total2"})""")
    parseAndAssertResult("""http_requests_total1{job="app",instance="inst-1"} / ignoring(instance) """ +
        """http_requests_total2{job="app",instance="inst-1"}""") ("""({job="app",instance="inst-1",__name__="http_requests_total1"} / ignoring(instance) {job="app",instance="inst-1",__name__="http_requests_total2"})""")
    parseAndAssertResult("""http_requests_total1{job="app",instance="inst-1"} / on(instance) """ +
      """http_requests_total2{job="app",instance="inst-1"}""") ("""({job="app",instance="inst-1",__name__="http_requests_total1"} / on(instance) {job="app",instance="inst-1",__name__="http_requests_total2"})""")
    parseAndAssertResult("""http_requests_total1{job="app",instance="inst-1"} / on(instance) group_left """ +
      """http_requests_total2{job="app",instance="inst-1"}""") ("""({job="app",instance="inst-1",__name__="http_requests_total1"} / on(instance) group_left """ +
      """{job="app",instance="inst-1",__name__="http_requests_total2"})""")
    parseAndAssertResult("""http_requests_total1{job="app",instance="inst-1"} / on(instance) group_left(job) """
      + """http_requests_total2{job="app",instance="inst-1"}""") ("""({job="app",instance="inst-1",__name__="http_requests_total1"} / on(instance) group_left(job) """
      + """{job="app",instance="inst-1",__name__="http_requests_total2"})""")
    parseAndAssertResult("""hist_to_prom_vectors(http_request_latency)""") ("""hist_to_prom_vectors({__name__="http_request_latency"})""")
    parseAndAssertResult("""label_join(http_requests_total1{job="app",instance="inst-1"},"dst","-"""" +
      ""","instance","job")""") ("""label_join({job="app",instance="inst-1",__name__="http_requests_total1"},"dst","-","instance","job")""")
    parseAndAssertResult("""label_replace(http_requests_total{job="app",instance="inst-1"},"$1-new-label-$2","""
      + s""""instance","(.*)-(.*)")""") ("""label_replace({job="app",instance="inst-1",__name__="http_requests_total"},"$1-new-label-$2","instance","(.*)-(.*)")""")
    parseAndAssertResult("""scalar(http_requests_total{job="app",instance="inst-1"})""")("""scalar({job="app",instance="inst-1",__name__="http_requests_total"})""")
    parseAndAssertResult("""vector(1.5)""")("""vector(1.5)""")
    parseAndAssertResult("""time()""")("""time()""")
    parseAndAssertResult("""http_requests_total::count{job="app"}""")("""{job="app",__name__="http_requests_total::count"}""")
    parseAndAssertResult("""http_requests_total::sum{job="app"}""")("""{job="app",__name__="http_requests_total::sum"}""")
    parseAndAssertResult("""topk(2.0,http_requests_total{job="app"})""")("""topk(2.0,{job="app",__name__="http_requests_total"})""")
    parseAndAssertResult("""quantile(0.2,http_requests_total{job="app"})""")("""quantile(0.2,{job="app",__name__="http_requests_total"})""")
    parseAndAssertResult("""count_values("freq",http_requests_total{job="app"})""")("""count_values("freq",{job="app",__name__="http_requests_total"})""")
    parseAndAssertResult("""timestamp(http_requests_total{job="app"})""")("""timestamp({job="app",__name__="http_requests_total"})""")
    parseAndAssertResult("""absent(http_requests_total{job="app"})""")("""absent({job="app",__name__="http_requests_total"})""")
    parseAndAssertResult("""absent(sum(http_requests_total{job="app"}))""")("""absent(sum({job="app",__name__="http_requests_total"}))""")
    parseAndAssertResult("""absent(sum_over_time(http_requests_total{job="app"}[5s]))""")("""absent(sum_over_time({job="app",__name__="http_requests_total"}[5s]))""")
    parseAndAssertResult("""absent(rate(http_requests_total{job="app"}[5s] offset 200s))""")("""absent(rate({job="app",__name__="http_requests_total"}[5s] offset 200s))""")
    parseAndAssertResult("""avg_over_time(test{_ws_="demo",_ns_=~"App.*",instance="Inst-1"}[5m:1m])""")("""avg_over_time({_ws_="demo",_ns_=~"App.*",instance="Inst-1",__name__="test"}[300s:60s])""")
    parseAndAssertResult("""quantile_over_time(0.5,test{_ws_="demo",_ns_=~"App.*",instance="Inst-1"}[5m:1m])""")("""quantile_over_time(0.5,{_ws_="demo",_ns_=~"App.*",instance="Inst-1",__name__="test"}[300s:60s])""")
    parseAndAssertResult("""foo{_ws_="demo",_ns_="App.*"}[5m:1m]""")("""{_ws_="demo",_ns_="App.*",__name__="foo"}[300s:60s]""")
    parseAndAssertResult("""max_over_time(avg_over_time(test{_ws_="demo",_ns_=~"App.*",instance="Inst-1"}[5m:1m])[3m:1m])""")("""max_over_time(avg_over_time({_ws_="demo",_ns_=~"App.*",instance="Inst-1",__name__="test"}[300s:60s])[180s:60s])""")
    parseAndAssertResult("""test{_ws_="demo",_ns_="App1",instance="Inst-1"}[600s]""")("""{_ws_="demo",_ns_="App1",instance="Inst-1",__name__="test"}[600s]""")
    parseAndAssertResult("""test{_ws_="demo",_ns_="App1",instance="Inst-1"}[600s] offset 1000s""")("""{_ws_="demo",_ns_="App1",instance="Inst-1",__name__="test"}[600s] offset 1000s""")
    parseAndAssertResult("""foo[5m:1m]""")("""{__name__="foo"}[300s:60s]""")
    parseAndAssertResult("""last_over_time_is_mad_outlier(3.0,1.0,sum(rate(http_requests_total{job="app"}[300s]))[432000s:300s])""")("""last_over_time_is_mad_outlier(3.0,1.0,sum(rate({job="app",__name__="http_requests_total"}[300s]))[432000s:300s])""")
  }

  it("should generate query from LogicalPlan having offset") {
    val query = "http_requests_total{job=\"app\"} offset 5m"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual("{job=\"app\",__name__=\"http_requests_total\"} offset 300s")
  }

  it("should generate query from LogicalPlan having limit") {
    val query = "http_requests_total{job=\"app\"} limit 10"
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual("{job=\"app\",__name__=\"http_requests_total\"} limit 10")
  }

  it("should generate query from LogicalPlan having @modifier") {
    var query = "http_requests_total{job=\"app\"} offset 5m @start()"
    var lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    var res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "{job=\"app\",__name__=\"http_requests_total\"} offset 300s @1000"

    query = "topk(1, http_requests_total{job=\"app\"}offset 5m @start())"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "topk(1.0,{job=\"app\",__name__=\"http_requests_total\"} offset 300s @1000)"

    query = "http_requests_total{job=\"app\"} and topk(1, http_requests_total{job=\"app\"}offset 5m @start())"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual
      "({job=\"app\",__name__=\"http_requests_total\"} and topk(1.0,{job=\"app\",__name__=\"http_requests_total\"} offset 300s @1000))"

    query = "scalar(topk(1, http_requests_total{job=\"app\"}offset 5m @start()))"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual
      "scalar(topk(1.0,{job=\"app\",__name__=\"http_requests_total\"} offset 300s @1000))"

    query = "ln(topk(1, http_requests_total{job=\"app\"}offset 5m @start()))"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual
      "ln(topk(1.0,{job=\"app\",__name__=\"http_requests_total\"} offset 300s @1000))"

    query = "rate(http_requests_total{job=\"app\"}[5m] offset 5m @1000)"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "rate({job=\"app\",__name__=\"http_requests_total\"}[300s] offset 300s @1000)"

    query = "sum(rate(http_requests_total{job=\"app\"}[5m] offset 5m @1000))"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "sum(rate({job=\"app\",__name__=\"http_requests_total\"}[300s] offset 300s @1000))"

    query = "topk(2, sum(rate(http_requests_total{job=\"app\"}[5m] offset 5m @start())))"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "topk(2.0,sum(rate({job=\"app\",__name__=\"http_requests_total\"}[300s] offset 300s @1000)))"

    query = "sum(rate(http_requests_total{job=\"app\"}[5m])) and " +
      "topk(2, sum(rate(http_requests_total{job=\"app\"}[5m] offset 5m @start())))"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "(sum(rate({job=\"app\",__name__=\"http_requests_total\"}[300s]))" +
      " and topk(2.0,sum(rate({job=\"app\",__name__=\"http_requests_total\"}[300s] offset 300s @1000))))"

    query = "sum(rate(http_requests_total{job=\"app\"}[5m: 1m] @100))"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "sum(rate({job=\"app\",__name__=\"http_requests_total\"}[300s:60s] @100))"
  }

  it("should correctly generate queries when LogicalPlan contains scalar() function") {
    parseAndAssertResult(
      "scalar(avg_over_time(http_requests_total[1h]))"
    )(
      "scalar(avg_over_time({__name__=\"http_requests_total\"}[3600s]))"
    )

    parseAndAssertResult(
      "scalar(cpu_usage_total)"
    )(
      "scalar({__name__=\"cpu_usage_total\"})"
    )

    parseAndAssertResult(
      "scalar(up{job=\"backend\"})"
    )(
      "scalar({job=\"backend\",__name__=\"up\"})"
    )

    parseAndAssertResult(
      "scalar(node_filesystem_size_bytes) - scalar(node_filesystem_free_bytes)"
    )(
      "(scalar({__name__=\"node_filesystem_size_bytes\"}) - scalar({__name__=\"node_filesystem_free_bytes\"}))"
    )

    parseAndAssertResult(
      "sum(scalar(rate(api_calls_total[5m])) + scalar(failed_calls_total))"
    )(
      "sum((scalar(rate({__name__=\"api_calls_total\"}[300s])) + scalar({__name__=\"failed_calls_total\"})))"
    )

    parseAndAssertResult(
      "scalar(time()) - scalar(node_boot_time_seconds)"
    )(
      "(scalar(time()) - scalar({__name__=\"node_boot_time_seconds\"}))"
    )

    parseAndAssertResult(
      "sum(rate(my_counter{_ws_=\"ws\",_ns_=\"ns\"}[5m])) / scalar(my_counter{_ws_=\"ws\",_ns_=\"ns\"})"
    )(
      "(sum(rate({_ws_=\"ws\",_ns_=\"ns\",__name__=\"my_counter\"}[300s])) / scalar({_ws_=\"ws\",_ns_=\"ns\",__name__=\"my_counter\"}))"
    )
  }

  it("should generate range query from LogicalPlan having @modifier") {
    var query = "http_requests_total{job=\"app\"} offset 5m @start()"
    var lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    var res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "{job=\"app\",__name__=\"http_requests_total\"} offset 300s @2000"

    query = "topk(1, http_requests_total{job=\"app\"}offset 5m @start())"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "topk(1.0,{job=\"app\",__name__=\"http_requests_total\"} offset 300s @2000)"

    query = "http_requests_total{job=\"app\"} and topk(1, http_requests_total{job=\"app\"}offset 5m @start())"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual
      "({job=\"app\",__name__=\"http_requests_total\"} and topk(1.0,{job=\"app\",__name__=\"http_requests_total\"} offset 300s @2000))"

    query = "scalar(topk(1, http_requests_total{job=\"app\"}offset 5m @start()))"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual
      "scalar(topk(1.0,{job=\"app\",__name__=\"http_requests_total\"} offset 300s @2000))"

    query = "ln(topk(1, http_requests_total{job=\"app\"}offset 5m @start()))"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual
      "ln(topk(1.0,{job=\"app\",__name__=\"http_requests_total\"} offset 300s @2000))"

    query = "rate(http_requests_total{job=\"app\"}[5m] offset 5m @2000)"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "rate({job=\"app\",__name__=\"http_requests_total\"}[300s] offset 300s @2000)"

    query = "sum(rate(http_requests_total{job=\"app\"}[5m] offset 5m @2000))"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "sum(rate({job=\"app\",__name__=\"http_requests_total\"}[300s] offset 300s @2000))"

    query = "topk(2, sum(rate(http_requests_total{job=\"app\"}[5m] offset 5m @end())))"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "topk(2.0,sum(rate({job=\"app\",__name__=\"http_requests_total\"}[300s] offset 300s @5000)))"

    query = "sum(rate(http_requests_total{job=\"app\"}[5m])) and " +
      "topk(2, sum(rate(http_requests_total{job=\"app\"}[5m] offset 5m @start())))"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "(sum(rate({job=\"app\",__name__=\"http_requests_total\"}[300s]))" +
      " and topk(2.0,sum(rate({job=\"app\",__name__=\"http_requests_total\"}[300s] offset 300s @2000))))"

    query = "sum(rate(http_requests_total{job=\"app\"}[5m: 1m] @100))"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "sum(rate({job=\"app\",__name__=\"http_requests_total\"}[300s:60s] @100))"
  }

  it("do not need @modifier time range is changed.") {
    val query = "http_requests_total{job=\"app\"}[5m] offset 5m @500"
    val lp = Parser.queryToLogicalPlan(query, 10000, 1000)
    // the selector time is changed to [500s, 500s]
    lp.asInstanceOf[RawSeries].rangeSelector shouldEqual IntervalSelector(500000, 500000)
    val res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "{job=\"app\",__name__=\"http_requests_total\"}[300s] offset 300s"
  }

  it("should generate query from SubqueryWithWindowing having offset") {
    val query = """sum_over_time(http_requests_total{job="app"}[5m:1m] offset 5m)"""
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000, Parser.Antlr)
    val res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual("""sum_over_time({job="app",__name__="http_requests_total"}[300s:60s] offset 300s)""")
  }

  it("should generate query from TopLevelSubquery having offset") {
    val query = """http_requests_total{job="app"}[5m:1m] offset 5m"""
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000, Parser.Antlr)
    val res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual("""{job="app",__name__="http_requests_total"}[300s:60s] offset 300s""")
  }

  it("should generate query from LogicalPlan having escape characters") {
    val query = """sum(rate(my_counter{_ws_="demo",_ns_=~".+",app_identity=~".+\\.test\\.identity",status=~"5.."}[60s]))"""
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanParser.convertToQuery(lp)
    res shouldEqual """sum(rate({_ws_="demo",_ns_=~".+",app_identity=~".+\\.test\\.identity",status=~"5..",__name__="my_counter"}[60s]))"""
  }

  it("should preserve brackets in Binary join query") {
    val query = """foo / (bar + baz)"""
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanParser.convertToQuery(lp)
    res shouldEqual "({__name__=\"foo\"} / ({__name__=\"bar\"} + {__name__=\"baz\"}))"
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
    res shouldEqual "(1.0 / (2.0 + {__name__=\"foo\"}))"
  }

  it("should convert metadata series match query1") {
    val query = """http_requests_total{job="app"}"""
    val lp = Parser.metadataQueryToLogicalPlan(query, TimeStepParams(1000, 10, 2000))
    val res = LogicalPlanParser.metadataMatchToQuery(lp.asInstanceOf[SeriesKeysByFilters])
    res shouldEqual "{job=\"app\",__name__=\"http_requests_total\"}"
  }

  it("should convert metadata series match query2") {
    val query = """{__name__="http_requests_total", job=~"app|job"}"""
    val lp = Parser.metadataQueryToLogicalPlan(query, TimeStepParams(1000, 10, 2000))
    val res = LogicalPlanParser.metadataMatchToQuery(lp.asInstanceOf[SeriesKeysByFilters])
    res shouldEqual "{__name__=\"http_requests_total\",job=~\"app|job\"}"
  }

  it("should convert metadata match query with __name__ regEx") {
    val query = """{__name__=~".*http_requests.*",job="app"}"""
    val lp = Parser.metadataQueryToLogicalPlan(query, TimeStepParams(1000, 10, 2000))
    val res = LogicalPlanParser.metadataMatchToQuery(lp.asInstanceOf[SeriesKeysByFilters])
    res shouldEqual "{__name__=~\".*http_requests.*\",job=\"app\"}"
  }

  it("should convert scalar vector operation query") {
    val query = """http_requests_total{job="app"} + 2.1"""
    val lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    val res = LogicalPlanParser.convertToQuery(lp)
    res shouldEqual "({job=\"app\",__name__=\"http_requests_total\"} + 2.1)"
  }
}
