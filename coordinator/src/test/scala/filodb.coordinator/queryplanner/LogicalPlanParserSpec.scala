package filodb.coordinator.queryplanner

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser
import filodb.query.LogicalPlan.getColumnFilterGroup
import filodb.query.util.{ExcludeAggRule, HierarchicalQueryExperienceParams, IncludeAggRule}
import filodb.query.{Aggregate, BinaryJoin, IntervalSelector, RawSeries, SeriesKeysByFilters}

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

  it("should generate query from LogicalPlan having @modifier") {
    var query = "http_requests_total{job=\"app\"} offset 5m @start()"
    var lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    var res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "http_requests_total{job=\"app\"} offset 300s @1000"

    query = "topk(1, http_requests_total{job=\"app\"}offset 5m @start())"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "topk(1.0,http_requests_total{job=\"app\"} offset 300s @1000)"

    query = "http_requests_total{job=\"app\"} and topk(1, http_requests_total{job=\"app\"}offset 5m @start())"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual
      "(http_requests_total{job=\"app\"} and topk(1.0,http_requests_total{job=\"app\"} offset 300s @1000))"

    query = "scalar(topk(1, http_requests_total{job=\"app\"}offset 5m @start()))"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual
      "scalar(topk(1.0,http_requests_total{job=\"app\"} offset 300s @1000))"

    query = "ln(topk(1, http_requests_total{job=\"app\"}offset 5m @start()))"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual
      "ln(topk(1.0,http_requests_total{job=\"app\"} offset 300s @1000))"

    query = "rate(http_requests_total{job=\"app\"}[5m] offset 5m @1000)"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "rate(http_requests_total{job=\"app\"}[300s] offset 300s @1000)"

    query = "sum(rate(http_requests_total{job=\"app\"}[5m] offset 5m @1000))"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "sum(rate(http_requests_total{job=\"app\"}[300s] offset 300s @1000))"

    query = "topk(2, sum(rate(http_requests_total{job=\"app\"}[5m] offset 5m @start())))"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "topk(2.0,sum(rate(http_requests_total{job=\"app\"}[300s] offset 300s @1000)))"

    query = "sum(rate(http_requests_total{job=\"app\"}[5m])) and " +
      "topk(2, sum(rate(http_requests_total{job=\"app\"}[5m] offset 5m @start())))"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "(sum(rate(http_requests_total{job=\"app\"}[300s]))" +
      " and topk(2.0,sum(rate(http_requests_total{job=\"app\"}[300s] offset 300s @1000))))"

    query = "sum(rate(http_requests_total{job=\"app\"}[5m: 1m] @100))"
    lp = Parser.queryToLogicalPlan(query, 1000, 1000)
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "sum(rate(http_requests_total{job=\"app\"}[300s:60s] @100))"
  }

  it("should generate range query from LogicalPlan having @modifier") {
    var query = "http_requests_total{job=\"app\"} offset 5m @start()"
    var lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    var res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "http_requests_total{job=\"app\"} offset 300s @2000"

    query = "topk(1, http_requests_total{job=\"app\"}offset 5m @start())"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "topk(1.0,http_requests_total{job=\"app\"} offset 300s @2000)"

    query = "http_requests_total{job=\"app\"} and topk(1, http_requests_total{job=\"app\"}offset 5m @start())"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual
      "(http_requests_total{job=\"app\"} and topk(1.0,http_requests_total{job=\"app\"} offset 300s @2000))"

    query = "scalar(topk(1, http_requests_total{job=\"app\"}offset 5m @start()))"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual
      "scalar(topk(1.0,http_requests_total{job=\"app\"} offset 300s @2000))"

    query = "ln(topk(1, http_requests_total{job=\"app\"}offset 5m @start()))"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual
      "ln(topk(1.0,http_requests_total{job=\"app\"} offset 300s @2000))"

    query = "rate(http_requests_total{job=\"app\"}[5m] offset 5m @2000)"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "rate(http_requests_total{job=\"app\"}[300s] offset 300s @2000)"

    query = "sum(rate(http_requests_total{job=\"app\"}[5m] offset 5m @2000))"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "sum(rate(http_requests_total{job=\"app\"}[300s] offset 300s @2000))"

    query = "topk(2, sum(rate(http_requests_total{job=\"app\"}[5m] offset 5m @end())))"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "topk(2.0,sum(rate(http_requests_total{job=\"app\"}[300s] offset 300s @5000)))"

    query = "sum(rate(http_requests_total{job=\"app\"}[5m])) and " +
      "topk(2, sum(rate(http_requests_total{job=\"app\"}[5m] offset 5m @start())))"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "(sum(rate(http_requests_total{job=\"app\"}[300s]))" +
      " and topk(2.0,sum(rate(http_requests_total{job=\"app\"}[300s] offset 300s @2000))))"

    query = "sum(rate(http_requests_total{job=\"app\"}[5m: 1m] @100))"
    lp = Parser.queryRangeToLogicalPlan(query, TimeStepParams(2000, 10, 5000))
    res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "sum(rate(http_requests_total{job=\"app\"}[300s:60s] @100))"
  }

  it("do not need @modifier time range is changed.") {
    val query = "http_requests_total{job=\"app\"}[5m] offset 5m @500"
    val lp = Parser.queryToLogicalPlan(query, 10000, 1000)
    // the selector time is changed to [500s, 500s]
    lp.asInstanceOf[RawSeries].rangeSelector shouldEqual IntervalSelector(500000, 500000)
    val res = LogicalPlanParser.convertToQuery(lp)
    // Converted query has time in seconds
    res shouldEqual "http_requests_total{job=\"app\"}[300s] offset 300s"
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
    res shouldEqual "(http_requests_total{job=\"app\"} + 2.1)"
  }

  it("LogicalPlan update for hierarchical aggregation queries with Aggregate and BinaryJoin") {
    val t = TimeStepParams(700, 1000, 10000)
    val nextLevelAggregatedMetricSuffix = "agg_2"
    val nextLevelAggregationTags = Set("aggTag", "aggTag2")
    // CASE 1 - BinaryJoin (lhs = Aggregate, rhs = Aggregate) - Both lhs and rhs should be updated
    val binaryJoinAggregationBothOptimization = "sum(metric1:::agg{aggTag=\"app\"}) + sum(metric2:::agg{aggTag=\"app\"})"
    var lp = Parser.queryRangeToLogicalPlan(binaryJoinAggregationBothOptimization, t)
    val includeAggRule = IncludeAggRule(nextLevelAggregatedMetricSuffix, nextLevelAggregationTags)
    val includeParams = HierarchicalQueryExperienceParams(":::", Map("agg" -> includeAggRule))
    var lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    lpUpdated.isInstanceOf[BinaryJoin] shouldEqual true
    lpUpdated.asInstanceOf[BinaryJoin].lhs.isInstanceOf[Aggregate] shouldEqual true
    lpUpdated.asInstanceOf[BinaryJoin].rhs.isInstanceOf[Aggregate] shouldEqual true
    var filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter( x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .endsWith(nextLevelAggregatedMetricSuffix).shouldEqual(true)
    )
    // CASE 2 - BinaryJoin (lhs = Aggregate, rhs = Aggregate) - rhs should be updated
    val binaryJoinAggregationRHSOptimization = "sum(metric1:::agg{nonAggTag=\"abc\"}) + sum(metric2:::agg{aggTag=\"app\"})"
    lp = Parser.queryRangeToLogicalPlan(binaryJoinAggregationRHSOptimization, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].rhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("metric2:::agg_2")
    )
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].lhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("metric1:::agg")
    )
    // CASE 3 - BinaryJoin (lhs = Aggregate, rhs = Aggregate) - lhs should be updated and rhs should not since it is
    // not an aggregated metric, even if both the metrics qualify for aggregation
    val binaryJoinAggregationLHSOptimization = "sum(metric1:::agg{aggTag=\"abc\"}) + sum(metric2{aggTag=\"app\"})"
    lp = Parser.queryRangeToLogicalPlan(binaryJoinAggregationLHSOptimization, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].rhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("metric2")
    )
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].lhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("metric1:::agg_2")
    )
  }

  it("LogicalPlan update for hierarchical aggregation queries with by clause and include tags") {
    // common parameters
    val t = TimeStepParams(700, 1000, 10000)
    val nextLevelAggregatedMetricSuffix = "agg_2"
    val nextLevelAggregationTags = Set("aggTag", "aggTag2", "aggTag3", "aggTag4")
    val includeAggRule = IncludeAggRule(nextLevelAggregatedMetricSuffix, nextLevelAggregationTags)
    val includeParams = HierarchicalQueryExperienceParams(":::", Map("agg" -> includeAggRule))
    // CASE 1 - Aggregate with by clause - should update the metric name as `by` clause labels are part of include tags
    var query = "sum(rate(my_counter:::agg{aggTag=\"spark\", aggTag2=\"app\"}[5m])) by (aggTag4, aggTag3)"
    var lp = Parser.queryRangeToLogicalPlan(query, t)
    var lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    var filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg_2")
    )
    // CASE 2 - should NOT update since bottomk aggregation operator is not allowed as of now
    query = "sum(bottomk(2, my_counter:::agg{aggTag=\"spark\", aggTag2=\"filodb\"}) by (aggTag3, aggTag4)) by (aggTag4)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter( x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg")
    )
    // CASE 3 - should NOT update since the by clause labels are not part of include tags
    query = "sum(rate(my_counter:::agg{aggTag=\"spark\", aggTag2=\"app\"}[5m])) by (aggTag4, aggTag3, nonAggTag)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg")
    )
    // CASE 4 - should update since the by clause labels are part of include tags - binary join case
    query = "sum(my_gauge:::agg{aggTag=\"spark\", aggTag2=\"filodb\"}) by (aggTag, aggTag2) and on(aggTag, aggTag2) sum(my_counter:::agg{aggTag=\"spark\", aggTag2=\"filodb\"}) by (aggTag, aggTag2)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .endsWith(nextLevelAggregatedMetricSuffix).shouldEqual(true)
    )
    // CASE 5 - lhs should not be updated since it does not match regex pattern - binary join case
    query = "sum(my_gauge{aggTag=\"spark\", aggTag2=\"filodb\"}) by (aggTag, aggTag2) and on(aggTag, aggTag2) sum(my_counter:::agg{aggTag=\"spark\", aggTag2=\"filodb\"}) by (aggTag, aggTag2)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].lhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_gauge")
    )
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].rhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg_2")
    )
    // CASE 6 - rhs should not be updated since it has column filters which is not present in include tags
    query = "sum(my_gauge:::agg{aggTag=\"spark\", aggTag2=\"filodb\"}) by (aggTag, aggTag2) and on(aggTag, aggTag2) sum(my_counter:::agg{aggTag=\"spark\", aggTag2=\"filodb\", nonAggTag=\"1\"}) by (aggTag, aggTag2)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].lhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_gauge:::agg_2")
    )
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].rhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg")
    )
  }

  it("LogicalPlan update for hierarchical aggregation queries with by clause and exclude tags") {
    // common parameters
    val t = TimeStepParams(700, 1000, 10000)
    val nextLevelAggregatedMetricSuffix = "agg_2"
    val nextLevelAggregationExcludeTags = Set("excludeAggTag", "excludeAggTag2")
    val excludeAggRule = ExcludeAggRule(nextLevelAggregatedMetricSuffix, nextLevelAggregationExcludeTags)
    val excludeParams = HierarchicalQueryExperienceParams(":::", Map("agg" -> excludeAggRule))
    // CASE 1 - should update the metric name as `by` clause labels are not part of exclude tags
    var query = "sum(rate(my_counter:::agg{job=\"spark\", application=\"app\"}[5m])) by (host)"
    var lp = Parser.queryRangeToLogicalPlan(query, t)
    var lpUpdated = lp.useHigherLevelAggregatedMetric(excludeParams)
    var filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg_2")
    )
    // CASE 2 - should NOT update the metric name as column filters are not part of exclude tags
    query = "sum(rate(my_counter:::agg{nonAggTag=\"spark\", application=\"app\", excludeAggTag2=\"2.0\"}[5m]))"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(excludeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg")
    )
    // CASE 2 - should NOT update since bottomk aggregation operator is not allowed as of now
    query = "sum(bottomk(2, my_counter:::agg{job=\"spark\", application=\"filodb\"})) by (host)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(excludeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg")
    )
    // CASE 3 - should NOT update since the by clause labels intersect with exclude tags
    query = "sum(rate(my_counter:::agg{job=\"spark\", application=\"app\"}[5m])) by (excludeAggTag2, excludeAggTag, id)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(excludeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg")
    )
    // CASE 4 - should update since the by clause labels are not part of exclude tags - binary join case
    query = "sum(my_gauge:::agg{job=\"spark\", application=\"filodb\"}) by (id, host) + sum(your_gauge:::agg{job=\"spark\", application=\"filodb\"}) by (id, host)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(excludeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .endsWith(nextLevelAggregatedMetricSuffix).shouldEqual(true)
    )
    // CASE 5 - lhs should not be updated since it does not match regex pattern - binary join case
    query = "sum(my_gauge{job=\"spark\", application=\"filodb\"}) by (id, host) - sum(your_gauge:::agg{job=\"spark\", application=\"filodb\"}) by (id, host)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(excludeParams)
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].lhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_gauge")
    )
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].rhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("your_gauge:::agg_2")
    )
    // CASE 6 - rhs should not be updated since it has column filters which are part of exclude tags
    query = "sum(my_gauge:::agg{job=\"spark\", application=\"filodb\"}) by (id, host) / sum(your_gauge:::agg{job=\"spark\", application=\"filodb\", excludeAggTag2=\"1\"}) by (id, host)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(excludeParams)
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].lhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_gauge:::agg_2")
    )
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].rhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("your_gauge:::agg")
    )
  }

  it ("LogicalPlan update for hierarchical aggregation queries with without clause and exclude tags") {
    // common parameters
    val t = TimeStepParams(700, 1000, 10000)
    val nextLevelAggregatedMetricSuffix = "agg_2"
    val nextLevelAggregationExcludeTags = Set("excludeAggTag", "excludeAggTag2")
    val excludeAggRule = ExcludeAggRule(nextLevelAggregatedMetricSuffix, nextLevelAggregationExcludeTags)
    val excludeParams = HierarchicalQueryExperienceParams(":::", Map("agg" -> excludeAggRule))
    // CASE 1 - should update since the exclude tags are subset of the without clause labels
    var query = "sum(rate(my_counter:::agg{job=\"spark\", application=\"app\"}[5m])) without (excludeAggTag2, excludeAggTag)"
    var lp = Parser.queryRangeToLogicalPlan(query, t)
    var lpUpdated = lp.useHigherLevelAggregatedMetric(excludeParams)
    var filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg_2")
    )
    // CASE 2 - should NOT update since bottomk aggregation operator is not allowed as of now
    query = "sum(bottomk(2, my_counter:::agg{job=\"spark\", application=\"filodb\"}) without (excludeAggTag, excludeAggTag2))"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(excludeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter( x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg")
    )
    // CASE 3 - should NOT update since the column filter label is part of exclude tags
    query = "sum(rate(my_counter:::agg{job=\"spark\", application=\"app\", excludeAggTag2=\"2\"}[5m]))"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(excludeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg")
    )
    // CASE 4 - should update since the exclude tags are subset of the without clause labels
    query = "sum(rate(my_counter:::agg{job=\"spark\", application=\"app\"}[5m])) without (excludeAggTag2, excludeAggTag, id)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(excludeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg_2")
    )
    // CASE 5 - should not update since the exclude tags are not subset of the without clause labels
    query = "sum(rate(my_counter:::agg{job=\"spark\", application=\"app\"}[5m])) without (excludeAggTag2)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(excludeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg")
    )
    // CASE 6 - should update since the exclude tags are subset of without clause labels - binary join case
    query = "sum(my_gauge:::agg{job=\"spark\", application=\"filodb\"}) without (excludeAggTag2, excludeAggTag) and ignoring(excludeAggTag2, excludeAggTag) sum(my_counter:::agg{job=\"spark\", application=\"filodb\"}) without (excludeAggTag2, excludeAggTag)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(excludeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .endsWith(nextLevelAggregatedMetricSuffix).shouldEqual(true)
    )
    // CASE 7 - lhs should not be updated since it does not match regex pattern - binary join case
    query = "sum(my_gauge{job=\"spark\", application=\"filodb\"}) without (excludeAggTag2, excludeAggTag) and ignoring(excludeAggTag2, excludeAggTag) sum(my_counter:::agg{job=\"spark\", application=\"filodb\"}) without (excludeAggTag2, excludeAggTag)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(excludeParams)
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].lhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_gauge")
    )
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].rhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg_2")
    )
    // CASE 8 - rhs should not be updated since it has column filters which is part of exclude tags
    query = "sum(my_gauge:::agg{job=\"spark\", application=\"filodb\"}) without (excludeAggTag2, excludeAggTag) and ignoring(excludeAggTag2, excludeAggTag) sum(my_counter:::agg{job=\"spark\", application=\"filodb\", excludeAggTag2=\"1\"}) without (excludeAggTag2, excludeAggTag)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(excludeParams)
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].lhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_gauge:::agg_2")
    )
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].rhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg")
    )
  }

  it ("LogicalPlan update for hierarchical aggregation queries with without clause and include tags") {
    // common parameters
    val t = TimeStepParams(700, 1000, 10000)
    val nextLevelAggregatedMetricSuffix = "agg_2"
    val nextLevelAggregationTags = Set("job", "application", "instance", "version")
    val includeAggRule = IncludeAggRule(nextLevelAggregatedMetricSuffix, nextLevelAggregationTags)
    val includeParams = HierarchicalQueryExperienceParams(":::", Map("agg" -> includeAggRule))
    // All the cases should not be updated since without clause with include tags is not supported as of now
    var query = "sum(rate(my_counter:::agg{job=\"spark\", application=\"app\"}[5m])) without (version, instance)"
    var lp = Parser.queryRangeToLogicalPlan(query, t)
    var lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    var filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg")
    )
    query = "sum(bottomk(2, my_counter:::agg{job=\"spark\", application=\"filodb\"}) without (instance, version))"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg")
    )
    query = "sum(my_gauge:::agg{job=\"spark\", application=\"filodb\"}) without (version, instance) + sum(your_gauge:::agg{job=\"spark\", application=\"filodb\"}) without (version, instance)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .endsWith(":::agg").shouldEqual(true)
    )
    query = "sum(my_gauge{job=\"spark\", application=\"filodb\"}) without (version, instance) - sum(your_gauge:::agg{job=\"spark\", application=\"filodb\"}) without (version, instance)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].lhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_gauge")
    )
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].rhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("your_gauge:::agg")
    )
    query = "sum(my_gauge:::agg{job=\"spark\", application=\"filodb\"}) without (version, instance) / sum(your_gauge:::agg{job=\"spark\", application=\"filodb\", version=\"1\"}) without (version, instance)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].lhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_gauge:::agg")
    )
    filterGroups = getColumnFilterGroup(lpUpdated.asInstanceOf[BinaryJoin].rhs)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("your_gauge:::agg")
    )
  }

  it ("LogicalPlan update for hierarchical aggregation queries should not update simple raw and range queries") {
      // common parameters
    val t = TimeStepParams(700, 1000, 10000)
    val nextLevelAggregatedMetricSuffix = "agg_2"
    val nextLevelAggregationTags = Set("job", "application", "instance", "version")
    val includeAggRule = IncludeAggRule(nextLevelAggregatedMetricSuffix, nextLevelAggregationTags)
    val includeParams = HierarchicalQueryExperienceParams(":::", Map("agg" -> includeAggRule))
    // CASE 1: Raw queries lp should not be updated directly
    var query = "my_counter:::agg{job=\"spark\", application=\"app\"}[5m]"
    var lp = Parser.queryToLogicalPlan(query, t.start, t.step)
    var lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    var filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg")
    )
    // CASE 2: Simple range query without aggregates lp should not be updated directly
    query = "rate(my_counter:::agg{job=\"spark\", application=\"app\"}[5m])"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter( x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg")
    )
  }

  it("LogicalPlan update for hierarchical aggregation queries should update for only allowed aggregate operators") {
    // common parameters
    val t = TimeStepParams(700, 1000, 10000)
    val nextLevelAggregatedMetricSuffix = "agg_2"
    val nextLevelAggregationTags = Set("job", "application", "instance", "version")
    val includeAggRule = IncludeAggRule(nextLevelAggregatedMetricSuffix, nextLevelAggregationTags)
    val includeParams = HierarchicalQueryExperienceParams(":::", Map("agg" -> includeAggRule))
    // CASE 1: count aggregate should not be allowed
    var query = "count(my_gauge:::agg{job=\"spark\", application=\"app\"})"
    var lp = Parser.queryToLogicalPlan(query, t.start, t.step)
    lp.isInstanceOf[Aggregate] shouldEqual true
    var lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    var filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_gauge:::agg")
    )
    // CASE 2: sum aggregate should be allowed
    query = "sum(rate(my_counter:::agg{job=\"spark\", application=\"app\"}[5m]))"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lp.isInstanceOf[Aggregate] shouldEqual true
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg_2")
    )
    // CASE 3: avg aggregate should not be allowed
    query = "avg(rate(my_counter:::agg{job=\"spark\", application=\"app\"}[5m]))"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lp.isInstanceOf[Aggregate] shouldEqual true
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg")
    )
    // CASE 4: min aggregate should be allowed
    query = "min(my_gauge:::agg{job=\"spark\", application=\"app\"})"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lp.isInstanceOf[Aggregate] shouldEqual true
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_gauge:::agg_2")
    )
    // CASE 5: max aggregate should be allowed
    query = "max(my_gauge:::agg{job=\"spark\", application=\"app\"})"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lp.isInstanceOf[Aggregate] shouldEqual true
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_gauge:::agg_2")
    )
  }

  it("LogicalPlan update for hierarchical nested aggregation queries") {
    // common parameters using include tags
    val t = TimeStepParams(700, 1000, 10000)
    val nextLevelAggregatedMetricSuffix = "agg_2"
    var nextLevelAggregationTags = Set("aggTag1", "aggTag2", "aggTag3")
    val includeAggRule = IncludeAggRule(nextLevelAggregatedMetricSuffix, nextLevelAggregationTags)
    val includeParams = HierarchicalQueryExperienceParams(":::", Map("agg" -> includeAggRule))
    // CASE 1: should update the metric name as `by` clause labels are part of include tags
    var query = "sum(sum(my_counter:::agg{aggTag1=\"spark\", aggTag2=\"app\"}) by (aggTag1, aggTag2, aggTag3))"
    var lp = Parser.queryToLogicalPlan(query, t.start, t.step)
    var lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    var filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_counter:::agg_2")
    )
    // CASE 2: should not update since count aggregate operator is not allowed
    query = "sum by (aggTag1, aggTag2) (count by (aggTag1, aggTag2) (my_gauge:::agg{aggTag1=\"a\",aggTag2=\"b\"}))"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_gauge:::agg")
    )
    // CASE 3: should update since min aggregate operator is allowed
    query = "sum by (aggTag1, aggTag2) (min by (aggTag1, aggTag2) (my_gauge:::agg{aggTag1=\"a\",aggTag2=\"b\"}))"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_gauge:::agg_2")
    )
    // using excludeTags
    nextLevelAggregationTags = Set("excludeAggTag1", "excludeAggTag2")
    val excludeAggRule = ExcludeAggRule(nextLevelAggregatedMetricSuffix, nextLevelAggregationTags)
    val excludeParams = HierarchicalQueryExperienceParams(":::", Map("agg" -> excludeAggRule))
    // CASE 4: should update since excludeTags are not used
    query = "sum by (aggTag1, aggTag2) (sum by (aggTag1, aggTag2) (my_gauge:::agg{aggTag1=\"a\", aggTag2=\"b\"}))"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(excludeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_gauge:::agg_2")
    )
    // CASE 5: should not update since excludeTags are used
    query = "sum by (excludeAggTag1,aggTag2) (sum by (excludeAggTag1,aggTag1, aggTag2) (my_gauge:::agg{aggTag1=\"a\", aggTag2=\"b\"}))"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(excludeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .shouldEqual("my_gauge:::agg")
    )
  }

  it("LogicalPlan update for BinaryJoin with multiple agg rules and suffixes") {
    // common parameters
    val t = TimeStepParams(700, 1000, 10000)
    val includeAggRule = IncludeAggRule("suffix1_2", Set("includeTag1", "includeTag2", "includeTag3"))
    val excludeAggRule = ExcludeAggRule("suffix2_2", Set("excludeTag1", "excludeTag2"))
    // Query with multiple agg rules and suffixes
    val includeParams = HierarchicalQueryExperienceParams(":::",
      Map("suffix1" -> includeAggRule, "suffix2" -> excludeAggRule))
    // CASE 1 - should update - simple binary join with two different aggregated metrics and suffixes, both of which are satisfying the next level aggregation metric constraints
    var query = "sum(my_gauge:::suffix1{includeTag1=\"spark\", includeTag2=\"filodb\"}) + sum(your_gauge:::suffix2{notExcludeTag1=\"spark\", notExcludeTag2=\"filodb\"})"
    var lp = Parser.queryRangeToLogicalPlan(query, t)
    var lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    var filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .endsWith("_2").shouldEqual(true)
    )
    // CASE 2 - should NOT update as rhs is using an exclude tag
    query = "sum(my_gauge:::suffix1{includeTag1=\"spark\", includeTag2=\"filodb\"}) + sum(your_gauge:::suffix2{excludeTag1=\"spark\", notExcludeTag2=\"filodb\"})"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    var updatedMetricNamesSet = filterGroups.flatten.filter(x => x.column == "__name__")
      .map(_.filter.valuesStrings.head.asInstanceOf[String]).toSet
    updatedMetricNamesSet.contains("my_gauge:::suffix1_2").shouldEqual(true)
    updatedMetricNamesSet.contains("your_gauge:::suffix2").shouldEqual(true) // not updated
    // CASE 3 - should NOT update as lhs is not using an include tag
    query = "sum(my_gauge:::suffix1{notIncludeTag1=\"spark\", includeTag2=\"filodb\"}) + sum(your_gauge:::suffix2{notExcludeTag1=\"spark\", notExcludeTag2=\"filodb\"})"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    updatedMetricNamesSet = filterGroups.flatten.filter(x => x.column == "__name__")
      .map(_.filter.valuesStrings.head.asInstanceOf[String]).toSet
    updatedMetricNamesSet.contains("my_gauge:::suffix1").shouldEqual(true) // not updated
    updatedMetricNamesSet.contains("your_gauge:::suffix2_2").shouldEqual(true)
    // CASE 4 - should NOT update as both lhs and rhs are not using appropriate tags for next level aggregation metric
    query = "sum(my_gauge:::suffix1{includeTag1=\"spark\", notIncludeTag2=\"filodb\"}) + sum(your_gauge:::suffix2{notExcludeTag1=\"spark\", excludeTag2=\"filodb\"})"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    updatedMetricNamesSet = filterGroups.flatten.filter(x => x.column == "__name__")
      .map(_.filter.valuesStrings.head.asInstanceOf[String]).toSet
    updatedMetricNamesSet.contains("my_gauge:::suffix1").shouldEqual(true) // not updated
    updatedMetricNamesSet.contains("your_gauge:::suffix2").shouldEqual(true) // not updated
  }

  it("LogicalPlan update for BinaryJoin with multiple agg rules and suffixes with by clauses") {
    // common parameters
    val t = TimeStepParams(700, 1000, 10000)
    val includeAggRule = IncludeAggRule("suffix1_2", Set("includeTag1", "includeTag2", "includeTag3"))
    val excludeAggRule = ExcludeAggRule("suffix2_2", Set("excludeTag1", "excludeTag2"))
    // Query with multiple agg rules and suffixes
    val includeParams = HierarchicalQueryExperienceParams(":::",
      Map("suffix1" -> includeAggRule, "suffix2" -> excludeAggRule))
    // CASE 1 - should update - both lhs and rhs are satisfying the next level aggregation metric constraints
    var query = "sum(my_gauge:::suffix1{includeTag1=\"spark\", includeTag2=\"filodb\"}) by (includeTag3, includeTag1) + sum(your_gauge:::suffix2{notExcludeTag1=\"spark\", notExcludeTag2=\"filodb\"}) by (notExcludeTag1)"
    var lp = Parser.queryRangeToLogicalPlan(query, t)
    var lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    var filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .endsWith("_2").shouldEqual(true)
    )
    // CASE 2 - should NOT update as rhs is using an exclude tag
    query = "sum(my_gauge:::suffix1{includeTag1=\"spark\", includeTag2=\"filodb\"}) by (includeTag3, includeTag1) + sum(your_gauge:::suffix2{notExcludeTag1=\"spark\", notExcludeTag2=\"filodb\"}) by (excludeTag1)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    var updatedMetricNamesSet = filterGroups.flatten.filter(x => x.column == "__name__")
      .map(_.filter.valuesStrings.head.asInstanceOf[String]).toSet
    updatedMetricNamesSet.contains("my_gauge:::suffix1_2").shouldEqual(true)
    updatedMetricNamesSet.contains("your_gauge:::suffix2").shouldEqual(true) // not updated
    // CASE 3 - should NOT update as lhs is not using an include tag
    query = "sum(my_gauge:::suffix1{includeTag1=\"spark\", includeTag2=\"filodb\"}) by (notIncludeTag3, includeTag1) + sum(your_gauge:::suffix2{notExcludeTag1=\"spark\", notExcludeTag2=\"filodb\"}) by (notExludeTag1)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    updatedMetricNamesSet = filterGroups.flatten.filter(x => x.column == "__name__")
      .map(_.filter.valuesStrings.head.asInstanceOf[String]).toSet
    updatedMetricNamesSet.contains("my_gauge:::suffix1").shouldEqual(true) // not updated
    updatedMetricNamesSet.contains("your_gauge:::suffix2_2").shouldEqual(true)
    // CASE 4 - should NOT update as both lhs and rhs are not using appropriate tags for next level aggregation metric
    query = "sum(my_gauge:::suffix1{includeTag1=\"spark\", includeTag2=\"filodb\"}) by (notIncludeTag3, includeTag1) + sum(your_gauge:::suffix2{notExcludeTag1=\"spark\", notExcludeTag2=\"filodb\"}) by (excludeTag1)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    updatedMetricNamesSet = filterGroups.flatten.filter(x => x.column == "__name__")
      .map(_.filter.valuesStrings.head.asInstanceOf[String]).toSet
    updatedMetricNamesSet.contains("my_gauge:::suffix1").shouldEqual(true) // not updated
    updatedMetricNamesSet.contains("your_gauge:::suffix2").shouldEqual(true) // not updated
  }

  it("LogicalPlan update for BinaryJoin with multiple agg rules and suffixes with by and without clauses") {
    // common parameters
    val t = TimeStepParams(700, 1000, 10000)
    val includeAggRule = IncludeAggRule("suffix1_2", Set("includeTag1", "includeTag2", "includeTag3"))
    val excludeAggRule = ExcludeAggRule("suffix2_2", Set("excludeTag1", "excludeTag2"))
    // Query with multiple agg rules and suffixes
    val includeParams = HierarchicalQueryExperienceParams(":::",
      Map("suffix1" -> includeAggRule, "suffix2" -> excludeAggRule))
    // CASE 1 - should update - both lhs and rhs are satisfying the next level aggregation metric constraints
    var query = "sum(my_gauge:::suffix1{includeTag1=\"spark\", includeTag2=\"filodb\"}) by (includeTag3, includeTag1) + sum(your_gauge:::suffix2{notExcludeTag1=\"spark\", notExcludeTag2=\"filodb\"}) without (excludeTag1, excludeTag2)"
    var lp = Parser.queryRangeToLogicalPlan(query, t)
    var lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    var filterGroups = getColumnFilterGroup(lpUpdated)
    filterGroups.foreach(
      filterSet => filterSet.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
        .endsWith("_2").shouldEqual(true)
    )
    // CASE 2 - should NOT update as excludeRule tags is not subset of rhs without clause labels
    query = "sum(my_gauge:::suffix1{includeTag1=\"spark\", includeTag2=\"filodb\"}) by (includeTag3, includeTag1) + sum(your_gauge:::suffix2{notExcludeTag1=\"spark\", notExcludeTag2=\"filodb\"}) without (excludeTag1)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    var updatedMetricNamesSet = filterGroups.flatten.filter(x => x.column == "__name__")
      .map(_.filter.valuesStrings.head.asInstanceOf[String]).toSet
    updatedMetricNamesSet.contains("my_gauge:::suffix1_2").shouldEqual(true)
    updatedMetricNamesSet.contains("your_gauge:::suffix2").shouldEqual(true) // not updated
  }

  it("LogicalPlan should not update when next level aggregation metric suffix is not matching agg rules") {
    // common parameters
    val t = TimeStepParams(700, 1000, 10000)
    val includeAggRule = IncludeAggRule("suffix1_2", Set("includeTag1", "includeTag2", "includeTag3"))
    val excludeAggRule = ExcludeAggRule("suffix2_2", Set("excludeTag1", "excludeTag2"))
    // Query with multiple agg rules and suffixes
    val includeParams = HierarchicalQueryExperienceParams(":::",
      Map("suffix1" -> includeAggRule, "suffix2" -> excludeAggRule))
    // CASE 1 - should not update - both lhs and rhs metric are not using suffix passed for lp update
    var query = "sum(my_gauge:::no_rule{includeTag1=\"spark\", includeTag2=\"filodb\"}) by (includeTag3, includeTag1)"
    var lp = Parser.queryRangeToLogicalPlan(query, t)
    var lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    var filterGroups = getColumnFilterGroup(lpUpdated)
    var updatedMetricNamesSet = filterGroups.flatten.filter(x => x.column == "__name__")
      .map(_.filter.valuesStrings.head.asInstanceOf[String]).toSet
    updatedMetricNamesSet.contains("my_gauge:::no_rule").shouldEqual(true)// not updated
    // CASE 2 - should NOT update rhs as it is not using the given suffix
    query = "sum(my_gauge:::suffix1{includeTag1=\"spark\", includeTag2=\"filodb\"}) by (includeTag3, includeTag1) + sum(your_gauge:::no_rule2{notExcludeTag1=\"spark\", notExcludeTag2=\"filodb\"}) by (notExcludeTag1)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    updatedMetricNamesSet = filterGroups.flatten.filter(x => x.column == "__name__")
      .map(_.filter.valuesStrings.head.asInstanceOf[String]).toSet
    updatedMetricNamesSet.contains("my_gauge:::suffix1_2").shouldEqual(true)
    updatedMetricNamesSet.contains("your_gauge:::no_rule2").shouldEqual(true) // not updated
    // CASE 3 - should NOT update lhs as it is not using the given suffix
    query = "sum(my_gauge:::no_rule{includeTag1=\"spark\", includeTag2=\"filodb\"}) by (includeTag3, includeTag1) + sum(your_gauge:::suffix2{notExcludeTag1=\"spark\", notExcludeTag2=\"filodb\"}) by (notExcludeTag1)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    updatedMetricNamesSet = filterGroups.flatten.filter(x => x.column == "__name__")
      .map(_.filter.valuesStrings.head.asInstanceOf[String]).toSet
    updatedMetricNamesSet.contains("my_gauge:::no_rule").shouldEqual(true)// not updated
    updatedMetricNamesSet.contains("your_gauge:::suffix2_2").shouldEqual(true)
    // CASE 3 - should NOT update lhs and rhs as it is not using the given suffix
    query = "sum(my_gauge:::no_rule{includeTag1=\"spark\", includeTag2=\"filodb\"}) by (includeTag3, includeTag1) + sum(your_gauge:::no_rule2{notExcludeTag1=\"spark\", notExcludeTag2=\"filodb\"}) by (notExcludeTag1)"
    lp = Parser.queryRangeToLogicalPlan(query, t)
    lpUpdated = lp.useHigherLevelAggregatedMetric(includeParams)
    filterGroups = getColumnFilterGroup(lpUpdated)
    updatedMetricNamesSet = filterGroups.flatten.filter(x => x.column == "__name__")
      .map(_.filter.valuesStrings.head.asInstanceOf[String]).toSet
    updatedMetricNamesSet.contains("my_gauge:::no_rule").shouldEqual(true) // not updated
    updatedMetricNamesSet.contains("your_gauge:::no_rule2").shouldEqual(true) // not updated
  }
}
