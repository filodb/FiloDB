package filodb.prometheus.parse

import filodb.prometheus.ast.{PeriodicSeries, RangeExpression, SimpleSeries, SubqueryClause, SubqueryExpression, TimeStepParams, VectorSpec}
import filodb.prometheus.parse.Parser.{Antlr, Shadow}
import filodb.query.{BinaryJoin, LogicalPlan}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeUnit.{MINUTES, SECONDS, HOURS, DAYS}
import scala.concurrent.duration.Duration

//noinspection ScalaStyle
// scalastyle:off
class ParserSpec extends AnyFunSpec with Matchers {

  it("test") {
    antlrParseSuccessfully("http_requests_total{job=\"prometheus\", method=\"GET\"} limit 10")
    parseSuccessfully("http_requests_total{job=\"prometheus\", method=\"GET\"} limit 1")
  }

  it("test columnName split regex for different type of metric names") {

    // Various test cases to test the regex on
    val input = List(
      ("http_requests_total:::aggregated_rule","http_requests_total:::aggregated_rule", None),
      ("http_requests_total:::aggregated_rule::count","http_requests_total:::aggregated_rule", Some("count")),
      ("http_requests_total::sum","http_requests_total", Some("sum")),
      ("http_requests_total","http_requests_total", None),
      ("mygauge","mygauge", None),
      ("mygauge:sum","mygauge:sum", None),
      ("mygauge:sum:::","mygauge:sum:::", None),
    )

    input.foreach { i => {
      val (metricName, columnName) = VectorSpec.apply().getMetricAndColumnName(i._1)
      metricName shouldEqual i._2
      columnName shouldEqual i._3
    }}
  }

  it("test columnName split regex should throw IllegalArgumentException for invalid metric name") {

    val input = List(
      ("http_requests_total:: ", "cannot use blank/whitespace column name"),
      ("test::    ", "cannot use blank/whitespace column name"),
    )

    input.foreach { i => {
      val thrownEx = the [IllegalArgumentException] thrownBy(VectorSpec.apply().getMetricAndColumnName(i._1))
      thrownEx.getMessage.contains(i._2) shouldEqual true
    }}
  }

  it("metadata matcher query") {
    parseSuccessfully("http_requests_total{job=\"prometheus\", method=\"GET\"}")
    parseSuccessfully("http_requests_total{job=\"prometheus\", method=\"GET\"}")
    parseSuccessfully("http_requests_total{job=\"prometheus\", method!=\"GET\"}")
    parseError("job{__name__=\"prometheus\"}")
    parseError("job[__name__=\"prometheus\"]")
    val queryToLpString = ("http_requests_total{job=\"prometheus\", method=\"GET\"}" ->
      "SeriesKeysByFilters(List(ColumnFilter(job,Equals(prometheus)), ColumnFilter(method,Equals(GET)), ColumnFilter(__name__,Equals(http_requests_total))),true,1524855988000,1524855988000)")
    val start: Long = 1524855988L
    val end: Long = 1524855988L
    val lp = Parser.metadataQueryToLogicalPlan(queryToLpString._1, TimeStepParams(start, -1, end), true)
    lp.toString shouldEqual queryToLpString._2
  }

  it("labelvalues filter query") {
    parseLabelValueSuccessfully("job=\"prometheus\", method=\"GET\"")
    parseLabelValueSuccessfully("job=\"prometheus\", method=\"GET\"")
    parseLabelValueSuccessfully("job=\"prometheus\", method!=\"GET\"")
    parseLabelValueError("http_requests_total{job=\"prometheus\", method!=\"GET\"}")
    parseLabelValueError("{__name__=\"prometheus\"}")
    parseLabelValueError("job[__name__=\"prometheus\"]")
    val queryToLpString = ("job=\"prometheus\", method!=\"GET\"" ->
      "LabelValues(List(_ns_),List(ColumnFilter(job,Equals(prometheus)), ColumnFilter(method,NotEquals(GET))),1524855988000,1524855988000)")
    val start: Long = 1524855988L
    val end: Long = 1524855988L
    val lp = Parser.labelValuesQueryToLogicalPlan(Seq("_ns_"), Some(queryToLpString._1), TimeStepParams(start, -1, end))
    lp.toString shouldEqual queryToLpString._2
  }

  it("parse basic scalar expressions") {
    parseSuccessfully("-5")
    parseSuccessfully("+5")
    parseSuccessfully("1")
    //    parse("+Inf")
    //    parse("-Inf")
    parseSuccessfully(".5")
    parseSuccessfully("5.")
    parseSuccessfully("123.4567")
    parseSuccessfully("5e-3")
    parseSuccessfully("5e3")
    //parse("0xc")
    parseSuccessfully("0755")
    parseSuccessfully("+5.5e-3")
    parseSuccessfully("-0755")
    parseSuccessfully("1 + 1")
    parseSuccessfully("1 - 1")
    parseSuccessfully("1 * 1")
    parseSuccessfully("1 % 1")
    parseSuccessfully("1 / 1")
    parseSuccessfully("1 == bool 1")
    parseSuccessfully("1 != bool 1")
    parseSuccessfully("1 > bool 1")
    parseSuccessfully("1 >= bool 1")
    parseSuccessfully("1 < bool 1")
    parseSuccessfully("1 <= bool 1")
    parseSuccessfully("4 == bool (2)")
    parseSuccessfully("4 == bool(2)")
    parseSuccessfully("+1 + -2 * 1")
    parseSuccessfully("1 < bool 2 - 1 * 2")
    parseSuccessfully("1 + 2/(3*1)")
    //parseSuccessfully("10 + -(20 + 30)")
    parseSuccessfully("-some_metric")
    parseSuccessfully("+some_metric")
    parseSuccessfully("(1 + heap_size{a=\"b\"})")
    parseSuccessfully("(1 + heap_size{a=\"b\"}) + 5")
    parseSuccessfully("(1 + heap_size{a=\"b\"}) + 5 * (3 - cpu_load{c=\"d\"})")
    parseSuccessfully("((1 + heap_size{a=\"b\"}) + 5) * (3 - cpu_load{c=\"d\"})")
    parseSuccessfully("foo:ba-r:a.b{a=\"bc\"}")
    parseSuccessfully("foo:ba-001:a.b{a=\"b-c\"}")

    parseError("")
    parseError("# just a comment\n\n")
    parseError("1+")
    parseError(".")
    parseError("2.5.")
    parseError("100..4")
    parseError("0deadbeef")
    parseError("1 /")
    parseError("*1")
    parseError("(1))")
    parseError("((1)")
    parseError("((1 + heap_size{a=\"b\"})")
    parseError("(1 + heap_size{a=\"b\"}))")
    parseError("(1 + heap_size{a=\"b\"}) + (5")
    parseError("(1 + heap_size{a=\"b\"}) + 5 * (3 - cpu_load{c=\"d\"}")

    parseError("(")
    // NOTE: Uncomment when we move to antlr

    // parseError("1 and 1")
    // parseError("1 == 1")  // reason: comparisons between scalars must use BOOL modifier
    // parseError("1 or 1")
    // parseError("1 unless 1")
    parseError("1 !~ 1")
    parseError("1 =~ 1")
    parseError("-\"string\"")
    parseError("-test[5m]")
    parseError("*test")
    parseError("1 offset 1d")
    parseError("a - on(b) ignoring(c) d")

    parseSuccessfully("foo * bar")
    parseSuccessfully("foo * bar limit 1")
    parseSuccessfully("(foo * bar) limit 1")
    parseSuccessfully("foo == 1")
    parseSuccessfully("foo == bool 1")
    parseSuccessfully("foo > bool bar")
    parseSuccessfully("scalar(foo) > bool scalar(bar)")
    parseSuccessfully("2.5 / bar")
    parseSuccessfully("foo and bar")
    parseSuccessfully("foo or bar")
    parseSuccessfully("foo unless bar")
    parseSuccessfully("foo + bar or bla and blub")
    parseSuccessfully("foo and bar unless baz or qux")
    parseSuccessfully("bar + on(foo) bla / on(baz, buz) group_right(test) blub")
    parseSuccessfully("foo * on(test,blub) bar")
    parseSuccessfully("foo * on(test,blub) group_left bar")
    parseSuccessfully("foo and on(test,blub) bar")
    parseSuccessfully("foo and on() bar")
    parseSuccessfully("foo and ignoring(test,blub) bar")
    parseSuccessfully("foo and ignoring() bar")
    parseSuccessfully("foo unless on(bar) baz")
    parseSuccessfully("foo / on(test,blub) group_left(bar) bar")
    parseSuccessfully("foo / ignoring(test,blub) group_left(blub) bar")
    parseSuccessfully("foo / ignoring(test,blub) group_left(bar) bar")
    parseSuccessfully("foo - on(test,blub) group_right(bar,foo) bar")
    parseSuccessfully("foo - ignoring(test,blub) group_right(bar,foo) bar")
    // NOTE: Uncomment when we move to antlr

    // parseError("foo and 1")
    // parseError("1 and foo")
    // parseError("foo or 1")
    // parseError("1 or foo")
    // parseError("foo unless 1")
    // parseError("1 unless foo")
    parseError("1 or on(bar) foo")
    parseError("foo == on(bar) 10")
    parseError("foo and on(bar) group_left(baz) bar")
    parseError("foo and on(bar) group_right(baz) bar")
    parseError("foo or on(bar) group_left(baz) bar")
    parseError("foo or on(bar) group_right(baz) bar")
    parseError("foo unless on(bar) group_left(baz) bar")
    parseError("foo unless on(bar) group_right(baz) bar")
    parseError("http_requests{group=\"production\"} + on(instance) group_left(job,instance) cpu_count{type=\"smp\"}")
    parseError("foo and bool 10")
    parseError("foo + bool 10")
    parseError("foo + bool bar")
    parseError("bool(1) < 2")

    parseSuccessfully("foo")
    parseSuccessfully("foo offset 5m")
    parseSuccessfully("foo limit 1")
    parseSuccessfully("foo:bar{a=\"bc\"}")
    parseSuccessfully("foo{NaN='bc'}")
    parseSuccessfully("foo{a=\"b\", foo!=\"bar\", test=~\"test\", bar!~\"baz\"}")
    parseSuccessfully(":node_memory_utilisation:{_ns=\"piepubliccloud\"}")

    parseError("{")
    parseError("}")
    parseError("some{")
    parseError("some}")
    parseError("some_metric{a=b}")
    parseError("some_metric{a:b=\"b\"}")
    parseError("foo{a*\"b\"}")
    parseError("foo{a>=\"b\"}")

    parseError("foo::b{gibberish}")
    parseError("foo{1}")
    parseError("{}")
    parseError("foo{__name__=\"bar\"}")

    parseSuccessfully("test{a=\"b\"}[5y] OFFSET 3d")
    parseSuccessfully("test{a=\"b\"}[5y] LIMIT 3")
    parseSuccessfully("test{a=\"b\"}[5y] OFFSET 3d LIMIT 3")
    parseSuccessfully("test[5s]")
    parseSuccessfully("test[5m]")
    parseSuccessfully("test[5h] OFFSET 5m")
    parseSuccessfully("test[5d] OFFSET 10s")
    parseSuccessfully("test[5w] offset 2w")
    parseSuccessfully("foo[5m30s]")
    parseSuccessfully("foo[5m] OFFSET 1h30m")
    parseSuccessfully("foo[5m25s] OFFSET 1h30m")
    parseSuccessfully("foo[1h5m25s] OFFSET 1h30m20s")

    parseError("foo[5mm]")
    parseError("foo[0m]")
    parseError("foo[1i5m]")
    parseError("foo[2i5i]")
    parseError("foo[3m4i]")
    parseError("foo[5m] LIMIT 1m")
    parseError("foo[\"5m\"]")
    parseError("foo[]")
    parseError("foo[1]")
    parseError("some_metric[5m] OFFSET 1")
    parseError("some_metric[5m] OFFSET 1mm")
    parseError("some_metric[5m] OFFSET 1m5i")
    parseError("some_metric[5m] OFFSET 5m2i")
    parseError("some_metric[5m] OFFSET")
    parseError("some_metric[5m] LIMIT")
    parseError("some_metric OFFSET 1m[5m]")
    parseError("some_metric LIMIT 1m[5m]")
    parseError("(foo + bar)[5m]")

    parseSuccessfully("sum by (foo)(some_metric)")
    parseSuccessfully("avg by (foo)(some_metric)")
    parseSuccessfully("max by (foo)(some_metric)")
    parseSuccessfully("sum without (foo) (some_metric)")
    parseSuccessfully("sum (some_metric) without (foo)")
    parseSuccessfully("stddev(some_metric)")
    parseSuccessfully("stdvar by (foo)(some_metric)")
    parseSuccessfully("sum by ()(some_metric)")
    parseSuccessfully("topk(5, some_metric)")
    parseSuccessfully("group(some_metric)")
    parseSuccessfully("group by(foo)(some_metric)")
    parseSuccessfully("count_values(\"value\",some_metric)")
    parseSuccessfully("sum without(and, by, avg, count, alert, annotations)(some_metric)")
    parseSuccessfully("sum:some_metric:dataset:1m{_ws_=\"some_workspace\", _ns_=\"some_namespace\"}")
    parseSuccessfully("count:some_metric:dataset:1m{_ws_=\"some_workspace\", _ns_=\"some_namespace\"}")
    parseSuccessfully("avg:some_metric:dataset:1m{_ws_=\"some_workspace\", _ns_=\"some_namespace\"}")
    parseSuccessfully("min:some_metric:dataset:1m{_ws_=\"some_workspace\", _ns_=\"some_namespace\"}")
    parseSuccessfully("max:some_metric:dataset:1m{_ws_=\"some_workspace\", _ns_=\"some_namespace\"}")
    parseSuccessfully("stddev:some_metric:dataset:1m{_ws_=\"some_workspace\", _ns_=\"some_namespace\"}")
    parseSuccessfully("stdvar:some_metric:dataset:1m{_ws_=\"some_workspace\", _ns_=\"some_namespace\"}")

    parseError("sum_over_time(foo)")
    parseError("sum(other_metric) by (foo)(some_metric)")
    parseError("sum without(==)(some_metric)")
    parseError("MIN keep_common (some_metric)")
    parseError("MIN (some_metric) keep_common")
    parseError("sum some_metric by (test)")
    parseError("sum (some_metric) by test")
    parseError("sum (some_metric) by test")
    parseError("sum () by (test)")
    parseError("sum (some_metric) without (test) by (test)")
    parseError("sum without (test) (some_metric) by (test)")
    parseError("topk(some_metric)")
    //    parseError("topk(some_metric, other_metric)")
    //    parseError("count_values(5, other_metric)")

    parseSuccessfully("time()")
    parseSuccessfully("floor(some_metric{foo!=\"bar\"})")
    parseSuccessfully("rate(some_metric[5m])")
    parseSuccessfully("last(some_metric[5m])")
    parseSuccessfully("round(some_metric)")
    parseSuccessfully("round(some_metric, 5)")
    parseSuccessfully("log2(some_metric, 5)")
    parseSuccessfully("sgn(some_metric)")


    //        parseError(  "floor()")
    //        parseError(  "floor(some_metric, other_metric)")
    //        parseError(  "floor(1)")
    parseError("non_existent_function_far_bar()")
    //        parseError(  "rate(some_metric)")
    parseError("label_replace(a, `b`, `cff`, `d`, `.*`)")
    parseError("-=")
    parseError("++-++-+-+-<")
    parseError("e-+=/(0)")
    //    parseError(  "-If")

    parseSuccessfully("quantiler{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("Counter0{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("Summer0{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("Avgger0{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("topkker{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("bottomkker{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("stddevr{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("stdvarr{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("maxer{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("miner{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("minn{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("count0{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("offset1{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("limit1{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("by1{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("with2{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("without3{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("or1{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("and1{job=\"SNRT-App-0\"}[1m] ")
    parseSuccessfully("and{job=\"SNRT-App-0\"}[1m] ")

    parseSuccessfully("foo{job=\"SNRT-App-0\"}[5i] ")
    parseSuccessfully("sum(rate(foo{job=\"SNRT-App-0\"}[5i]))")
    parseSuccessfully("rate(foo{job=\"SNRT-App-0\"}[5i]) + rate(bar{job=\"SNRT-App-0\"}[4i]) ")

    // negative/positive test-cases for functions in RangeFunctionID
    // avg_over_time
    parseSuccessfully("avg_over_time(some_metric[5m])")
    parseError("avg_over_time(some_metric)") // reason : Expected range-vector
    parseError("avg_over_time(some_metric[5m], hello)") // reason : Expected only 1 arg, got 2
    parseError("avg_over_time(abcd, some_metric[5m])") // reason : Expected range, got instant

    // changes
    parseSuccessfully("changes(some_metric[5m])")
    parseError("changes(some_metric)")  // reason : Expected range-vector
    parseError("changes(some_metric[5m], hello)") // reason : Expected only 1 arg, got 2
    parseError("changes(abcd, some_metric[5m])") // reason : Expected range, got instant

    // count_over_time
    parseSuccessfully("count_over_time(some_metric[5m])")
    parseError("count_over_time(some_metric)")  // reason : Expected range-vector
    parseError("count_over_time(some_metric[5m], hello)") // reason : Expected only 1 arg, got 2
    parseError("count_over_time(hello, some_metric[5m])") // reason : Expected range, got instant

    // delta
    parseSuccessfully("delta(some_metric[5m])")
    parseError("delta(some_metric)") // reason : Expected range-vector
    parseError("delta(some_metric[5m], hello)") // reason : Expected only 1 arg, got 2
    parseError("delta(hello, some_metric[5m])") // reason : Expected range, got instant

    // deriv
    parseSuccessfully("deriv(some_metric[5m])")
    parseError("delta(some_metric)") // reason : Expected range-vector
    parseError("deriv(some_metric[5m], hello)") // reason : Expected only 1 arg, got 2
    parseError("deriv(hello, some_metric[5m])") // reason : Expected range, got instant

    // holt_winters
    parseSuccessfully("holt_winters(some_metric[5m], 0.5, 0.5)")
    parseError("holt_winters(some_metric, 0.5, 0.5)") // reason : Expected range-vector, got instant
    parseError("holt_winters(some_metric[5m])") // reason : Expected 3 args, got 1
    parseError("holt_winters(some_metric[5m], 1, 0.1 )") // reason : Invalid smoothing value, 0<sf<1
    parseError("holt_winters(some_metric[5m], 0.1, 100 )") // reason : Invalid trend value, 0<sf<1
    parseError("holt_winters(some_metric[5m], 100, 100 )") // reason : Invalid trend value, 0<sf<1
    parseError("holt_winters(0.1, 0.1, some_metric[5m])") // reason : Expected range-vector, got scalar
    parseError("holt_winters(some_metric[5m], 0.5, 0.5, hello)") // reason :Expected 3 args, got 4

    // ZScore
    parseSuccessfully("z_score(some_metric[5m])")
    parseError("z_score(some_metric)") // reason : Expected range-vector
    parseError("z_score(some_metric[5m], hello)") // reason : Expected only 1 arg, got 2
    parseError("z_score(hello, some_metric[5m])") // reason : Expected range, got instant

    // Idelta
    parseSuccessfully("idelta(some_metric[5m])")
    parseError("idelta(some_metric)") // reason : Expected range-vector
    parseError("idelta(some_metric[5m], hello)") // reason : Expected only 1 arg, got 2
    parseError("idelta(hello, some_metric[5m])") // reason : Expected range, got instant

    // Increase
    parseSuccessfully("increase(some_metric[5m])")
    parseError("increase(some_metric)") // reason : Expected range-vector
    parseError("increase(some_metric[5m], hello)") // reason : Expected only 1 arg, got 2
    parseError("increase(hello, some_metric[5m])") // reason : Expected range, got instant

    // Irate
    parseSuccessfully("irate(some_metric[5m])")
    parseError("irate(some_metric)") // reason : Expected range-vector
    parseError("irate(some_metric[5m], hello)") // reason : Expected only 1 arg, got 2
    parseError("irate(hello, some_metric[5m])") // reason : Expected range, got instant

    // MaxOverTime
    parseSuccessfully("max_over_time(some_metric[5m])")
    parseError("max_over_time(some_metric)") // reason : Expected range-vector
    parseError("max_over_time(some_metric[5m], hello)") // reason : Expected only 1 arg, got 2
    parseError("max_over_time(hello, some_metric[5m])") // reason : Expected range, got instant

    // MinOverTime
    parseSuccessfully("min_over_time(some_metric[5m])")
    parseError("min_over_time(some_metric)") // reason : Expected range-vector
    parseError("min_over_time(some_metric[5m], hello)") // reason : Expected only 1 arg, got 2
    parseError("min_over_time(hello, some_metric[5m])") // reason : Expected range, got instant

    // predict_linear
    parseSuccessfully("predict_linear(some_metric[5m], 0.5)")
    parseError("predict_linear(some_metric, 0.5)") // reason : Expected range-vector, got instant
    parseError("predict_linear(some_metric[5m])") // reason : Expected 2 args, got 1
    parseError("predict_linear(1, some_metric[5m])") // reason : Expected range-vector, got scalar
    parseError("predict_linear(some_metric[5m], 1, hello)") // reason :Expected 2 args, got 3

    // quantile_over_time
    parseSuccessfully("quantile_over_time(1, some_metric[5m])")
    parseError("quantile_over_time(0.5, some_metric)") // reason : Expected range-vector, got instant
    parseError("quantile_over_time(some_metric[5m])") // reason : Expected 2 args, got 1
    parseError("quantile_over_time(some_metric[5m], 1)") // reason : Expected scalar, got range_vector
    parseError("quantile_over_time(1, some_metric[5m], hello)") // reason :Expected 2 args, got 3

    // Rate
    parseSuccessfully("rate(some_metric[5m])")
    parseError("rate(some_metric)") // reason : Expected range-vector
    parseError("rate(some_metric[5m], hello)") // reason : Expected only 1 arg, got 2
    parseError("rate(hello, some_metric[5m])") // reason : Expected range, got instant

    //  Resets
    parseSuccessfully("resets(some_metric[5m])")
    parseError("resets(some_metric)") // reason : Expected range-vector
    parseError("resets(some_metric[5m], hello)") // reason : Expected only 1 arg, got 2
    parseError("resets(hello, some_metric[5m])") // reason : Expected range, got instant

    //  StdDevOverTime
    parseSuccessfully("stddev_over_time(some_metric[5m])")
    parseError("std_over_time(some_metric)") // reason : Expected range-vector
    parseError("std_over_time(some_metric[5m], hello)") // reason : Expected only 1 arg, got 2
    parseError("std_over_time(hello, some_metric[5m])") // reason : Expected range, got instant

    //  StdVarOverTime
    parseSuccessfully("stdvar_over_time(some_metric[5m])")
    parseError("stdvar_over_time(some_metric)") // reason : Expected range-vector
    parseError("stdvar_over_time(some_metric[5m], hello)") // reason : Expected only 1 arg, got 2
    parseError("stdvar_over_time(hello, some_metric[5m])") // reason : Expected range, got instant

    //  SumOverTime
    parseSuccessfully("sum_over_time(some_metric[5m]) limit 10")
    parseError("sum_over_time(some_metric)") // reason : Expected range-vector
    parseError("sum_over_time(some_metric[5m], hello)") // reason : Expected only 1 arg, got 2
    parseError("sum_over_time(hello, some_metric[5m])") // reason : Expected range, got instant

    // regexp length
    parseError(s"sum_over_time(some_metric{longregex~='${"f"*1001}'})") // reason : Regex len > 1000
    parseError(s"sum_over_time(some_metric{longregex~!'${"f"*1001}'})") // reason : Regex len > 1000

    //  Timestamp
    parseSuccessfully("timestamp(some_metric)")
    parseError("timestamp(some_metric[5m])") // reason : Expected instant vector, got range vector
    parseError("timestamp(some_metric, hello)") // reason : Expected only 1 arg, got 2

    // Trailing Commas
    parseSuccessfully("sum without(and, by, avg, count, alert, annotations,)(some_metric)")
    parseSuccessfully("sum without(and, by, avg, count, alert, annotations, )(some_metric)")
    parseSuccessfully("sum by(and, by, avg, count, alert, annotations, )(some_metric)")

    // Trailing Commas in Binary Joins
    parseSuccessfully("foo and ignoring(test,blub,) bar")
    parseSuccessfully("foo and ignoring(test,blub, ) bar")

    parseSuccessfully("foo / on(test,blub, ) group_left(bar) bar")
    parseSuccessfully("foo / ignoring(test,blub,) group_left(blub) bar")

    parseSuccessfully("foo - on(test,blub,) group_right(bar,foo,) bar")
    parseSuccessfully("foo - ignoring(test,blub,) group_right(bar,foo, ) bar")
  }

  it("parse long identifiers") {
    // This should not cause a stack overflow error.

    val bob = new StringBuilder().append("requests{job=\"")
    for (i <- 1 to 100) {
      bob.append("abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_")
    }

    parseSuccessfully(bob.append("\"}").toString())
  }

  it("parse subqueries") {
    parseSubquery("min_over_time( rate(http_requests_total[5m])[30m:1m] )")
    parseSubquery("max_over_time( deriv( rate(distance_covered_meters_total[1m])[5m:1m] )[10m:] )")
    parseSubquery("max_over_time((time() - max(foo) < 1000)[5m:10s] offset 5m)")
    parseSubquery("max_over_time((time() - max(foo) < 1000)[5m:10s]) limit 5")
    parseSubquery("max_over_time((time() - max(foo) < 1000)[5m:10s] offset 5m) limit 2")
    parseSubquery("avg_over_time(rate(demo_cpu_usage_seconds_total[1m])[2m:10s])")

    parseSubquery("foo[5m:1m]")
    parseSubquery("foo[5m:]")
    parseSubquery("max_over_time(rate(foo[5m])[5m:1m])")
    parseSubquery("max_over_time(sum(foo)[5m:1m])")
    parseSubquery("sum(foo)[5m:1m]")
    parseSubquery("group(foo)[5m:1m]")
    parseSubquery("log2(foo)[5m:1m]")
    parseSubquery("log2(foo)[5m:]")
    parseSubquery("sgn(foo)[5m:]")
    parseSubquery("(foo + bar)[5m:1m]")
    parseSubquery("sum_over_time((foo + bar)[5m:1m])")
    parseSubquery("avg_over_time(max_over_time(rate(foo[5m])[5m:1m])[10m:2m])")
    parseSubquery("sum(rate(foo[5m])[5m:1m])")
    parseSubquery("log2(rate(foo[5m])[5m:1m])")
    parseSubquery("sgn(rate(foo[5m])[5m:1m])")

    parseSubqueryError("log2(foo)[5m][5m:1m]")
    parseSubqueryError("sum(foo)[5m]")
    parseSubqueryError("log2(foo)[5m:1m][5m:1m]")
    parseSubqueryError("sgn(foo)[5m:1m][5m:1m]")
  }

  // TODO
  // it's impossible to parse subqueries with offsets and binary expressions
  // using Packrat parsers, here we will be explicitely calling antlr parser until antlr
  // becomes the default parser
  it("parse subquery using antl"){
    parseWithAntlr(
      "min_over_time(rate(http_requests_total[5m])[30m:1m] offset 1m) limit 10",
      "ApplyLimitFunction(SubqueryWithWindowing(PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524854160000,1524855900000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524854160000,60000,1524855900000,300000,Rate,false,List(),None,List(ColumnFilter(__name__,Equals(http_requests_total)))),1524855988000,0,1524855988000,MinOverTime,List(),1800000,60000,Some(60000)),List(),RangeParams(1524855988,1000,1524855988),10)"
    )
    parseWithAntlr(
      "(heap_usage + heap_usage)[5m:1m]",
      "TopLevelSubquery(BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855720000,1524855960000),List(ColumnFilter(__name__,Equals(heap_usage))),List(),Some(300000),None),1524855720000,60000,1524855960000,None),ADD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855720000,1524855960000),List(ColumnFilter(__name__,Equals(heap_usage))),List(),Some(300000),None),1524855720000,60000,1524855960000,None),List(),List(),List()),1524855720000,60000,1524855960000,300000,None)"
    )
    parseWithAntlr(
      "heap_usage[10m:1m] offset 30s",
      "TopLevelSubquery(PeriodicSeries(RawSeries(IntervalSelector(1524855360000,1524855900000),List(ColumnFilter(__name__,Equals(heap_usage))),List(),Some(300000),None),1524855360000,60000,1524855900000,None),1524855360000,60000,1524855900000,600000,Some(30000))"
    )

    parseWithAntlr(
      "sum_over_time(heap_usage[10m:1m] offset 3m)[2m:1m] offset 1m",
      "TopLevelSubquery(SubqueryWithWindowing(PeriodicSeries(RawSeries(IntervalSelector(1524855060000,1524855720000),List(ColumnFilter(__name__,Equals(heap_usage))),List(),Some(300000),None),1524855060000,60000,1524855720000,None),1524855840000,60000,1524855900000,SumOverTime,List(),600000,60000,Some(180000)),1524855840000,60000,1524855900000,120000,Some(60000))"
    )
    parseWithAntlr(
      "sum_over_time(heap_usage[3m:1m] offset 3m)",
      "SubqueryWithWindowing(PeriodicSeries(RawSeries(IntervalSelector(1524855660000,1524855780000),List(ColumnFilter(__name__,Equals(heap_usage))),List(),Some(300000),None),1524855660000,60000,1524855780000,None),1524855988000,0,1524855988000,SumOverTime,List(),180000,60000,Some(180000))"
    )
    parseWithAntlr(
      "sum_over_time(sum_over_time(heap_usage[3m:1m] offset 3m)[2m:1m] offset 1m)",
      "SubqueryWithWindowing(SubqueryWithWindowing(PeriodicSeries(RawSeries(IntervalSelector(1524855480000,1524855720000),List(ColumnFilter(__name__,Equals(heap_usage))),List(),Some(300000),None),1524855480000,60000,1524855720000,None),1524855840000,60000,1524855900000,SumOverTime,List(),180000,60000,Some(180000)),1524855988000,0,1524855988000,SumOverTime,List(),120000,60000,Some(60000))"
    )
  }

  it("parse expressions with unary operators") {
    parseWithAntlr("-1", """ScalarBinaryOperation(SUB,Left(0.0),Left(1.0),RangeParams(1524855988,1000,1524855988))""")
    parseWithAntlr("-foo", """ScalarVectorBinaryOperation(SUB,ScalarFixedDoublePlan(0.0,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),true)""")
    parseWithAntlr("foo * -1","""BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),MUL,OneToOne,ScalarBinaryOperation(SUB,Left(0.0),Left(1.0),RangeParams(1524855988,1000,1524855988)),List(),List(),List())""")
    parseWithAntlr("-1 * foo", """BinaryJoin(ScalarBinaryOperation(SUB,Left(0.0),Left(1.0),RangeParams(1524855988,1000,1524855988)),MUL,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List())""")
    parseWithAntlr("-1 * -foo", """BinaryJoin(ScalarBinaryOperation(SUB,Left(0.0),Left(1.0),RangeParams(1524855988,1000,1524855988)),MUL,OneToOne,ScalarVectorBinaryOperation(SUB,ScalarFixedDoublePlan(0.0,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),true),List(),List(),List())""")
    parseWithAntlr("sum(foo) < -1", """BinaryJoin(Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),None),LSS,OneToOne,ScalarBinaryOperation(SUB,Left(0.0),Left(1.0),RangeParams(1524855988,1000,1524855988)),List(),List(),List())""")
    parseWithAntlr("-sum(foo) < -1", "BinaryJoin(ScalarVectorBinaryOperation(SUB,ScalarFixedDoublePlan(0.0,RangeParams(1524855988,1000,1524855988)),Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),None),true),LSS,OneToOne,ScalarBinaryOperation(SUB,Left(0.0),Left(1.0),RangeParams(1524855988,1000,1524855988)),List(),List(),List())")
    parseWithAntlr("sum(-foo) < -1", """BinaryJoin(Aggregate(Sum,ScalarVectorBinaryOperation(SUB,ScalarFixedDoublePlan(0.0,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),true),List(),None),LSS,OneToOne,ScalarBinaryOperation(SUB,Left(0.0),Left(1.0),RangeParams(1524855988,1000,1524855988)),List(),List(),List())""")
    parseWithAntlr("-sum(-foo) < -1", """BinaryJoin(ScalarVectorBinaryOperation(SUB,ScalarFixedDoublePlan(0.0,RangeParams(1524855988,1000,1524855988)),Aggregate(Sum,ScalarVectorBinaryOperation(SUB,ScalarFixedDoublePlan(0.0,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),true),List(),None),true),LSS,OneToOne,ScalarBinaryOperation(SUB,Left(0.0),Left(1.0),RangeParams(1524855988,1000,1524855988)),List(),List(),List())""")

    parseWithAntlr("+1", """ScalarBinaryOperation(ADD,Left(0.0),Left(1.0),RangeParams(1524855988,1000,1524855988))""")
    parseWithAntlr("+foo", """ScalarVectorBinaryOperation(ADD,ScalarFixedDoublePlan(0.0,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),true)""")
    parseWithAntlr("foo * +1", """BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),MUL,OneToOne,ScalarBinaryOperation(ADD,Left(0.0),Left(1.0),RangeParams(1524855988,1000,1524855988)),List(),List(),List())""")
    parseWithAntlr("+1 * foo", """BinaryJoin(ScalarBinaryOperation(ADD,Left(0.0),Left(1.0),RangeParams(1524855988,1000,1524855988)),MUL,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List())""")
    parseWithAntlr("+1 * +foo", """BinaryJoin(ScalarBinaryOperation(ADD,Left(0.0),Left(1.0),RangeParams(1524855988,1000,1524855988)),MUL,OneToOne,ScalarVectorBinaryOperation(ADD,ScalarFixedDoublePlan(0.0,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),true),List(),List(),List())""")
    parseWithAntlr("sum(foo) < +1", """BinaryJoin(Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),None),LSS,OneToOne,ScalarBinaryOperation(ADD,Left(0.0),Left(1.0),RangeParams(1524855988,1000,1524855988)),List(),List(),List())""")
    parseWithAntlr("+sum(foo) < +1", "BinaryJoin(ScalarVectorBinaryOperation(ADD,ScalarFixedDoublePlan(0.0,RangeParams(1524855988,1000,1524855988)),Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),None),true),LSS,OneToOne,ScalarBinaryOperation(ADD,Left(0.0),Left(1.0),RangeParams(1524855988,1000,1524855988)),List(),List(),List())")
    parseWithAntlr("sum(+foo) < +1", """BinaryJoin(Aggregate(Sum,ScalarVectorBinaryOperation(ADD,ScalarFixedDoublePlan(0.0,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),true),List(),None),LSS,OneToOne,ScalarBinaryOperation(ADD,Left(0.0),Left(1.0),RangeParams(1524855988,1000,1524855988)),List(),List(),List())""")
    parseWithAntlr("+sum(+foo) < +1", """BinaryJoin(ScalarVectorBinaryOperation(ADD,ScalarFixedDoublePlan(0.0,RangeParams(1524855988,1000,1524855988)),Aggregate(Sum,ScalarVectorBinaryOperation(ADD,ScalarFixedDoublePlan(0.0,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),true),List(),None),true),LSS,OneToOne,ScalarBinaryOperation(ADD,Left(0.0),Left(1.0),RangeParams(1524855988,1000,1524855988)),List(),List(),List())""")

    parseWithAntlr("+-1", """ScalarVectorBinaryOperation(ADD,ScalarFixedDoublePlan(0.0,RangeParams(1524855988,1000,1524855988)),ScalarBinaryOperation(SUB,Left(0.0),Left(1.0),RangeParams(1524855988,1000,1524855988)),true)""")
  }

  it("Should be able to make logical plans for Series Expressions") {
    val queryToLpString = Map(
      "http_requests_total + time()" -> "ScalarVectorBinaryOperation(ADD,ScalarTimeBasedPlan(Time,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),false)",
      "time()" -> "ScalarTimeBasedPlan(Time,RangeParams(1524855988,1000,1524855988))",
      "hour()" -> "ScalarTimeBasedPlan(Hour,RangeParams(1524855988,1000,1524855988))",
      "scalar(http_requests_total)" -> "ScalarVaryingDoublePlan(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000),Scalar,List())",
      "scalar(http_requests_total) + node_info" ->
        "ScalarVectorBinaryOperation(ADD,ScalarVaryingDoublePlan(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),Scalar,List()),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(node_info))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),true)",
      "10 + http_requests_total" -> "ScalarVectorBinaryOperation(ADD,ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),true)",
      "vector(3)" -> "VectorPlan(ScalarFixedDoublePlan(3.0,RangeParams(1524855988,1000,1524855988)))",
      "vector(scalar(http_requests_total))" -> "VectorPlan(ScalarVaryingDoublePlan(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),Scalar,List()))",
      "scalar(http_requests_total)" -> "ScalarVaryingDoublePlan(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),Scalar,List())",
      "10 + http_requests_total * 5" ->
        "ScalarVectorBinaryOperation(ADD,ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988)),ScalarVectorBinaryOperation(MUL,ScalarFixedDoublePlan(5.0,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),false),true)",
      "10 + (http_requests_total * 5)" ->
        "ScalarVectorBinaryOperation(ADD,ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988)),ScalarVectorBinaryOperation(MUL,ScalarFixedDoublePlan(5.0,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),false),true)",
      "(10 + http_requests_total) * 5" ->
        "ScalarVectorBinaryOperation(MUL,ScalarFixedDoublePlan(5.0,RangeParams(1524855988,1000,1524855988)),ScalarVectorBinaryOperation(ADD,ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),true),false)",
      "topk(5, http_requests_total)" ->
        "Aggregate(TopK,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(5.0),None)",
      "topk(5, http_requests_total::foo)" ->
        "Aggregate(TopK,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(foo),Some(300000),None),1524855988000,1000000,1524855988000,None),List(5.0),None)",
      "topk(5, http_requests_total::foo::bar)" ->
        "Aggregate(TopK,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total::foo))),List(bar),Some(300000),None),1524855988000,1000000,1524855988000,None),List(5.0),None)",
      "stdvar(http_requests_total)" ->
        "Aggregate(Stdvar,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),None)",
      "stddev(http_requests_total)" ->
        "Aggregate(Stddev,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),None)",
      "group(http_requests_total)" ->
        "Aggregate(Group,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),None)",
      "irate(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,Irate,false,List(),None,List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))))",
      "idelta(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,Idelta,false,List(),None,List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))))",
      "resets(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,Resets,false,List(),None,List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))))",
      "deriv(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,Deriv,false,List(),None,List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))))",
      "rate(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,Rate,false,List(),None,List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))))",
      "last(jvm_memory{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(jvm_memory))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,Last,false,List(),None,List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(jvm_memory))))",
      "http_requests_total{job=\"prometheus\"}[5m]" ->
        "RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(prometheus)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None)",
      "http_requests_total::sum{job=\"prometheus\"}[5m]" ->
        "RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(prometheus)), ColumnFilter(__name__,Equals(http_requests_total))),List(sum),Some(300000),None)",
      "http_requests_total::foo::sum{job=\"prometheus\"}[5m]" ->
        "RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(prometheus)), ColumnFilter(__name__,Equals(http_requests_total::foo))),List(sum),Some(300000),None)",
      "http_requests_total offset 5m" ->
        "PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),Some(300000)),1524855988000,1000000,1524855988000,Some(300000))",
      "http_requests_total offset 0.5m" ->
        "PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),Some(30000)),1524855988000,1000000,1524855988000,Some(30000))",
      "http_requests_total{environment=~\"staging|testing|development\",method!=\"GET\"}" ->
        "PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(environment,EqualsRegex(staging|testing|development)), ColumnFilter(method,NotEquals(GET)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None)",

      "http_req_latency{job=\"api-server\",_bucket_=\"2.5\"}" ->
        "ApplyInstantFunction(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_req_latency))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),HistogramBucket,List(ScalarFixedDoublePlan(2.5,RangeParams(0,9223372036854775807,60000))))",
      "rate(http_req_latency{job=\"api-server\",_bucket_=\"2.5\"}[5m])" ->
        "PeriodicSeriesWithWindowing(ApplyInstantFunctionRaw(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_req_latency))),List(),Some(300000),None),HistogramBucket,List(ScalarFixedDoublePlan(2.5,RangeParams(0,9223372036854775807,60000)))),1524855988000,1000000,1524855988000,300000,Rate,false,List(),None,List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_req_latency))))",

      "method_code:http_errors:rate5m / ignoring(code) group_left method:http_requests:rate5m" ->
        "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(method_code:http_errors:rate5m))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),DIV,ManyToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(method:http_requests:rate5m))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(code),List())",
      "method_code:http_errors:rate5m / ignoring(code) group_left(mode, instance) method:http_requests:rate5m" ->
        "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(method_code:http_errors:rate5m))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),DIV,ManyToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(method:http_requests:rate5m))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(code),List(mode, instance))",
      "method_code:http_errors:rate5m / ignoring(code) group_right method:http_requests:rate5m" ->
        "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(method_code:http_errors:rate5m))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),DIV,OneToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(method:http_requests:rate5m))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(code),List())",

      "increase(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,Increase,false,List(),None,List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))))",
      "sum(http_requests_total{method=\"GET\"} offset 5m)" ->
        "Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(method,Equals(GET)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),Some(300000)),1524855988000,1000000,1524855988000,Some(300000)),List(),None)",
      """{__name__="foo\\\"\n\t",job="myjob"}[5m]""" ->
        "RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo\\\"\n\t)), ColumnFilter(job,Equals(myjob))),List(),Some(300000),None)",
      "{__name__=\"foo\",job=\"myjob\"}" ->
        "PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo)), ColumnFilter(job,Equals(myjob))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None)",
      "{__name__=\"foo\",job=\"myjob\"}[5m]" ->
        "RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo)), ColumnFilter(job,Equals(myjob))),List(),Some(300000),None)",
      "sum({__name__=\"foo\",job=\"myjob\"})" ->
        "Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo)), ColumnFilter(job,Equals(myjob))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),None)",
      "sum(http_requests_total)       \n \n / \n\n    sum(http_requests_total)" ->
        "BinaryJoin(Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),None),DIV,OneToOne,Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),None),List(),List(),List())",

      "changes(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,Changes,false,List(),None,List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))))",
      "quantile_over_time(0.4,http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,QuantileOverTime,false,List(ScalarFixedDoublePlan(0.4,RangeParams(1524855988,1000,1524855988))),None,List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))))",
       "label_replace(http_requests_total,instance,new-label,instance,\"(.*)-(.*)\")" -> "ApplyMiscellaneousFunction(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),LabelReplace,List(instance, new-label, instance, (.*)-(.*)))",
      "hist_to_prom_vectors(http_request_latency)" ->
        "ApplyMiscellaneousFunction(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_request_latency))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),HistToPromVectors,List())",
      "holt_winters(http_requests_total{job=\"api-server\"}[5m], 0.01, 0.1)" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,HoltWinters,false,List(ScalarFixedDoublePlan(0.01,RangeParams(1524855988,1000,1524855988)), ScalarFixedDoublePlan(0.1,RangeParams(1524855988,1000,1524855988))),None,List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))))",
      "z_score(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,ZScore,false,List(),None,List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))))",
      "predict_linear(http_requests_total{job=\"api-server\"}[5m], 10)" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,PredictLinear,false,List(ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988))),None,List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))))",

      // Binary Expressions should generate Logical Plan according to precedence
     // Logical plan generated when expression does not have brackets according to precedence is same as logical plan for expression with brackets which are according to precedence
      "(10 % http_requests_total) + 5" ->
        "ScalarVectorBinaryOperation(ADD,ScalarFixedDoublePlan(5.0,RangeParams(1524855988,1000,1524855988)),ScalarVectorBinaryOperation(MOD,ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),true),false)",
      "10 % http_requests_total + 5" ->
        "ScalarVectorBinaryOperation(ADD,ScalarFixedDoublePlan(5.0,RangeParams(1524855988,1000,1524855988)),ScalarVectorBinaryOperation(MOD,ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),true),false)",

      "(http_requests_total % http_requests_total) + http_requests_total" ->
        "BinaryJoin(BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),MOD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),ADD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List())",
      "http_requests_total % http_requests_total + http_requests_total" ->
        "BinaryJoin(BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),MOD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),ADD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List())",

      // "unless" and "and" have same priority but are not right associative so "and" should be evaluated first
      "((foo and bar) unless baz) or qux" ->
        "BinaryJoin(BinaryJoin(BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),LAND,ManyToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(bar))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),LUnless,ManyToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(baz))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),LOR,ManyToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(qux))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List())",
      "foo and bar unless baz or qux" ->
      "BinaryJoin(BinaryJoin(BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),LAND,ManyToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(bar))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),LUnless,ManyToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(baz))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),LOR,ManyToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(qux))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List())",

      // Pow is right associative so (bar ^ baz) should be evaluated first
      "(foo ^ (bar ^ baz))" ->
        "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),POW,OneToOne,BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(bar))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),POW,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(baz))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),List(),List(),List())",
      "foo ^ bar ^ baz" ->
        "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),POW,OneToOne,BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(bar))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),POW,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(baz))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),List(),List(),List())",

      "(foo + bar) or (bla and blub)" ->
        "BinaryJoin(BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),ADD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(bar))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),LOR,ManyToMany,BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(bla))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),LAND,ManyToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(blub))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),List(),List(),List())",
      "foo + bar or bla and blub" ->
        "BinaryJoin(BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),ADD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(bar))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),LOR,ManyToMany,BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(bla))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),LAND,ManyToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(blub))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),List(),List(),List())",

      "bar + on(foo) (bla / on(baz, buz) group_right(test) blub)" -> "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(bar))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),ADD,OneToOne,BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(bla))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),DIV,OneToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(blub))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(baz, buz),List(),List(test)),List(foo),List(),List())",
      "bar + on(foo) bla / on(baz, buz) group_right(test) blub" -> "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(bar))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),ADD,OneToOne,BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(bla))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),DIV,OneToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(blub))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(baz, buz),List(),List(test)),List(foo),List(),List())",
      "sort(http_requests_total)" ->
        "ApplySortFunction(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),Sort)",
      "sort_desc(http_requests_total)" ->
        "ApplySortFunction(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),SortDesc)",
      "absent(http_requests_total{host=\"api-server\"})" -> "ApplyAbsentFunction(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(host,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(ColumnFilter(host,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),RangeParams(1524855988,1000,1524855988),List())",
      "count_values(\"freq\", http_requests_total)" ->
        "Aggregate(CountValues,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(freq),None)",
      "timestamp(http_requests_total)" -> "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,0,Timestamp,false,List(),None,List())",
      "sum:some_metric:dataset:1m{_ws_=\"demo\", _ns_=\"test\"}" -> "PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(test)), ColumnFilter(__name__,Equals(sum:some_metric:dataset:1m))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None)",
      "1 + 2 * 3" -> "ScalarBinaryOperation(ADD,Left(1.0),Right(ScalarBinaryOperation(MUL,Left(2.0),Left(3.0),RangeParams(1524855988,1000,1524855988))),RangeParams(1524855988,1000,1524855988))",

      // step multiple tests
      "http_requests_total offset 2i" ->
        "PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),Some(2000000)),1524855988000,1000000,1524855988000,Some(2000000))",
      "sum(rate(foo{job=\"SNRT-App-0\"}[5i]))" -> "Aggregate(Sum,PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(SNRT-App-0)), ColumnFilter(__name__,Equals(foo))),List(),Some(5000000),None),1524855988000,1000000,1524855988000,5000000,Rate,true,List(),None,List(ColumnFilter(job,Equals(SNRT-App-0)), ColumnFilter(__name__,Equals(foo)))),List(),None)",
      "rate(foo{job=\"SNRT-App-0\"}[5i]) + rate(bar{job=\"SNRT-App-0\"}[4i])" -> "BinaryJoin(PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(SNRT-App-0)), ColumnFilter(__name__,Equals(foo))),List(),Some(5000000),None),1524855988000,1000000,1524855988000,5000000,Rate,true,List(),None,List(ColumnFilter(job,Equals(SNRT-App-0)), ColumnFilter(__name__,Equals(foo)))),ADD,OneToOne,PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(SNRT-App-0)), ColumnFilter(__name__,Equals(bar))),List(),Some(4000000),None),1524855988000,1000000,1524855988000,4000000,Rate,true,List(),None,List(ColumnFilter(job,Equals(SNRT-App-0)), ColumnFilter(__name__,Equals(bar)))),List(),List(),List())",
      "sum(rate(foo{job=\"SNRT-App-0\"}[0.5i]))" -> "Aggregate(Sum,PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(SNRT-App-0)), ColumnFilter(__name__,Equals(foo))),List(),Some(500000),None),1524855988000,1000000,1524855988000,500000,Rate,true,List(),None,List(ColumnFilter(job,Equals(SNRT-App-0)), ColumnFilter(__name__,Equals(foo)))),List(),None)",
     "http_requests_total - 10/2" -> "ScalarVectorBinaryOperation(SUB,ScalarBinaryOperation(DIV,Left(10.0),Left(2.0),RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),false)",
      "foo - http_requests_total * 2^3" -> "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),SUB,OneToOne,ScalarVectorBinaryOperation(MUL,ScalarBinaryOperation(POW,Left(2.0),Left(3.0),RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),false),List(),List(),List())",
      "sum(http_requests_total) - 10/2" -> "ScalarVectorBinaryOperation(SUB,ScalarBinaryOperation(DIV,Left(10.0),Left(2.0),RangeParams(1524855988,1000,1524855988)),Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),None),false)",
      "ceil(vector(100) / 10 / 10)" -> "ApplyInstantFunction(ScalarVectorBinaryOperation(DIV,ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988)),ScalarVectorBinaryOperation(DIV,ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988)),VectorPlan(ScalarFixedDoublePlan(100.0,RangeParams(1524855988,1000,1524855988))),false),false),Ceil,List())",
      "ceil(sum(foo) / 10 / 10 / 10)" -> "ApplyInstantFunction(ScalarVectorBinaryOperation(DIV,ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988)),ScalarVectorBinaryOperation(DIV,ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988)),ScalarVectorBinaryOperation(DIV,ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988)),Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),None),false),false),false),Ceil,List())",
      "metric1 * metric2 + metric3" -> "BinaryJoin(BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric1))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),MUL,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric2))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),ADD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric3))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List())",
      "metric1 * (metric2 + metric3)" -> "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric1))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),MUL,OneToOne,BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric2))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),ADD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric3))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),List(),List(),List())",

      "(metric1 + (metric2 * metric3)) + metric4" -> "BinaryJoin(BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric1))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),ADD,OneToOne,BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric2))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),MUL,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric3))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),List(),List(),List()),ADD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric4))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List())",
      "metric1 + metric2 * metric3 + metric4" -> "BinaryJoin(BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric1))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),ADD,OneToOne,BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric2))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),MUL,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric3))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),List(),List(),List()),ADD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric4))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List())",

      "(metric1 + metric2) * (metric3 + metric4)" -> "BinaryJoin(BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric1))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),ADD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric2))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),MUL,OneToOne,BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric3))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),ADD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(metric4))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),List(),List(),List())",
      "count((some_metric / 300) >= 1 ) or vector(0)" -> "BinaryJoin(Aggregate(Count,ScalarVectorBinaryOperation(GTE,ScalarFixedDoublePlan(1.0,RangeParams(1524855988,1000,1524855988)),ScalarVectorBinaryOperation(DIV,ScalarFixedDoublePlan(300.0,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(some_metric))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),false),false),List(),None),LOR,ManyToMany,VectorPlan(ScalarFixedDoublePlan(0.0,RangeParams(1524855988,1000,1524855988))),List(),List(),List())",
      "count(some_metric / 300 >= 1 ) or vector(0)" -> "BinaryJoin(Aggregate(Count,ScalarVectorBinaryOperation(GTE,ScalarFixedDoublePlan(1.0,RangeParams(1524855988,1000,1524855988)),ScalarVectorBinaryOperation(DIV,ScalarFixedDoublePlan(300.0,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(some_metric))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),false),false),List(),None),LOR,ManyToMany,VectorPlan(ScalarFixedDoublePlan(0.0,RangeParams(1524855988,1000,1524855988))),List(),List(),List())",
      "sum((some_metric))" -> "Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(some_metric))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),None)",
      "sum((foo + foo))" -> "Aggregate(Sum,BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),ADD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),List(),None)",
      "(sum(foo1) + sum(foo2))/(sum(foo3) + sum(foo4))" -> "BinaryJoin(BinaryJoin(Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo1))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),None),ADD,OneToOne,Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo2))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),None),List(),List(),List()),DIV,OneToOne,BinaryJoin(Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo3))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),None),ADD,OneToOne,Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo4))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),None),List(),List(),List()),List(),List(),List())",
      "foo[5m:1m]" -> "TopLevelSubquery(PeriodicSeries(RawSeries(IntervalSelector(1524855720000,1524855960000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855720000,60000,1524855960000,None),1524855720000,60000,1524855960000,300000,None)",
      "deriv(rate(distance_covered_meters_total[1m])[5m:1m])[6m:3m]" -> "TopLevelSubquery(SubqueryWithWindowing(PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855480000,1524855960000),List(ColumnFilter(__name__,Equals(distance_covered_meters_total))),List(),Some(60000),None),1524855480000,60000,1524855960000,60000,Rate,false,List(),None,List(ColumnFilter(__name__,Equals(distance_covered_meters_total)))),1524855780000,180000,1524855960000,Deriv,List(),300000,60000,None),1524855780000,180000,1524855960000,360000,None)",
      "max_over_time(rate(foo[5m])[5m:1m])" -> "SubqueryWithWindowing(PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855720000,1524855960000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855720000,60000,1524855960000,300000,Rate,false,List(),None,List(ColumnFilter(__name__,Equals(foo)))),1524855988000,0,1524855988000,MaxOverTime,List(),300000,60000,None)",
      "max_over_time(sum(foo)[5m:1m])" -> "SubqueryWithWindowing(Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855720000,1524855960000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855720000,60000,1524855960000,None),List(),None),1524855988000,0,1524855988000,MaxOverTime,List(),300000,60000,None)",
      "sum(foo)[5m:1m]" -> "TopLevelSubquery(Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855720000,1524855960000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855720000,60000,1524855960000,None),List(),None),1524855720000,60000,1524855960000,300000,None)",
      "avg_over_time(max_over_time(rate(foo[5m])[5m:1m])[10m:2m])" -> "SubqueryWithWindowing(SubqueryWithWindowing(PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855180000,1524855960000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855180000,60000,1524855960000,300000,Rate,false,List(),None,List(ColumnFilter(__name__,Equals(foo)))),1524855480000,120000,1524855960000,MaxOverTime,List(),300000,60000,None),1524855988000,0,1524855988000,AvgOverTime,List(),600000,120000,None)",
      "1-1" -> "ScalarBinaryOperation(SUB,Left(1.0),Left(1.0),RangeParams(1524855988,1000,1524855988))",
      "rate(___http_requests_total{job=\"api-server\"}[10m])" -> "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(___http_requests_total))),List(),Some(600000),None),1524855988000,1000000,1524855988000,600000,Rate,false,List(),None,List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(___http_requests_total))))",
      "(http_requests_total{job=\"api-server\"}==1)+4 " -> "ScalarVectorBinaryOperation(ADD,ScalarFixedDoublePlan(4.0,RangeParams(1524855988,1000,1524855988)),ScalarVectorBinaryOperation(EQL,ScalarFixedDoublePlan(1.0,RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),false),false)"
    )

    val qts: Long = 1524855988L
    val step = 1000

    // Parsing with Legacy Parser
    queryToLpString.foreach { case (q, e) =>
      info(s"Parsing with Legacy parser $q")
      val lp = Parser.queryToLogicalPlan(q, qts, step, Shadow)
      if (lp.isInstanceOf[BinaryJoin]) printBinaryJoin(lp)
      lp.toString shouldEqual (e)
    }

    // Parsing with Antlr
    queryToLpString.foreach { case (q, e) =>
      info(s"Parsing with Antlr Parser$q")
      val lp = Parser.queryToLogicalPlan(q, qts, step, Antlr)
      if (lp.isInstanceOf[BinaryJoin]) printBinaryJoin(lp)
      //if(lp.toString != e) {
        //println("\""+q.replaceAll("\"","\\\\\"")+"\" -> \""+lp.toString+"\",")
      //}
      
      lp.toString shouldEqual (e)
    }
  }

  it("should find subqueryWithWindowing") {
    var lp = Parser.queryToLogicalPlan("foo[5m:1m]", 100000, 1000)
    LogicalPlan.hasSubqueryWithWindowing(lp) shouldEqual false
    lp = Parser.queryToLogicalPlan("avg_over_time(foo[5m:1m])", 100000, 1000)
    LogicalPlan.hasSubqueryWithWindowing(lp) shouldEqual true
    lp = Parser.queryToLogicalPlan("avg_over_time(foo[5m])", 100000, 1000)
    LogicalPlan.hasSubqueryWithWindowing(lp) shouldEqual false
    lp = Parser.queryToLogicalPlan("avg_over_time(avg_over_time(foo[5m])[5m:1m]) + avg_over_time(avg_over_time(foo[5m])[5m:1m])", 100000, 1000)
    LogicalPlan.hasSubqueryWithWindowing(lp) shouldEqual true
  }

  it("should error instant queries without step when step multiple notation is used") {
    val q = "sum(rate(foo{job=\"SNRT-App-0\"}[5i]))"
    val qts: Long = 1524855988L
    val step = 0
    info(s"Parsing $q")
    intercept[IllegalArgumentException] {
      Parser.queryToLogicalPlan(q, qts, step)
    }
  }

  it("should not error instant queries without step when step multiple notation is NOT used") {
    val q = "sum(rate(foo{job=\"SNRT-App-0\"}[5m]))"
    val qts: Long = 1524855988L
    val step = 0
    info(s"Parsing $q")
    Parser.queryToLogicalPlan(q, qts, step)
  }

  it("should correctly build a label map") {
    val queryMapPairs = Seq(
      ("foo", Map("__name__" -> "foo")),
      ("""{__name__="foo"}""" -> Map("__name__" -> "foo")),
      ("""{__name__="foo", bar="baz"}""" -> Map("__name__" -> "foo", "bar" -> "baz")),
      ("""{bar="baz"}""" -> Map("bar" -> "baz")),
      ("""foo{bar="baz", bog="bah"}""" -> Map("__name__" -> "foo", "bar" -> "baz", "bog" -> "bah")),
    )

    val queriesShouldFail = Seq(
      """foo{__name__="bar"}""",
      """foo{__name__="bar", bog="baz"}""",
      """{}"""
    )

    queryMapPairs.foreach{case (query, expectedMap) =>
      Parser.queryToEqualLabelMap(query) shouldEqual expectedMap
    }

    queriesShouldFail.foreach{query =>
      assertThrows[IllegalArgumentException](Parser.queryToEqualLabelMap(query))
    }
  }

  it("should correctly parse durations with multiple units") {
    case class Spec(query: String, windowDuration: Duration, offsetDuration: Duration)
    val specs = Seq(
      Spec(
        """foo{label="bar"}[1m30s] offset 2h15m""",
            Duration(1, MINUTES) + Duration(30, SECONDS),
            Duration(2, HOURS) + Duration(15, MINUTES)
      ),
      Spec(
        """foo{label="bar"}[3d2h25m10s] offset 2d12h15m30s""",
            Duration(3, DAYS) + Duration(2, HOURS) + Duration(25, MINUTES) + Duration(10, SECONDS),
            Duration(2, DAYS) + Duration(12, HOURS) + Duration(15, MINUTES) + Duration(30, SECONDS),
      ),
      Spec(
        """foo{label="bar"}[3d0h25m0s] offset 0d12h15m30s""",
            Duration(3, DAYS) + Duration(25, MINUTES),
            Duration(12, HOURS) + Duration(15, MINUTES) + Duration(30, SECONDS),
      )
    )
    specs.foreach{ spec =>
      Parser.parseQuery(spec.query) match {
        case RangeExpression(_, _, window, offset) =>
          window.millis(0) shouldEqual spec.windowDuration.toMillis
          offset.get.millis(0) shouldEqual spec.offsetDuration.toMillis
      }
    }
  }

  it("should correctly parse subquery durations with multiple units") {
    case class Spec(query: String, windowDuration: Duration, stepDuration: Duration, offsetDuration: Duration)
    val specs = Seq(
      Spec(
        """foo{label="bar"}[3d2h25m10s:1d4h30m4s] offset 2d12h15m30s""",
            Duration(3, DAYS) + Duration(2, HOURS) + Duration(25, MINUTES) + Duration(10, SECONDS),
            Duration(1, DAYS) + Duration(4, HOURS) + Duration(30, MINUTES) + Duration(4, SECONDS),
            Duration(2, DAYS) + Duration(12, HOURS) + Duration(15, MINUTES) + Duration(30, SECONDS)
      ),
      Spec(
        """foo{label="bar"}[3d0h25m0s:1d0h2m] offset 0d12h15m30s""",
            Duration(3, DAYS) + Duration(25, MINUTES),
            Duration(1, DAYS) + Duration(2, MINUTES),
            Duration(12, HOURS) + Duration(15, MINUTES) + Duration(30, SECONDS)
      )
    )
    specs.foreach{ spec =>
      Parser.parseQuery(spec.query) match {
        case SubqueryExpression(_, SubqueryClause(window, step), offset, _) =>
          window.millis(0) shouldEqual spec.windowDuration.toMillis
          step.map(_.millis(0)).getOrElse(0) shouldEqual spec.stepDuration.toMillis
          offset.get.millis(0) shouldEqual spec.offsetDuration.toMillis
      }
    }
  }

  private def printBinaryJoin( lp: LogicalPlan, level: Int = 0) : scala.Unit =  {
    if (!lp.isInstanceOf[BinaryJoin]) {
      info(s"${"  "*level}" + lp.toString)
    }
    else {
      val binaryJoin = lp.asInstanceOf[BinaryJoin]
      info(s"${"  "*level}" + "lhs:" )
      printBinaryJoin(binaryJoin.lhs, level + 1)
      info(s"${"  "*level}" + "Cardinality: " + binaryJoin.cardinality)
      info(s"${"  "*level}" + "Operator: " + binaryJoin.operator)
      info(s"${"  "*level}" + "On labels: " + binaryJoin.on )
      info(s"${"  "*level}" + "Include labels: " + binaryJoin.include)
      info(s"${"  "*level}" + "Ignoring labels: " + binaryJoin.ignoring)
      info(s"${"  "*level}" + "rhs: ")
      printBinaryJoin(binaryJoin.rhs, level + 1)
    }
  }

  private def parseSuccessfully(query: String) = {
    antlrParseSuccessfully(query)
  }

  private def parseSubquery(query: String) = {
    try {
      val result = AntlrParser.parseQuery(query)
    } catch {
      case e: Exception => {
        // FIXME: don't catch any exception when subquery support is finished
        if (!e.getMessage().startsWith("Expected")) {
          throw e
        }
      }
    }
  }

  private def parseWithAntlr(query: String, expectedLp: String) = {
    info(s"Parsing $query")
    val qts: Long = 1524855988L
    val step = 1000
    val defaultQueryParams = TimeStepParams(qts, step, qts)
    val result = AntlrParser.parseQuery(query)
    val lp: LogicalPlan = result match {
      case p: PeriodicSeries => p.toSeriesPlan(defaultQueryParams)
      case r: SimpleSeries   => r.toSeriesPlan(defaultQueryParams, isRoot = true)
      case _ => throw new UnsupportedOperationException()
    }
    val planString = lp.toString
    planString shouldEqual (expectedLp)
  }

  private def parseSubqueryError(query: String) = {
    intercept[IllegalArgumentException] {
      AntlrParser.parseQuery(query)
    }
  }

  private def antlrParseSuccessfully(query: String) = {
    val result = AntlrParser.parseQuery(query)
    info(String.valueOf(result))
  }

  private def parseError(query: String) = {
    antlrParseError(query)
  }

  private def antlrParseError(query: String) = {
    intercept[IllegalArgumentException] {
      AntlrParser.parseQuery(query)
      try {
        Parser.queryToLogicalPlan(query, 1524855988L, 0)
      } catch {
        case e: UnsupportedOperationException => {
          // This case is reached only for certain unsupported unary operations.
          throw new IllegalArgumentException()
        }
      }
    }
  }

  private def parseLabelValueSuccessfully(query: String) = {
    Parser.parseLabelValueFilter(query)
  }

  private def parseLabelValueError(query: String) = {
    intercept[IllegalArgumentException] {
      Parser.parseLabelValueFilter(query)
    }
  }
}
