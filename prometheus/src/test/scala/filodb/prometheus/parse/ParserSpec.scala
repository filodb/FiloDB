package filodb.prometheus.parse

import filodb.prometheus.ast.TimeStepParams
import filodb.query.{BinaryJoin, LogicalPlan}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

//noinspection ScalaStyle
// scalastyle:off
class ParserSpec extends AnyFunSpec with Matchers {

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
    parseError("1 and 1")
    parseError("1 == 1")  // reason: comparisons between scalars must use BOOL modifier
    parseError("1 or 1")
    parseError("1 unless 1")
    parseError("1 !~ 1")
    parseError("1 =~ 1")
    parseError("-\"string\"")
    parseError("-test[5m]")
    parseError("*test")
    parseError("1 offset 1d")
    parseError("a - on(b) ignoring(c) d")

    parseSuccessfully("foo * bar")
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

    parseError("foo and 1")
    parseError("1 and foo")
    parseError("foo or 1")
    parseError("1 or foo")
    parseError("foo unless 1")
    parseError("1 unless foo")
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
    parseError("{x=\"\"}")
    parseError("{x=~\".*\"}")
    parseError("{x!~\".+\"}")
    parseError("{x!=\"a\"}")
    parseError("foo{__name__=\"bar\"}")

    parseSuccessfully("test{a=\"b\"}[5y] OFFSET 3d")
    parseSuccessfully("test[5s]")
    parseSuccessfully("test[5m]")
    parseSuccessfully("test[5h] OFFSET 5m")
    parseSuccessfully("test[5d] OFFSET 10s")
    parseSuccessfully("test[5w] offset 2w")

    parseError("foo[5mm]")
    parseError("foo[0m]")
    parseError("foo[5m30s]")
    parseError("foo[5m] OFFSET 1h30m")
    parseError("foo[\"5m\"]")
    parseError("foo[]")
    parseError("foo[1]")
    parseError("some_metric[5m] OFFSET 1")
    parseError("some_metric[5m] OFFSET 1mm")
    parseError("some_metric[5m] OFFSET")
    parseError("some_metric OFFSET 1m[5m]")
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
    parseSuccessfully("sum_over_time(some_metric[5m])")
    parseError("sum_over_time(some_metric)") // reason : Expected range-vector
    parseError("sum_over_time(some_metric[5m], hello)") // reason : Expected only 1 arg, got 2
    parseError("sum_over_time(hello, some_metric[5m])") // reason : Expected range, got instant

    //  Timestamp
    parseSuccessfully("timestamp(some_metric)")
    parseError("timestamp(some_metric[5m])") // reason : Expected instant vector, got range vector
    parseError("timestamp(some_metric, hello)") // reason : Expected only 1 arg, got 2
  }

  it("parse long identifiers") {
    // This should not cause a stack overflow error.

    val bob = new StringBuilder().append("requests{job=\"")
    for (i <- 1 to 100) {
      bob.append("abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_")
    }

    parseSuccessfully(bob.append("\"}").toString())
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
        "Aggregate(TopK,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(5.0),List(),List())",
      "topk(5, http_requests_total::foo)" ->
        "Aggregate(TopK,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(foo),Some(300000),None),1524855988000,1000000,1524855988000,None),List(5.0),List(),List())",
      "topk(5, http_requests_total::foo::bar)" ->
        "Aggregate(TopK,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total::foo))),List(bar),Some(300000),None),1524855988000,1000000,1524855988000,None),List(5.0),List(),List())",
      "stdvar(http_requests_total)" ->
        "Aggregate(Stdvar,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List())",
      "stddev(http_requests_total)" ->
        "Aggregate(Stddev,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List())",
      "irate(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,Irate,false,List(),None)",
      "idelta(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,Idelta,false,List(),None)",
      "resets(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,Resets,false,List(),None)",
      "deriv(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,Deriv,false,List(),None)",
      "rate(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,Rate,false,List(),None)",
      "last(jvm_memory{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(jvm_memory))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,Last,false,List(),None)",
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
        "PeriodicSeriesWithWindowing(ApplyInstantFunctionRaw(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_req_latency))),List(),Some(300000),None),HistogramBucket,List(ScalarFixedDoublePlan(2.5,RangeParams(0,9223372036854775807,60000)))),1524855988000,1000000,1524855988000,300000,Rate,false,List(),None)",

      "method_code:http_errors:rate5m / ignoring(code) group_left method:http_requests:rate5m" ->
        "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(method_code:http_errors:rate5m))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),DIV,ManyToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(method:http_requests:rate5m))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(code),List())",
      "method_code:http_errors:rate5m / ignoring(code) group_left(mode, instance) method:http_requests:rate5m" ->
        "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(method_code:http_errors:rate5m))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),DIV,ManyToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(method:http_requests:rate5m))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(code),List(mode, instance))",
      "method_code:http_errors:rate5m / ignoring(code) group_right method:http_requests:rate5m" ->
        "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(method_code:http_errors:rate5m))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),DIV,OneToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(method:http_requests:rate5m))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(code),List())",

      "increase(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,Increase,false,List(),None)",
      "sum(http_requests_total{method=\"GET\"} offset 5m)" ->
        "Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(method,Equals(GET)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),Some(300000)),1524855988000,1000000,1524855988000,Some(300000)),List(),List(),List())",
      """{__name__="foo\\\"\n\t",job="myjob"}[5m]""" ->
        "RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo\\\"\n\t)), ColumnFilter(job,Equals(myjob))),List(),Some(300000),None)",
      "{__name__=\"foo\",job=\"myjob\"}" ->
        "PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo)), ColumnFilter(job,Equals(myjob))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None)",
      "{__name__=\"foo\",job=\"myjob\"}[5m]" ->
        "RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo)), ColumnFilter(job,Equals(myjob))),List(),Some(300000),None)",
      "sum({__name__=\"foo\",job=\"myjob\"})" ->
        "Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo)), ColumnFilter(job,Equals(myjob))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List())",
      "sum(http_requests_total)       \n \n / \n\n    sum(http_requests_total)" ->
        "BinaryJoin(Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),DIV,OneToOne,Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),List(),List(),List())",

      "changes(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,Changes,false,List(),None)",
      "quantile_over_time(0.4,http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,QuantileOverTime,false,List(ScalarFixedDoublePlan(0.4,RangeParams(1524855988,1000,1524855988))),None)",
       "label_replace(http_requests_total,instance,new-label,instance,\"(.*)-(.*)\")" -> "ApplyMiscellaneousFunction(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),LabelReplace,List(instance, new-label, instance, (.*)-(.*)))",
      "hist_to_prom_vectors(http_request_latency)" ->
        "ApplyMiscellaneousFunction(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_request_latency))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),HistToPromVectors,List())",
      "holt_winters(http_requests_total{job=\"api-server\"}[5m], 0.01, 0.1)" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,HoltWinters,false,List(ScalarFixedDoublePlan(0.01,RangeParams(1524855988,1000,1524855988)), ScalarFixedDoublePlan(0.1,RangeParams(1524855988,1000,1524855988))),None)",
      "z_score(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,ZScore,false,List(),None)",
      "predict_linear(http_requests_total{job=\"api-server\"}[5m], 10)" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,300000,PredictLinear,false,List(ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988))),None)",

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
        "Aggregate(CountValues,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(\"freq\"),List(),List())",
      "timestamp(http_requests_total)" -> "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,0,Timestamp,false,List(),None)",
      "sum:some_metric:dataset:1m{_ws_=\"demo\", _ns_=\"test\"}" -> "PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(_ws_,Equals(demo)), ColumnFilter(_ns_,Equals(test)), ColumnFilter(__name__,Equals(sum:some_metric:dataset:1m))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None)",
      "1 + 2 * 3" -> "ScalarBinaryOperation(ADD,Left(1.0),Right(ScalarBinaryOperation(MUL,Left(2.0),Left(3.0),RangeParams(1524855988,1000,1524855988))),RangeParams(1524855988,1000,1524855988))",

      // step multiple tests
      "http_requests_total offset 2i" ->
        "PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),Some(2000000)),1524855988000,1000000,1524855988000,Some(2000000))",
      "sum(rate(foo{job=\"SNRT-App-0\"}[5i]))" -> "Aggregate(Sum,PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(SNRT-App-0)), ColumnFilter(__name__,Equals(foo))),List(),Some(5000000),None),1524855988000,1000000,1524855988000,5000000,Rate,true,List(),None),List(),List(),List())",
      "rate(foo{job=\"SNRT-App-0\"}[5i]) + rate(bar{job=\"SNRT-App-0\"}[4i])" -> "BinaryJoin(PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(SNRT-App-0)), ColumnFilter(__name__,Equals(foo))),List(),Some(5000000),None),1524855988000,1000000,1524855988000,5000000,Rate,true,List(),None),ADD,OneToOne,PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(SNRT-App-0)), ColumnFilter(__name__,Equals(bar))),List(),Some(4000000),None),1524855988000,1000000,1524855988000,4000000,Rate,true,List(),None),List(),List(),List())",
      "sum(rate(foo{job=\"SNRT-App-0\"}[0.5i]))" -> "Aggregate(Sum,PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(job,Equals(SNRT-App-0)), ColumnFilter(__name__,Equals(foo))),List(),Some(500000),None),1524855988000,1000000,1524855988000,500000,Rate,true,List(),None),List(),List(),List())",
     "http_requests_total - 10/2" -> "ScalarVectorBinaryOperation(SUB,ScalarBinaryOperation(DIV,Left(10.0),Left(2.0),RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),false)",
      "foo - http_requests_total * 2^3" -> "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),SUB,OneToOne,ScalarVectorBinaryOperation(MUL,ScalarBinaryOperation(POW,Left(2.0),Left(3.0),RangeParams(1524855988,1000,1524855988)),PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),false),List(),List(),List())",
      "sum(http_requests_total) - 10/2" -> "ScalarVectorBinaryOperation(SUB,ScalarBinaryOperation(DIV,Left(10.0),Left(2.0),RangeParams(1524855988,1000,1524855988)),Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),false)",
      "ceil(vector(100) / 10 / 10)" -> "ApplyInstantFunction(ScalarVectorBinaryOperation(DIV,ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988)),ScalarVectorBinaryOperation(DIV,ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988)),VectorPlan(ScalarFixedDoublePlan(100.0,RangeParams(1524855988,1000,1524855988))),false),false),Ceil,List())",
      "ceil(sum(foo) / 10 / 10 / 10)" -> "ApplyInstantFunction(ScalarVectorBinaryOperation(DIV,ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988)),ScalarVectorBinaryOperation(DIV,ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988)),ScalarVectorBinaryOperation(DIV,ScalarFixedDoublePlan(10.0,RangeParams(1524855988,1000,1524855988)),Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855988000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List(),Some(300000),None),1524855988000,1000000,1524855988000,None),List(),List(),List()),false),false),false),Ceil,List())"
    )

    val qts: Long = 1524855988L
    val step = 1000
    queryToLpString.foreach { case (q, e) =>
      info(s"Parsing $q")
      val lp = Parser.queryToLogicalPlan(q, qts, step)
      if (lp.isInstanceOf[BinaryJoin])
        printBinaryJoin(lp)
      lp.toString shouldEqual (e)
    }
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
    Parser.parseQuery(query)
  }

  private def parseError(query: String) = {
    intercept[IllegalArgumentException] {
      Parser.parseQuery(query)
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
