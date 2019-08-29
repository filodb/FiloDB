package filodb.prometheus.parse

import org.scalatest.{FunSpec, Matchers}
import filodb.prometheus.ast.TimeStepParams
import filodb.query.{BinaryJoin, LogicalPlan}

//noinspection ScalaStyle
// scalastyle:off
class ParserSpec extends FunSpec with Matchers {

  it("metadata matcher query") {
    parseSuccessfully("http_requests_total{job=\"prometheus\", method=\"GET\"}")
    parseSuccessfully("http_requests_total{job=\"prometheus\", method=\"GET\"}")
    parseSuccessfully("http_requests_total{job=\"prometheus\", method!=\"GET\"}")
    parseError("job{__name__=\"prometheus\"}")
    parseError("job[__name__=\"prometheus\"]")
    val queryToLpString = ("http_requests_total{job=\"prometheus\", method=\"GET\"}" ->
      "SeriesKeysByFilters(List(ColumnFilter(job,Equals(prometheus)), ColumnFilter(method,Equals(GET)), ColumnFilter(__name__,Equals(http_requests_total))),1524855988000,1524855988000)")
    val start: Long = 1524855988L
    val end: Long = 1524855988L
    val lp = Parser.metadataQueryToLogicalPlan(queryToLpString._1, TimeStepParams(start, -1, end))
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
    parseError("1 == 1")
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

    parseSuccessfully("foo")
    parseSuccessfully("foo offset 5m")
    parseSuccessfully("foo:bar{a=\"bc\"}")
    parseSuccessfully("foo{NaN='bc'}")
    parseSuccessfully("foo{a=\"b\", foo!=\"bar\", test=~\"test\", bar!~\"baz\"}")


    parseError("{")
    parseError("}")
    parseError("some{")
    parseError("some}")
    parseError("some_metric{a=b}")
    parseError("some_metric{a:b=\"b\"}")
    parseError("foo{a*\"b\"}")
    parseError("foo{a>=\"b\"}")

    parseError("foo{gibberish}")
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
    parseSuccessfully("sum_over_time(foo)")


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
  }

  it("Should be able to make logical plans for Series Expressions") {
    val queryToLpString = Map(
      "10 + http_requests_total * 5" ->
        "ScalarVectorBinaryOperation(ADD,10.0,ScalarVectorBinaryOperation(MUL,5.0,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000),false),true)",
      "10 + (http_requests_total * 5)" ->
        "ScalarVectorBinaryOperation(ADD,10.0,ScalarVectorBinaryOperation(MUL,5.0,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000),false),true)",
      "(10 + http_requests_total) * 5" ->
        "ScalarVectorBinaryOperation(MUL,5.0,ScalarVectorBinaryOperation(ADD,10.0,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000),true),false)",
      "topk(5, http_requests_total)" ->
        "Aggregate(TopK,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000),List(5.0),List(),List())",
      "irate(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000,300000,Irate,List())",
      "idelta(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000,300000,Idelta,List())",
      "resets(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000,300000,Resets,List())",
      "deriv(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000,300000,Deriv,List())",
      "rate(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000,300000,Rate,List())",
      "http_requests_total{job=\"prometheus\"}[5m]" ->
        "RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(job,Equals(prometheus)), ColumnFilter(__name__,Equals(http_requests_total))),List())",
      "http_requests_total offset 5m" ->
        "PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000)",
      "http_requests_total{environment=~\"staging|testing|development\",method!=\"GET\"}" ->
        "PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(environment,EqualsRegex(staging|testing|development)), ColumnFilter(method,NotEquals(GET)), ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000)",

      "method_code:http_errors:rate5m / ignoring(code) group_left method:http_requests:rate5m" ->
        "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(method_code:http_errors:rate5m))),List()),1524855988000,1000000,1524855988000),DIV,ManyToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(method:http_requests:rate5m))),List()),1524855988000,1000000,1524855988000),List(),List(code),List())",
      "method_code:http_errors:rate5m / ignoring(code) group_left(mode, instance) method:http_requests:rate5m" ->
        "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(method_code:http_errors:rate5m))),List()),1524855988000,1000000,1524855988000),DIV,ManyToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(method:http_requests:rate5m))),List()),1524855988000,1000000,1524855988000),List(),List(code),List(mode, instance))",
      "method_code:http_errors:rate5m / ignoring(code) group_right method:http_requests:rate5m" ->
        "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(method_code:http_errors:rate5m))),List()),1524855988000,1000000,1524855988000),DIV,OneToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(method:http_requests:rate5m))),List()),1524855988000,1000000,1524855988000),List(),List(code),List())",

      "increase(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(job,Equals(api-server)), ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000,300000,Increase,List())",
      "sum(http_requests_total{method=\"GET\"} offset 5m)" ->
        "Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(method,Equals(GET)), ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000),List(),List(),List())",
      "absent(nonexistent{job=\"myjob\",instance=~\".*\"})" ->
        "ApplyInstantFunction(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(job,Equals(myjob)), ColumnFilter(instance,EqualsRegex(.*)), ColumnFilter(__name__,Equals(nonexistent))),List()),1524855988000,1000000,1524855988000),Absent,List())",
      "absent(sum(nonexistent{job=\"myjob\"}))" ->
        "ApplyInstantFunction(Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(job,Equals(myjob)), ColumnFilter(__name__,Equals(nonexistent))),List()),1524855988000,1000000,1524855988000),List(),List(),List()),Absent,List())",
      """{__name__="foo\\\"\n\t",job="myjob"}[5m]""" ->
        "RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(foo\\\"\n\t)), ColumnFilter(job,Equals(myjob))),List())",
      "{__name__=\"foo\",job=\"myjob\"}" ->
        "PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(foo)), ColumnFilter(job,Equals(myjob))),List()),1524855988000,1000000,1524855988000)",
      "{__name__=\"foo\",job=\"myjob\"}[5m]" ->
        "RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(foo)), ColumnFilter(job,Equals(myjob))),List())",
      "sum({__name__=\"foo\",job=\"myjob\"})" ->
        "Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(foo)), ColumnFilter(job,Equals(myjob))),List()),1524855988000,1000000,1524855988000),List(),List(),List())",
      "sum(http_requests_total)       \n \n / \n\n    sum(http_requests_total)" ->
        "BinaryJoin(Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000),List(),List(),List()),DIV,OneToOne,Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000),List(),List(),List()),List(),List(),List())",

     // Binary Expressions should generate Logical Plan according to precedence
     // Logical plan generated when expression does not have brackets according to precedence is same as logical plan for expression with brackets which are according to precedence
      "(10 % http_requests_total) + 5" ->
        "ScalarVectorBinaryOperation(ADD,5.0,ScalarVectorBinaryOperation(MOD,10.0,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000),true),false)",
      "10 % http_requests_total + 5" ->
        "ScalarVectorBinaryOperation(ADD,5.0,ScalarVectorBinaryOperation(MOD,10.0,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000),true),false)",

      "(http_requests_total % http_requests_total) + http_requests_total" ->
        "BinaryJoin(BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000),MOD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000),List(),List(),List()),ADD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000),List(),List(),List())",
      "http_requests_total % http_requests_total + http_requests_total" ->
        "BinaryJoin(BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000),MOD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000),List(),List(),List()),ADD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988000,1000000,1524855988000),List(),List(),List())",

      // "unless" and "and" have same priority but are not right associative so "and" should be evaluated first
      "((foo and bar) unless baz) or qux" ->
        "BinaryJoin(BinaryJoin(BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List()),1524855988000,1000000,1524855988000),LAND,ManyToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(bar))),List()),1524855988000,1000000,1524855988000),List(),List(),List()),LUnless,ManyToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(baz))),List()),1524855988000,1000000,1524855988000),List(),List(),List()),LOR,ManyToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(qux))),List()),1524855988000,1000000,1524855988000),List(),List(),List())",
      "foo and bar unless baz or qux" ->
      "BinaryJoin(BinaryJoin(BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List()),1524855988000,1000000,1524855988000),LAND,ManyToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(bar))),List()),1524855988000,1000000,1524855988000),List(),List(),List()),LUnless,ManyToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(baz))),List()),1524855988000,1000000,1524855988000),List(),List(),List()),LOR,ManyToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(qux))),List()),1524855988000,1000000,1524855988000),List(),List(),List())",

      // Pow is right associative so (bar ^ baz) should be evaluated first
      "(foo ^ (bar ^ baz))" ->
        "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List()),1524855988000,1000000,1524855988000),POW,OneToOne,BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(bar))),List()),1524855988000,1000000,1524855988000),POW,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(baz))),List()),1524855988000,1000000,1524855988000),List(),List(),List()),List(),List(),List())",
      "foo ^ bar ^ baz" ->
        "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List()),1524855988000,1000000,1524855988000),POW,OneToOne,BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(bar))),List()),1524855988000,1000000,1524855988000),POW,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(baz))),List()),1524855988000,1000000,1524855988000),List(),List(),List()),List(),List(),List())",

      "(foo + bar) or (bla and blub)" ->
        "BinaryJoin(BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List()),1524855988000,1000000,1524855988000),ADD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(bar))),List()),1524855988000,1000000,1524855988000),List(),List(),List()),LOR,ManyToMany,BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(bla))),List()),1524855988000,1000000,1524855988000),LAND,ManyToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(blub))),List()),1524855988000,1000000,1524855988000),List(),List(),List()),List(),List(),List())",
      "foo + bar or bla and blub" ->
        "BinaryJoin(BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(foo))),List()),1524855988000,1000000,1524855988000),ADD,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(bar))),List()),1524855988000,1000000,1524855988000),List(),List(),List()),LOR,ManyToMany,BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(bla))),List()),1524855988000,1000000,1524855988000),LAND,ManyToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(blub))),List()),1524855988000,1000000,1524855988000),List(),List(),List()),List(),List(),List())",

      "bar + on(foo) (bla / on(baz, buz) group_right(test) blub)" -> "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(bar))),List()),1524855988000,1000000,1524855988000),ADD,OneToOne,BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(bla))),List()),1524855988000,1000000,1524855988000),DIV,OneToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(blub))),List()),1524855988000,1000000,1524855988000),List(baz, buz),List(),List(test)),List(foo),List(),List())",
      "bar + on(foo) bla / on(baz, buz) group_right(test) blub" -> "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(bar))),List()),1524855988000,1000000,1524855988000),ADD,OneToOne,BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(bla))),List()),1524855988000,1000000,1524855988000),DIV,OneToMany,PeriodicSeries(RawSeries(IntervalSelector(1524855688000,1524855988000),List(ColumnFilter(__name__,Equals(blub))),List()),1524855988000,1000000,1524855988000),List(baz, buz),List(),List(test)),List(foo),List(),List())"
    )

    val qts: Long = 1524855988L
    queryToLpString.foreach { case (q, e) =>
      info(s"Parsing $q")
      val lp = Parser.queryToLogicalPlan(q, qts)
      if (lp.isInstanceOf[BinaryJoin])
       printBinaryJoin(lp)
      lp.toString shouldEqual (e)
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
    Parser.parseQuery(query)
  }

  private def parseError(query: String) = {
    intercept[IllegalArgumentException] {
      Parser.parseQuery(query)
    }
  }
}
