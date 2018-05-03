package filodb.prometheus.parse

import org.scalatest.{FunSpec, Matchers}


//noinspection ScalaStyle
// scalastyle:off
class ParserSpec extends FunSpec with Matchers {

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
  }

  it("Should be able to make logical plans for Series Expressions") {
    val queryToLpString = Map(
      "http_requests_total" ->
        "PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(http_requests_total,Equals(__name__))),List()),1524855988170,1,1524855988170)",
      "http_requests_total{job=\"prometheus\",group=\"canary\"}" ->
        "PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(job,Equals(\"prometheus\")), ColumnFilter(group,Equals(\"canary\")), ColumnFilter(http_requests_total,Equals(__name__))),List()),1524855988170,1,1524855988170)",
      "http_requests_total{environment=~\"staging|testing|development\",method!=\"GET\"}" ->
        "PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(environment,Equals(\"staging|testing|development\")), ColumnFilter(method,Equals(\"GET\")), ColumnFilter(http_requests_total,Equals(__name__))),List()),1524855988170,1,1524855988170)",
      "http_requests_total{job=\"prometheus\"}[5m]" ->
        "RawSeries(IntervalSelector(List(1524855688170),List(1524855988170)),List(ColumnFilter(job,Equals(\"prometheus\")), ColumnFilter(http_requests_total,Equals(__name__))),List())",
      "http_requests_total offset 5m" ->
        "PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(http_requests_total,Equals(__name__))),List()),1524855988170,1,1524855988170)",
      "sum(http_requests_total{method=\"GET\"} offset 5m)" ->
        "Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(method,Equals(\"GET\")), ColumnFilter(http_requests_total,Equals(__name__))),List()),1524855988170,1,1524855988170),List(),List(),List())",
      "rate(http_requests_total[5m] offset 1w)" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(List(1524855688170),List(1524855988170)),List(ColumnFilter(http_requests_total,Equals(__name__))),List()),1524855988170,1,1524855988170,300000,Rate,List())",
      "method_code:http_errors:rate5m / ignoring(code) group_left method:http_requests:rate5m" ->
        "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(method_code:http_errors:rate5m,Equals(__name__))),List()),1524855988170,1,1524855988170),DIV,OneToMany,PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(method:http_requests:rate5m,Equals(__name__))),List()),1524855988170,1,1524855988170),List(),List())",
      "method_code:http_errors:rate5m{code=\"500\"} / ignoring(code) method:http_requests:rate5m" ->
        "BinaryJoin(PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(code,Equals(\"500\")), ColumnFilter(method_code:http_errors:rate5m,Equals(__name__))),List()),1524855988170,1,1524855988170),DIV,OneToOne,PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(method:http_requests:rate5m,Equals(__name__))),List()),1524855988170,1,1524855988170),List(),List())",
      "sum(http_requests_total) without (instance)" ->
        "Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(http_requests_total,Equals(__name__))),List()),1524855988170,1,1524855988170),List(),List(),List(instance))",
      "sum(http_requests_total) by (application, group)" ->
        "Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(http_requests_total,Equals(__name__))),List()),1524855988170,1,1524855988170),List(),List(application, group),List())",
      "sum(http_requests_total)" ->
        "Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(http_requests_total,Equals(__name__))),List()),1524855988170,1,1524855988170),List(),List(),List())",
      "count_values(\"version\", build_version)" ->
        "Aggregate(CountValues,PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(build_version,Equals(__name__))),List()),1524855988170,1,1524855988170),List(\"version\"),List(),List())",
      "topk(5, http_requests_total)" ->
        "Aggregate(TopK,PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(http_requests_total,Equals(__name__))),List()),1524855988170,1,1524855988170),List(5.0),List(),List())",
      "absent(nonexistent{job=\"myjob\"})" ->
        "ApplyInstantFunction(PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(job,Equals(\"myjob\")), ColumnFilter(nonexistent,Equals(__name__))),List()),1524855988170,1,1524855988170),Absent,List())",
      "absent(nonexistent{job=\"myjob\",instance=~\".*\"})" ->
        "ApplyInstantFunction(PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(job,Equals(\"myjob\")), ColumnFilter(instance,Equals(\".*\")), ColumnFilter(nonexistent,Equals(__name__))),List()),1524855988170,1,1524855988170),Absent,List())",
      "absent(sum(nonexistent{job=\"myjob\"}))" ->
        "ApplyInstantFunction(Aggregate(Sum,PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(job,Equals(\"myjob\")), ColumnFilter(nonexistent,Equals(__name__))),List()),1524855988170,1,1524855988170),List(),List(),List()),Absent,List())",
      "delta(cpu_temp_celsius{host=\"zeus\"}[2h])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(List(1524848788170),List(1524855988170)),List(ColumnFilter(host,Equals(\"zeus\")), ColumnFilter(cpu_temp_celsius,Equals(__name__))),List()),1524855988170,1,1524855988170,7200000,Delta,List())",
      "histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[10m]))" ->
        "ApplyInstantFunction(PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(List(1524855388170),List(1524855988170)),List(ColumnFilter(http_request_duration_seconds_bucket,Equals(__name__))),List()),1524855988170,1,1524855988170,600000,Rate,List()),HistogramQuantile,List(0.9))",
      "histogram_quantile(0.9, sum(rate(http_request_duration_seconds_bucket[10m])) by (job, le))" ->
        "ApplyInstantFunction(Aggregate(Sum,PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(List(1524855388170),List(1524855988170)),List(ColumnFilter(http_request_duration_seconds_bucket,Equals(__name__))),List()),1524855988170,1,1524855988170,600000,Rate,List()),List(),List(job, le),List()),HistogramQuantile,List(0.9))",
      "histogram_quantile(0.9, sum(rate(http_request_duration_seconds_bucket[10m])) by (le))" ->
        "ApplyInstantFunction(Aggregate(Sum,PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(List(1524855388170),List(1524855988170)),List(ColumnFilter(http_request_duration_seconds_bucket,Equals(__name__))),List()),1524855988170,1,1524855988170,600000,Rate,List()),List(),List(le),List()),HistogramQuantile,List(0.9))",
      "increase(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(List(1524855688170),List(1524855988170)),List(ColumnFilter(job,Equals(\"api-server\")), ColumnFilter(http_requests_total,Equals(__name__))),List()),1524855988170,1,1524855988170,300000,Increase,List())",
      "irate(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(List(1524855688170),List(1524855988170)),List(ColumnFilter(job,Equals(\"api-server\")), ColumnFilter(http_requests_total,Equals(__name__))),List()),1524855988170,1,1524855988170,300000,Irate,List())",
      "label_join(up{job=\"api-server\",src1=\"a\",src2=\"b\",src3=\"c\"}, \"foo\", \",\", \"src1\", \"src2\", \"src3\")" ->
        "ApplyInstantFunction(PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(job,Equals(\"api-server\")), ColumnFilter(src1,Equals(\"a\")), ColumnFilter(src2,Equals(\"b\")), ColumnFilter(src3,Equals(\"c\")), ColumnFilter(up,Equals(__name__))),List()),1524855988170,1,1524855988170),LabelJoin,List())",
      "label_replace(up{job=\"api-server\",service=\"a:c\"}, \"foo\", \"$1\", \"service\", \"(.*):.*\")" ->
        "ApplyInstantFunction(PeriodicSeries(RawSeries(IntervalSelector(List(1524855988170),List(1524855988170)),List(ColumnFilter(job,Equals(\"api-server\")), ColumnFilter(service,Equals(\"a:c\")), ColumnFilter(up,Equals(__name__))),List()),1524855988170,1,1524855988170),LabelReplace,List())",
      "rate(http_requests_total{job=\"api-server\"}[5m])" ->
        "PeriodicSeriesWithWindowing(RawSeries(IntervalSelector(List(1524855688170),List(1524855988170)),List(ColumnFilter(job,Equals(\"api-server\")), ColumnFilter(http_requests_total,Equals(__name__))),List()),1524855988170,1,1524855988170,300000,Rate,List())"
    )

    val qts: Long = 1524855988170L
    queryToLpString.foreach { case (q, e) =>
      val lp = Parser.queryToLogicalPlan(q, qts)
      lp.toString should be(e)
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
