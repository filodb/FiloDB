package filodb.coordinator.parse

import org.scalatest.{FunSpec, Matchers}

//noinspection ScalaStyle
// scalastyle:off
class PrometheusQLParserSpec extends FunSpec with Matchers {

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
    //parseError("topk(some_metric)")
    //parseError("topk(some_metric, other_metric)")
    //parseError("count_values(5, other_metric)")

    parseSuccessfully("time()")
    parseSuccessfully("floor(some_metric{foo!=\"bar\"})")
    parseSuccessfully("rate(some_metric[5m])")
    parseSuccessfully("round(some_metric)")
    parseSuccessfully("round(some_metric, 5)")

    //    parseError(  "floor()")
    //    parseError(  "floor(some_metric, other_metric)")
    //    parseError(  "floor(1)")
    //    parseError(  "non_existent_function_far_bar()")
    //    parseError(  "rate(some_metric)")
    parseError("label_replace(a, `b`, `cff`, `d`, `.*`)")
    parseError("-=")
    parseError("++-++-+-+-<")
    parseError("e-+=/(0)")
    //    parseError(  "-If")


  }

  it("Should be able to make logical plans for Series Expressions") {
    val queries = Seq(
      "http_requests_total",
      "http_requests_total{job=\"prometheus\",group=\"canary\"}",
      "http_requests_total{environment=~\"staging|testing|development\",method!=\"GET\"}",
      "http_requests_total{job=\"prometheus\"}[5m]",
      "http_requests_total offset 5m"
    )

    val expected = Seq(
      "PeriodicSeries(RawSeries(IntervalSelector(WrappedArray(1524855688170),WrappedArray(1524855988170)),List(ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988170,1,1524855988170)",
      "PeriodicSeries(RawSeries(IntervalSelector(WrappedArray(1524855688170),WrappedArray(1524855988170)),List(ColumnFilter(job,Equals(prometheus)), ColumnFilter(group,Equals(canary)), ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988170,1,1524855988170)",
      "PeriodicSeries(RawSeries(IntervalSelector(WrappedArray(1524855688170),WrappedArray(1524855988170)),List(ColumnFilter(environment,Equals(staging|testing|development)), ColumnFilter(method,Equals(GET)), ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988170,1,1524855988170)",
      "RawSeries(IntervalSelector(WrappedArray(1524855688170),WrappedArray(1524855988170)),List(ColumnFilter(job,Equals(prometheus)), ColumnFilter(__name__,Equals(http_requests_total))),List())",
      "PeriodicSeries(RawSeries(IntervalSelector(WrappedArray(1524855688170),WrappedArray(1524855988170)),List(ColumnFilter(__name__,Equals(http_requests_total))),List()),1524855988170,1,1524855988170)"
    )
    val qts: Long = 1524855988170L
    queries.zipWithIndex.foreach { case (q, i) =>
      val lp = PrometheusQLParser.queryToLogicalPlan(q, qts)
      lp.toString should be(expected(i))
    }

  }

  def parseSuccessfully(query: String): Unit = {
    val expression = PrometheusQLParser.parseQuery(query)
  }

  def parseError(query: String): Unit = {
    intercept[IllegalArgumentException] {
      PrometheusQLParser.parseQuery(query)
    }
  }


}
