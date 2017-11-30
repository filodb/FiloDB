package filodb.coordinator.client

import scala.util.{Failure, Success, Try}

import org.parboiled2.ParseError
import org.scalatest.{FunSpec, Matchers}

import filodb.core.metadata.DatasetOptions
import filodb.core.query.{ColumnFilter, Filter}

class PromQLParserSpec extends FunSpec with Matchers {
  import PromQLParser._
  import filodb.coordinator.QueryCommands._

  def validate(query: String): PromQuery = {
    val parser = new PromQLParser(query)
    parser.Query.run() match {
      case Success(p: PromQuery) => p
      case Failure(e: ParseError) =>
        println(s"Failure parsing $query:\n${parser.formatError(e)}")
        throw e
      case Failure(t: Throwable) => throw t
    }
  }

  def parse(query: String): Try[PromQuery] = (new PromQLParser(query)).Query.run()

  val filter1 = ColumnFilter("method", Filter.Equals("GET"))
  val fiveMin = DurationSecs(300)
  val sixMin = DurationSecs(360)

  it("should parse valid input correctly") {

    validate("""http-requests-total#avg""") should equal (
      VectorExprOnlyQuery(PartitionSpec("http-requests-total", "avg", Nil, NoTimeSpecified)))

    validate("""http-requests-total""") should equal (
      VectorExprOnlyQuery(PartitionSpec("http-requests-total", "value", Nil, NoTimeSpecified)))

    validate("""http-requests-total#avg{method="GET"}""") should equal (
      VectorExprOnlyQuery(PartitionSpec("http-requests-total", "avg", Seq(filter1), NoTimeSpecified)))

    validate("""http-requests-total#avg[1h]""") should equal (
      VectorExprOnlyQuery(PartitionSpec("http-requests-total", "avg", Nil, DurationSecs(SecsInHour))))

    validate("""sum(http-requests-total#avg{method="GET"}[5m])""") should equal (
      FunctionQuery("sum", None, PartitionSpec("http-requests-total", "avg", Seq(filter1), fiveMin)))

    validate("""sum(http-requests-total{method="GET"}[5m])""") should equal (
      FunctionQuery("sum", None, PartitionSpec("http-requests-total", "value", Seq(filter1), fiveMin)))

    validate("""sum(100, http-requests-total#avg[5m])""") should equal (
      FunctionQuery("sum", Some("100"), PartitionSpec("http-requests-total", "avg", Nil, fiveMin)))

    val filters = Seq(filter1, ColumnFilter("app", Filter.Equals("myApp")))
    validate("""sum(50, http-requests-total#avg{method="GET", app="myApp"}[5m])""") should equal (
      FunctionQuery("sum", Some("50"), PartitionSpec("http-requests-total", "avg", filters, fiveMin)))

    validate("""topk(5, sum(http-requests-total#avg[5m]))""") should equal (
      FunctionQuery("topk", Some("5"),
        FunctionQuery("sum", None, PartitionSpec("http-requests-total", "avg", Nil, fiveMin))))

    validate("""topk(5, sum(http-requests-total[5m]))""") should equal (
      FunctionQuery("topk", Some("5"),
        FunctionQuery("sum", None, PartitionSpec("http-requests-total", "value", Nil, fiveMin))))
  }

  val filters = Seq(filter1, ColumnFilter("__name__", Filter.Equals("http-requests-total")))
  val justMetricFilt = filters drop 1

  it("should parseAndGetArgs successfully") {
    val parser1 = new PromQLParser("""time_group_avg(30, http-requests-total#avg{method="GET"}[5m])""")
    parser1.parseAndGetArgs(false) match {
      case Success(ArgsAndPartSpec(QueryArgs("time_group_avg", "avg", Seq("30"), MostRecentTime(300000L), _, Nil),
                                   PartitionSpec("http-requests-total", "avg", filters, fiveMin))) =>
    }

    val parser2 = new PromQLParser("""http-requests-total#min[6m]""")
    parser2.parseAndGetArgs(false) match {
      case Success(ArgsAndPartSpec(QueryArgs("last", "min", Nil, MostRecentTime(360000L), "simple", Nil),
                                   PartitionSpec("http-requests-total", "min", justMetricFilt, sixMin))) =>
      case x: Any => throw new RuntimeException(s"Got $x instead")
    }

    val parser3 = new PromQLParser("""http-requests-total[6m]""")
    parser3.parseAndGetArgs(true) match {
      case Success(ArgsAndPartSpec(QueryArgs("last", "value", Nil, _, "simple", Nil),
                                   PartitionSpec("http-requests-total", "value", justMetricFilt, sixMin))) =>
      case x: Any => throw new RuntimeException(s"Got $x instead")
    }
  }

  it("should allow scan of everything (special case)") {
    val parser3 = new PromQLParser(""":_really_scan_everything[6m]""")
    parser3.parseAndGetArgs(true) match {
      case Success(ArgsAndPartSpec(QueryArgs("last", "value", Nil, _, "simple", Nil),
                                   PartitionSpec(":_really_scan_everything", "value", Nil, sixMin))) =>
      case x: Any => throw new RuntimeException(s"Got $x instead")
    }
  }

  it("should use nonstandard options to parse queries") {
    val myOptions = DatasetOptions.DefaultOptions.copy(valueColumn = "data1",
                                                       metricColumn = "kpi")
    val metricFilter = Seq(ColumnFilter("kpi", Filter.Equals("http-requests-total")))

    val parser3 = new PromQLParser("""http-requests-total[6m]""", myOptions)
    parser3.parseAndGetArgs(true) match {
      case Success(ArgsAndPartSpec(QueryArgs("last", "data1", Nil, _, "simple", Nil),
                                   PartitionSpec("http-requests-total", "data1", metricFilter, sixMin))) =>
      case x: Any => throw new RuntimeException(s"Got $x instead")
    }
  }

  it("should return ParseError for invalid input") {

    // missing parenthesis
    parse("""topk(5, sum(http-requests-total#avg[5m])""") should be ('failure)
  }
}