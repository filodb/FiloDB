package filodb.coordinator.client

import org.parboiled2.ParseError
import scala.util.{Try, Success, Failure}

import filodb.core.query.{ColumnFilter, Filter}

import org.scalatest.{FunSpec, Matchers}

class PromQLParserSpec extends FunSpec with Matchers {
  import PromQLParser._

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

  it("should parse valid input correctly") {
    val filter1 = ColumnFilter("method", Filter.Equals("GET"))

    validate("""http-requests-total#avg""") should equal (
      VectorExprOnlyQuery(PartitionSpec("http-requests-total", "avg", Nil, DefaultRange)))

    validate("""http-requests-total#avg{method="GET"}""") should equal (
      VectorExprOnlyQuery(PartitionSpec("http-requests-total", "avg", Seq(filter1), DefaultRange)))

    validate("""http-requests-total#avg[1h]""") should equal (
      VectorExprOnlyQuery(PartitionSpec("http-requests-total", "avg", Nil, SecsInHour)))

    validate("""sum(http-requests-total#avg{method="GET"}[5m])""") should equal (
      FunctionQuery("sum", None, PartitionSpec("http-requests-total", "avg", Seq(filter1), 300)))

    validate("""sum(100, http-requests-total#avg[5m])""") should equal (
      FunctionQuery("sum", Some("100"), PartitionSpec("http-requests-total", "avg", Nil, 300)))

    val filters = Seq(filter1, ColumnFilter("app", Filter.Equals("myApp")))
    validate("""sum(50, http-requests-total#avg{method="GET", app="myApp"}[5m])""") should equal (
      FunctionQuery("sum", Some("50"), PartitionSpec("http-requests-total", "avg", filters, 300)))

    validate("""topk(5, sum(http-requests-total#avg[5m]))""") should equal (
      FunctionQuery("topk", Some("5"),
        FunctionQuery("sum", None, PartitionSpec("http-requests-total", "avg", Nil, 300))))
  }

  it("should return ParseError for invalid input") {
    parse("""abasdfasd""") should be ('failure)

    // missing parenthesis
    parse("""topk(5, sum(http-requests-total#avg[5m])""") should be ('failure)
  }
}