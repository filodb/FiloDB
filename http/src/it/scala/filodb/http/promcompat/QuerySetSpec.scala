package filodb.http.promcompat

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.typesafe.config.ConfigFactory

class QuerySetSpec extends AnyFunSpec with Matchers {
  it("loads categories of 3-tuple queries") {
    val cats = QuerySet.load("/promcompat/queries-instant.conf")
    cats.keySet should contain ("raw_data")
    cats("raw_data") should have length 2
    cats("raw_data").head.promql should startWith ("my_gauge")
    cats("aggregate_functions").head.errorLimit shouldEqual "0.0001"
  }

  it("keeps the useful load sanity checks") {
    val cats = QuerySet.load("/promcompat/queries-instant.conf")
    cats("rate_functions").map(_.promql).exists(_.contains("rate(")) shouldEqual true
  }

  it("rejects a category whose list length is not a multiple of 3") {
    val bad = ConfigFactory.parseString("""bad_category = ["query_only", "no_flags"]""")
    an [IllegalArgumentException] should be thrownBy QuerySet.fromConfig(bad)
  }
}

