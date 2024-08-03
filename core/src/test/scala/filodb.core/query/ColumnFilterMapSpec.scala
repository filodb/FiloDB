package filodb.core.query

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ColumnFilterMapSpec extends AnyFunSpec with Matchers {

  private def equals(column: String, value: String): ColumnFilter = {
    ColumnFilter(column, Filter.Equals(value))
  }

  private def regex(column: String, value: String): ColumnFilter = {
    ColumnFilter(column, Filter.EqualsRegex(value))
  }

  it("DefaultColumnFilterMap should store/retrieve elements correctly") {
    {
      // empty map
      val cfMap = new DefaultColumnFilterMap[String](Nil)
      cfMap.get(Map("foo" -> "bar")) shouldEqual None
      cfMap.get(Map("foo" -> "bar", "baz" -> "bat")) shouldEqual None
      cfMap.get(Map()) shouldEqual None
    }

    {
      // single equals filter
      val entries = Seq(
        (Seq(equals("a", "a")), "foo")
      )
      val cfMap = new DefaultColumnFilterMap[String](entries)
      cfMap.get(Map("a" -> "a")).get shouldEqual "foo"
      cfMap.get(Map("a" -> "a", "b" -> "b")).get shouldEqual "foo"
      cfMap.get(Map("a" -> "b")) shouldEqual None
      cfMap.get(Map("b" -> "b")) shouldEqual None
      cfMap.get(Map("a" -> "b", "b" -> "a")) shouldEqual None
      cfMap.get(Map()) shouldEqual None
    }

    {
      // single regex filter
      val entries = Seq(
        (Seq(regex("a", ".*")), "foo")
      )
      val cfMap = new DefaultColumnFilterMap[String](entries)
      cfMap.get(Map("a" -> "hello")).get shouldEqual "foo"
      cfMap.get(Map("a" -> "hello", "b" -> "goodbye")).get shouldEqual "foo"
      cfMap.get(Map("b" -> "hello")) shouldEqual None
      cfMap.get(Map()) shouldEqual None
    }

    {
      // multiple filters
      val entries = Seq(
        (Seq(
          regex("a", "aaa.*"),
          equals("b", "b"),
          regex("c", "ccc.*"),
          equals("d", "d")), "foo")
      )
      val cfMap = new DefaultColumnFilterMap[String](entries)
      cfMap.get(Map("a" -> "hello")) shouldEqual None
      cfMap.get(Map("b" -> "b", "d" -> "d")) shouldEqual None
      cfMap.get(Map("a" -> "aaa123", "c" -> "ccc123")) shouldEqual None
      cfMap.get(Map("a" -> "aaa123", "b" -> "b", "c" -> "ccc123", "d" -> "d")).get shouldEqual "foo"
      cfMap.get(Map("a" -> "aaa123", "b" -> "b", "c" -> "ccc123", "d" -> "d", "e" -> "e")).get shouldEqual "foo"
      cfMap.get(Map("e" -> "e")) shouldEqual None
      cfMap.get(Map()) shouldEqual None
    }

    {
      // multiple sets with single filter
      val entries = Seq(
        (Seq(regex("a", ".*")), "a"),
        (Seq(regex("b", ".*")), "b"),
        (Seq(equals("c", "c")), "c"),
      )
      val cfMap = new DefaultColumnFilterMap[String](entries)
      cfMap.get(Map("a" -> "hello")).get shouldEqual "a"
      cfMap.get(Map("b" -> "hello")).get shouldEqual "b"
      cfMap.get(Map("c" -> "hello")) shouldEqual None
      cfMap.get(Map("d" -> "d")) shouldEqual None
      cfMap.get(Map("c" -> "c")).get shouldEqual "c"
      cfMap.get(Map()) shouldEqual None
      cfMap.get(Map("a" -> "hello", "b" -> "hello")).isDefined shouldEqual true
      cfMap.get(Map("a" -> "hello", "b" -> "hello", "d" -> "d")).isDefined shouldEqual true
    }

    {
      // multiple sets; different lengths at same paths
      val entries = Seq(
        (Seq(
          equals("a", "a")), "1"),
        (Seq(
          equals("a", "a"),
          equals("b", "b")), "2"),
        (Seq(
          equals("a", "a"),
          equals("b", "b"),
          equals("c", "c")), "3"),
        (Seq(
          equals("b", "b"),
          equals("c", "c")), "4"),
        (Seq(
          regex("a", "aaa.*")), "1r"),
        (Seq(
          regex("a", "aaa.*"),
          regex("b", "bbb.*")), "2r"),
        (Seq(
          regex("a", "aaa.*"),
          regex("b", "bbb.*"),
          regex("c", "ccc.*")), "3r"),
        (Seq(
          regex("b", "bbb.*123.*"),
          regex("c", "ccc.*123.*")), "4r"),
      )
      val cfMap = new DefaultColumnFilterMap[String](entries)
      cfMap.get(Map()) shouldEqual None
      cfMap.get(Map("a" -> "b")) shouldEqual None
      cfMap.get(Map("d" -> "d")) shouldEqual None
      cfMap.get(Map("a" -> "a")).get shouldEqual "1"
      cfMap.get(Map("a" -> "a", "b" -> "b")).isDefined shouldEqual true
      cfMap.get(Map("a" -> "a", "b" -> "b", "c" -> "c")).isDefined shouldEqual true
      cfMap.get(Map("b" -> "b", "c" -> "c")).get shouldEqual "4"
      cfMap.get(Map("a" -> "aaa123")).get shouldEqual "1r"
      cfMap.get(Map("a" -> "aaa123", "b" -> "bbb123")).isDefined shouldEqual true
      cfMap.get(Map("a" -> "aaa123", "b" -> "bbb123", "c" -> "ccc123")).isDefined shouldEqual true
      cfMap.get(Map("b" -> "bbbXYZ123", "c" -> "cccXYZ123")).get shouldEqual "4r"
    }
  }

  it("FastColumnFilterMap should store/retrieve elements correctly") {
    {
      // empty map
      val cfMap = new FastColumnFilterMap[String](
        "equals",
        Map(),
        "filter",
        Nil)
      cfMap.get(Map("equals" -> "foo", "filter" -> "bar")) shouldEqual None
      cfMap.get(Map("equals" -> "foo", "filter" -> "bar", "DNE" -> "DNE")) shouldEqual None
    }

    {
      // equals filters only
      val equalsValues = Map(
        "foo" -> "fooVal",
        "bar" -> "barVal",
        "baz" -> "bazVal"
      )
      val cfMap = new FastColumnFilterMap[String](
        "equals", equalsValues,
        "filter", Nil)
      cfMap.get(Map("equals" -> "hello", "filter" -> "DNE")) shouldEqual None
      cfMap.get(Map("equals" -> "hello", "filter" -> "DNE", "DNE" -> "DNE")) shouldEqual None
      cfMap.get(Map("equals" -> "foo", "filter" -> "DNE")).get shouldEqual "fooVal"
      cfMap.get(Map("equals" -> "foo", "filter" -> "DNE", "DNE" -> "DNE")).get shouldEqual "fooVal"
      cfMap.get(Map("equals" -> "bar", "filter" -> "DNE")).get shouldEqual "barVal"
      cfMap.get(Map("equals" -> "baz", "filter" -> "DNE")).get shouldEqual "bazVal"
      cfMap.get(Map("equals" -> "DNE", "filter" -> "foo")) shouldEqual None
    }

    {
      // filtered values only
      val filterEltPairs = Seq(
        (Filter.EqualsRegex("foo.*"), "fooVal"),  // Prefix regex!
        (Filter.EqualsRegex("bar.123.*"), "barVal"),  // Not prefix regex (contains regex chars before .*).
        (Filter.Equals("baz"), "bazVal"),
      )
      val cfMap = new FastColumnFilterMap[String](
        "equals",
        Map(),
        "filter",
        filterEltPairs)
      cfMap.get(Map("equals" -> "DNE", "filter" -> "DNE")) shouldEqual None
      cfMap.get(Map("equals" -> "DNE", "filter" -> "foo123")).get shouldEqual "fooVal"
      cfMap.get(Map("equals" -> "DNE", "filter" -> "foo123", "DNE" -> "DNE")).get shouldEqual "fooVal"
      cfMap.get(Map("equals" -> "DNE", "filter" -> "barA123456")).get shouldEqual "barVal"
      cfMap.get(Map("equals" -> "DNE", "filter" -> "baz")).get shouldEqual "bazVal"
    }
  }

  {
    // filter mix
    val equalsValues = Map(
      "foo" -> "equalsFooVal",
      "bar" -> "equalsBarVal",
      "baz" -> "equalsBazVal",
      "bat" -> "equalsBatVal",
    )
    val filterEltPairs = Seq(
      (Filter.EqualsRegex("foo.*"), "filterFooVal"), // Prefix regex!
      (Filter.EqualsRegex("foo.*"), "filterFooVal2"),
      (Filter.EqualsRegex("bar.123.*"), "filterBarVal"), // Not prefix regex (contains regex chars before .*).
      (Filter.EqualsRegex("bar.123.*"), "filterBarVal2"),
      (Filter.Equals("baz"), "filterBazVal"),
      (Filter.Equals("baz"), "filterBazVal2"),
      (Filter.EqualsRegex("bat.*123.*"), "filterBatVal"),
    )
    val cfMap = new FastColumnFilterMap[String](
      "equals",
      equalsValues,
      "filter",
      filterEltPairs)

    cfMap.get(Map("equals" -> "DNE", "filter" -> "DNE")) shouldEqual None
    cfMap.get(Map("equals" -> "DNE", "filter" -> "DNE", "DNE" -> "DNE")) shouldEqual None
    cfMap.get(Map("equals" -> "foo", "filter" -> "DNE")).get shouldEqual "equalsFooVal"
    cfMap.get(Map("equals" -> "foo", "filter" -> "DNE", "DNE" -> "DNE")).get shouldEqual "equalsFooVal"
    cfMap.get(Map("equals" -> "bar", "filter" -> "DNE")).get shouldEqual "equalsBarVal"
    cfMap.get(Map("equals" -> "baz", "filter" -> "DNE")).get shouldEqual "equalsBazVal"

    // Additionally makes sure filtered labels are evaluated in order
    cfMap.get(Map("equals" -> "DNE", "filter" -> "DNE")) shouldEqual None
    cfMap.get(Map("equals" -> "DNE", "filter" -> "foo123")).get shouldEqual "filterFooVal"
    cfMap.get(Map("equals" -> "DNE", "filter" -> "foo123", "DNE" -> "DNE")).get shouldEqual "filterFooVal"
    cfMap.get(Map("equals" -> "DNE", "filter" -> "barA123456")).get shouldEqual "filterBarVal"
    cfMap.get(Map("equals" -> "DNE", "filter" -> "baz")).get shouldEqual "filterBazVal"
    cfMap.get(Map("equals" -> "DNE", "filter" -> "bat123")).get shouldEqual "filterBatVal"

    // Make sure equals label is matched after filtered label
    cfMap.get(Map("equals" -> "baz", "filter" -> "foo123")).get shouldEqual "filterFooVal"
    cfMap.get(Map("equals" -> "baz", "filter" -> "foo123", "DNE" -> "DNE")).get shouldEqual "filterFooVal"
    cfMap.get(Map("equals" -> "baz", "filter" -> "barA123456")).get shouldEqual "filterBarVal"
    cfMap.get(Map("equals" -> "baz", "filter" -> "baz")).get shouldEqual "filterBazVal"
  }
}
