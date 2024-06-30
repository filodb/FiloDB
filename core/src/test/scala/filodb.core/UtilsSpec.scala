package filodb.core

import filodb.core.query.{ColumnFilter, ColumnFilterMap, Filter}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class UtilsSpec extends AnyFunSpec with Matchers {

  private def equals(column: String, value: String): ColumnFilter = {
    ColumnFilter(column, Filter.Equals(value))
  }

  private def regex(column: String, value: String): ColumnFilter = {
    ColumnFilter(column, Filter.EqualsRegex(value))
  }

  it ("should store/retrieve ColumnFilterMap elements correctly") {
    {
      // empty map
      val cfMap = new ColumnFilterMap[String](Nil)
      cfMap.get(Map("foo" -> "bar")) shouldEqual None
      cfMap.get(Map()) shouldEqual None
    }

    {
      // too many regex filters
      val entries = Seq(
        (Seq(equals("a", "a")), "foo"),
        (Seq(regex("b", "b"), regex("c", "c")), "bar")
      )
      intercept[IllegalArgumentException] {
        new ColumnFilterMap[String](entries)
      }
    }

    {
      // single equals filter
      val entries = Seq(
        (Seq(equals("a", "a")), "foo")
      )
      val cfMap = new ColumnFilterMap[String](entries)
      cfMap.get(Map("a" -> "a")).get shouldEqual "foo"
      cfMap.get(Map("a" -> "b")) shouldEqual None
      cfMap.get(Map("b" -> "b")) shouldEqual None
      cfMap.get(Map()) shouldEqual None
    }

    {
      // single regex filter
      val entries = Seq(
        (Seq(regex("a", ".*")), "foo")
      )
      val cfMap = new ColumnFilterMap[String](entries)
      cfMap.get(Map("a" -> "hello")).get shouldEqual "foo"
      cfMap.get(Map("b" -> "hello")) shouldEqual None
      cfMap.get(Map()) shouldEqual None
    }

    {
      // multiple filters
      val entries = Seq(
        (Seq(
          regex("a", ".*"),
          equals("b", "b"),
          equals("c", "c"),
          equals("d", "d")), "foo")
      )
      val cfMap = new ColumnFilterMap[String](entries)
      cfMap.get(Map("a" -> "hello")) shouldEqual None
      cfMap.get(Map("b" -> "b", "c" -> "c", "d" -> "d")) shouldEqual None
      cfMap.get(Map("a" -> "hello", "b" -> "b", "c" -> "c", "d" -> "d")).get shouldEqual "foo"
      cfMap.get(Map("a" -> "hello", "b" -> "b", "c" -> "c", "d" -> "d", "e" -> "e")).get shouldEqual "foo"
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
      val cfMap = new ColumnFilterMap[String](entries)
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
      // multiple sets at same path
      val entries = Seq(
        (Seq(regex("a", "a.*")), "a"),
        (Seq(regex("a", "b.*")), "b"),
        (Seq(equals("a", "c")), "c"),
        (Seq(equals("a", "a")), "matches_first"),
      )
      val cfMap = new ColumnFilterMap[String](entries)
      cfMap.get(Map("a" -> "a123")).get shouldEqual "a"
      cfMap.get(Map("a" -> "b123")).get shouldEqual "b"
      cfMap.get(Map("a" -> "c")).get shouldEqual "c"
      cfMap.get(Map("a" -> "a")).isDefined shouldEqual true
      cfMap.get(Map()) shouldEqual None
    }

    {
      // multiple sets; different lengths
      val entries = Seq(
        (Seq(equals("a", "a"), equals("b", "b")), "a"),
        (Seq(equals("c", "c")), "c"),
      )
      val cfMap = new ColumnFilterMap[String](entries)
      cfMap.get(Map("a" -> "a")) shouldEqual None
      cfMap.get(Map("a" -> "a", "b" -> "b")).get shouldEqual "a"
      cfMap.get(Map("c" -> "c")).get shouldEqual "c"
      cfMap.get(Map("b" -> "b", "c" -> "c")).get shouldEqual "c"
      cfMap.get(Map("a" -> "a", "b" -> "b", "c" -> "c")).isDefined shouldEqual true
      cfMap.get(Map()) shouldEqual None
    }
  }
}
