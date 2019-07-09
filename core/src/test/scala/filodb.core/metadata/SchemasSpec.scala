package filodb.core.metadata

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}

import filodb.core._

class SchemasSpec extends FunSpec with Matchers {
  import Column.ColumnType._
  import Dataset._
  import NamesTestData._

  describe("DataSchema") {
    it("should return NotNameColonType if column specifiers not name:type format") {
      val resp1 = DataSchema.make("dataset", dataColSpecs :+ "column2", Nil, "first")
      resp1.isBad shouldEqual true
      resp1.swap.get shouldEqual ColumnErrors(Seq(NotNameColonType("column2")))
    }

    it("should return BadColumnParams if name:type:params portion not valid key=value pairs") {
      val resp1 = DataSchema.make("dataset", dataColSpecs :+ "column2:a:b", Nil, "first")
      resp1.isBad shouldEqual true
      resp1.swap.get shouldBe a[ColumnErrors]
      val errors = resp1.swap.get.asInstanceOf[ColumnErrors].errs
      errors should have length 1
      errors.head shouldBe a[BadColumnParams]
    }

    it("should return BadColumnParams if required param config not specified") {
      val resp1 = DataSchema.make("dataset", dataColSpecs :+ "h:hist:foo=bar", Nil, "first")
      resp1.isBad shouldEqual true
      resp1.swap.get shouldBe a[ColumnErrors]
      val errors = resp1.swap.get.asInstanceOf[ColumnErrors].errs
      errors should have length 1
      errors.head shouldBe a[BadColumnParams]

      val resp2 = DataSchema.make("dataset", dataColSpecs :+ "h:hist:counter=bar", Nil, "first")
      resp2.isBad shouldEqual true
      resp2.swap.get shouldBe a[ColumnErrors]
      val errors2 = resp2.swap.get.asInstanceOf[ColumnErrors].errs
      errors2 should have length 1
      errors2.head shouldBe a[BadColumnParams]
    }

    it("should return BadColumnName if illegal chars in column name") {
      val resp1 = DataSchema.make("dataset", Seq("col, umn1:string"), Nil, "first")
      resp1.isBad shouldEqual true
      val errors = resp1.swap.get match {
        case ColumnErrors(errs) => errs
        case x => throw new RuntimeException(s"Did not expect $x")
      }
      errors should have length (1)
      errors.head shouldBe a[BadColumnName]
    }

    it("should return BadColumnType if unsupported type specified in column spec") {
      val resp1 = DataSchema.make("dataset", dataColSpecs :+ "part:linkedlist", Nil, "first")
      resp1.isBad shouldEqual true
      val errors = resp1.swap.get match {
        case ColumnErrors(errs) => errs
        case x => throw new RuntimeException(s"Did not expect $x")
      }
      errors should have length (1)
      errors.head shouldEqual BadColumnType("linkedlist")
    }

    it("should return BadColumnName if value column not one of other columns") {
      val conf2 = ConfigFactory.parseString("""
                    {
                      columns = ["first:string", "last:string", "age:long"]
                      value-column = "first2"
                      downsamplers = []
                    }""")
      val resp = DataSchema.fromConfig("dataset", conf2)
      resp.isBad shouldEqual true
      resp.swap.get shouldBe a[BadColumnName]
    }

    it("should return multiple column spec errors") {
      val resp1 = DataSchema.make("dataset", Seq("first:str", "age:long", "la(st):int"), Nil, "first")
      resp1.isBad shouldEqual true
      val errors = resp1.swap.get match {
        case ColumnErrors(errs) => errs
        case x => throw new RuntimeException(s"Did not expect $x")
      }
      errors should have length (2)
      errors.head shouldEqual BadColumnType("str")
    }

    it("should return NoTimestampRowKey if non timestamp used for row key / first column") {
      val ds1 = DataSchema.make("dataset", dataColSpecs, Nil, "first")
      ds1.isBad shouldEqual true
      ds1.swap.get shouldBe a[NoTimestampRowKey]
    }

    it("should return a valid Dataset when a good specification passed") {
      val conf2 = ConfigFactory.parseString("""
                    {
                      columns = ["timestamp:ts", "code:long", "event:string"]
                      value-column = "event"
                      downsamplers = []
                    }""")
      val schema = DataSchema.fromConfig("dataset", conf2).get
      schema.columns should have length (3)
      schema.columns.map(_.id) shouldEqual Seq(0, 1, 2)
      schema.columns.map(_.columnType) shouldEqual Seq(TimestampColumn, LongColumn, StringColumn)
      schema.timestampColumn.name shouldEqual "timestamp"
    }
  }

  describe("PartitionSchema") {
    it("should allow MapColumns only in last position of partition key") {
      val mapCol = "tags:map"

      // OK: only partition column is map
      val ds1 = PartitionSchema.make(Seq(mapCol), DatasetOptions.DefaultOptions).get
      ds1.columns.map(_.name) should equal (Seq("tags"))

      // OK: last partition column is map
      val ds2 = PartitionSchema.make(Seq("first:string", mapCol), DatasetOptions.DefaultOptions).get
      ds2.columns.map(_.name) should equal (Seq("first", "tags"))

      // Not OK: first partition column is map
      val resp3 = PartitionSchema.make(Seq(mapCol, "first:string"), DatasetOptions.DefaultOptions)
      resp3.isBad shouldEqual true
      resp3.swap.get shouldBe an[IllegalMapColumn]
    }
  }
}
