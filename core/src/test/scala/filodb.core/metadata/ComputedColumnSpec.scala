package filodb.core.metadata

import java.sql.Timestamp
import org.scalactic._
import org.velvia.filo.TupleRowReader

import filodb.core._
import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}

class ComputedColumnSpec extends FunSpec with Matchers {
  import NamesTestData._
  import RichProjection.ComputedColumnErrs

  describe(":getOrElse") {
    it("should return WrongNumberArguments when # args not 2") {
      val resp = RichProjection.make(dataset.copy(partitionColumns = Seq(":getOrElse 1")), schema)
      resp.isBad should be (true)
      resp.recover {
        case ComputedColumnErrs(Seq(WrongNumberArguments(given, expected))) =>
          given should equal (1)
          expected should equal (2)
      }
    }

    it("should return BadArgument if source column not found") {
      val resp = RichProjection.make(dataset.copy(partitionColumns = Seq(":getOrElse xx 1")), schema)
      resp.isBad should be (true)
      resp.recover {
        case ComputedColumnErrs(Seq(BadArgument(reason))) =>
          reason should include ("Could not find source column")
      }
    }

    it("should return BadArgument if cannot parse non-string default value") {
      val resp = RichProjection.make(dataset.copy(partitionColumns = Seq(":getOrElse age notInt")), schema)
      resp.isBad should be (true)
      resp.recover {
        case ComputedColumnErrs(Seq(BadArgument(reason))) =>
          reason should include ("Could not parse")
      }
    }

    it("should parse normal (non-null) value and pass it through") {
      val proj = RichProjection(dataset.copy(partitionColumns = Seq(":getOrElse age -1")), schema)
      val partFunc = proj.partitionKeyFunc

      partFunc(TupleRowReader(names(3))) should equal (40L)
    }

    it("should parse null value and pass through default value") {
      val proj = RichProjection(dataset.copy(partitionColumns = Seq(":getOrElse last --")), schema)
      val partFunc = proj.partitionKeyFunc

      partFunc(TupleRowReader(names(3))) should equal ("--")
    }
  }

  describe(":round") {
    it("should return BadArgument if rounding value different type than source column") {
      val resp = RichProjection.make(dataset.copy(partitionColumns = Seq(":round age 1.23")), schema)
      resp.isBad should be (true)
      resp.recover {
        case ComputedColumnErrs(Seq(BadArgument(reason))) =>
          reason should include ("Could not parse")
      }
    }

    it("should return BadArgument if attempt to use :round with unsupported type") {
      val resp = RichProjection.make(dataset.copy(partitionColumns = Seq(":round first 10")), schema)
      resp.isBad should be (true)
      resp.recover {
        case ComputedColumnErrs(Seq(BadArgument(reason))) =>
          reason should include ("not in allowed")
      }
    }

    it("should round long value") {
      val proj = RichProjection(dataset.copy(partitionColumns = Seq(":round age 10")), schema)
      val partFunc = proj.partitionKeyFunc

      partFunc(TupleRowReader(names(1))) should equal (20L)
    }

    it("should round double value") {
      val dblDataset = Dataset("a", ":round dbl 2.0", ":string /0")
      val dblColumn = DataColumn(0, "dbl", "a", 0, Column.ColumnType.DoubleColumn)
      val proj = RichProjection(dblDataset, Seq(dblColumn))
      proj.rowKeyFunc(TupleRowReader((Some(1.999), None))) should equal (0.0)
      proj.rowKeyFunc(TupleRowReader((Some(3.999), None))) should equal (2.0)
      proj.rowKeyFunc(TupleRowReader((Some(2.00001), None))) should equal (2.0)
    }
  }

  describe(":timeslice") {
    it("should return BadArgument if time duration string not formatted properly") {
      val resp = RichProjection.make(dataset.copy(
                                       partitionColumns = Seq(":timeslice age 2zz")), schema)
      resp.isBad should be (true)
      resp.recover {
        case ComputedColumnErrs(Seq(BadArgument(reason))) =>
          reason should include ("Could not parse time unit")
      }
    }

    it("should timeslice long values as milliseconds") {
      val proj = RichProjection(dataset.copy(partitionColumns = Seq(":timeslice age 5s")), schema)
      val partFunc = proj.partitionKeyFunc

      partFunc(TupleRowReader(names(1))) should equal (0L)
      partFunc(TupleRowReader(names(1).copy(_3 = Some(9999L)))) should equal (5000L)
    }

    it("should timeslice Timestamp values") {
      val tsDataset = Dataset("a", ":timeslice ts 5m", ":string /0")
      val tsColumn = DataColumn(0, "ts", "a", 0, Column.ColumnType.TimestampColumn)
      val proj = RichProjection(tsDataset, Seq(tsColumn))

      proj.rowKeyFunc(TupleRowReader((Some(new Timestamp(300001L)), None))) should equal (300000L)
    }
  }

  describe(":stringPrefix") {
    it("should take string prefix") {
      val proj = RichProjection(dataset.copy(partitionColumns = Seq(":stringPrefix first 2")), schema)
      val partFunc = proj.partitionKeyFunc

      partFunc(TupleRowReader(names(1))) should equal ("Nd")
    }

    it("should return empty string if column value null") {
      val proj = RichProjection(dataset.copy(partitionColumns = Seq(":stringPrefix last 3")), schema)
      val partFunc = proj.partitionKeyFunc

      partFunc(TupleRowReader(names(3))) should equal ("")
    }
  }
}