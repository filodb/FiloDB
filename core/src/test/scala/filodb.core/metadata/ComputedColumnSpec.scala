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
      resp.recover[Any] {
        case ComputedColumnErrs(Seq(WrongNumberArguments(given, expected))) =>
          given should equal (1)
          expected should equal (2)
      }
    }

    it("should return BadArgument if source column not found") {
      val resp = RichProjection.make(dataset.copy(partitionColumns = Seq(":getOrElse xx 1")), schema)
      resp.isBad should be (true)
      resp.recover[Any] {
        case ComputedColumnErrs(Seq(BadArgument(reason))) =>
          reason should include ("Could not find source column")
      }
    }

    it("should return BadArgument if cannot parse non-string default value") {
      val resp = RichProjection.make(dataset.copy(partitionColumns = Seq(":getOrElse age notInt")), schema)
      resp.isBad should be (true)
      resp.recover[Any] {
        case ComputedColumnErrs(Seq(BadArgument(reason))) =>
          reason should include ("Could not parse")
      }
    }

    it("should parse normal (non-null) value and pass it through") {
      val proj = RichProjection(dataset.copy(partitionColumns = Seq(":getOrElse age -1")), schema)
      val partFunc = proj.partitionKeyFunc

      partFunc(TupleRowReader(names(3))).getLong(0) should equal (40L)
    }

    it("should parse null value and pass through default value") {
      val proj = RichProjection(dataset.copy(partitionColumns = Seq(":getOrElse last --")), schema)
      val partFunc = proj.partitionKeyFunc

      partFunc(TupleRowReader(names(3))).getString(0) should equal ("--")
    }
  }

  describe(":round") {
    it("should return BadArgument if rounding value different type than source column") {
      val resp = RichProjection.make(dataset.copy(partitionColumns = Seq(":round age 1.23")), schema)
      resp.isBad should be (true)
      resp.recover[Any] {
        case ComputedColumnErrs(Seq(BadArgument(reason))) =>
          reason should include ("Could not parse")
      }
    }

    it("should return BadArgument if attempt to use :round with unsupported type") {
      val resp = RichProjection.make(dataset.copy(partitionColumns = Seq(":round first 10")), schema)
      resp.isBad should be (true)
      resp.recover[Any] {
        case ComputedColumnErrs(Seq(BadArgument(reason))) =>
          reason should include ("not in allowed")
      }
    }

    it("should round long value") {
      val proj = RichProjection(dataset.copy(partitionColumns = Seq(":round age 10")), schema)
      val partFunc = proj.partitionKeyFunc

      partFunc(TupleRowReader(names(1))).getLong(0) should equal (20L)
    }

    it("should round double value") {
      val dblDataset = Dataset("a", "dbl", ":string 0", ":round dbl 2.5")
      val dblColumn = DataColumn(0, "dbl", "a", 0, Column.ColumnType.DoubleColumn)
      val proj = RichProjection(dblDataset, Seq(dblColumn))
      proj.partitionKeyFunc(TupleRowReader((Some(2.499), None))).getDouble(0) should equal (0.0)
      proj.partitionKeyFunc(TupleRowReader((Some(4.999), None))).getDouble(0) should equal (2.5)
      proj.partitionKeyFunc(TupleRowReader((Some(2.501), None))).getDouble(0) should equal (2.5)
    }
  }

  describe(":timeslice") {
    it("should return BadArgument if time duration string not formatted properly") {
      val resp = RichProjection.make(dataset.copy(
                                       partitionColumns = Seq(":timeslice age 2zz")), schema)
      resp.isBad should be (true)
      resp.recover[Any] {
        case ComputedColumnErrs(Seq(BadArgument(reason))) =>
          reason should include ("Could not parse time unit")
      }
    }

    it("should timeslice long values as milliseconds") {
      val proj = RichProjection(dataset.copy(partitionColumns = Seq(":timeslice age 5s")), schema)
      val partFunc = proj.partitionKeyFunc

      partFunc(TupleRowReader(names(1))).getLong(0) should equal (0L)
      partFunc(TupleRowReader(names(1).copy(_3 = Some(9999L)))).getLong(0) should equal (5000L)
    }

    it("should timeslice Timestamp values") {
      val tsDataset = Dataset("a", "ts", ":string 0", ":timeslice ts 5m")
      val tsColumn = DataColumn(0, "ts", "a", 0, Column.ColumnType.TimestampColumn)
      val proj = RichProjection(tsDataset, Seq(tsColumn))

      val reader = TupleRowReader((Some(new Timestamp(300001L)), None))
      proj.partitionKeyFunc(reader).getLong(0) should equal (300000L)
    }
  }

  describe(":monthOfYear") {
    it("should return month of year for timestamp column") {
      val tsDataset = Dataset("a", "ts", ":string 0", ":monthOfYear ts")
      val tsColumn = DataColumn(0, "ts", "a", 0, Column.ColumnType.TimestampColumn)
      val proj = RichProjection(tsDataset, Seq(tsColumn))

      val reader = TupleRowReader((Some(new Timestamp(300001L)), None))
      proj.partitionKeyFunc(reader).getInt(0) should equal (1)
    }
  }

  describe(":stringPrefix") {
    it("should take string prefix") {
      val proj = RichProjection(dataset.copy(partitionColumns = Seq(":stringPrefix first 2")), schema)
      val partFunc = proj.partitionKeyFunc

      partFunc(TupleRowReader(names(1))).getString(0) should equal ("Nd")
    }

    it("should return empty string if column value null") {
      val proj = RichProjection(dataset.copy(partitionColumns = Seq(":stringPrefix last 3")), schema)
      val partFunc = proj.partitionKeyFunc

      partFunc(TupleRowReader(names(3))).getString(0) should equal ("")
    }
  }

  describe(":hash") {
    it("should hash different string values to int between 0 and N") {
      val proj = RichProjection(dataset.copy(partitionColumns = Seq(":hash first 10")), schema)
      val partFunc = proj.partitionKeyFunc

      // This code will change quite a bit, this is just to get it to pass
      partFunc(TupleRowReader(names(1))).getInt(0) should equal (1)
      partFunc(TupleRowReader(names(2))).getInt(0) should equal (0)
    }

    it("should hash long values to int between 0 and N") {
      val proj = RichProjection(dataset.copy(partitionColumns = Seq(":hash age 8")), schema)
      val partFunc = proj.partitionKeyFunc

      partFunc(TupleRowReader(names(1))).getInt(0) should equal (4)
    }
  }
}
