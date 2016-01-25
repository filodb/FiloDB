package filodb.core.metadata

import org.velvia.filo.TupleRowReader

import filodb.core._
import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}

class ComputedColumnSpec extends FunSpec with Matchers {
  import NamesTestData._

  describe(":getOrElse") {
    it("should return WrongNumberArguments when # args not 2") {
      val resp = RichProjection.make(dataset.copy(partitionColumns = Seq(":getOrElse 1")), schema)
      resp.isFailure should be (true)
      resp.recover {
        case WrongNumberArguments(given, expected) =>
          given should equal (1)
          expected should equal (2)
      }
    }

    it("should return BadArgument if source column not found") {
      val resp = RichProjection.make(dataset.copy(partitionColumns = Seq(":getOrElse xx 1")), schema)
      resp.isFailure should be (true)
      resp.recover {
        case BadArgument(reason) => reason should include ("Could not find source column")
      }
    }

    it("should return BadArgument if cannot parse non-string default value") {
      val resp = RichProjection.make(dataset.copy(partitionColumns = Seq(":getOrElse age notInt")), schema)
      resp.isFailure should be (true)
      resp.recover {
        case BadArgument(reason) => reason should include ("Could not parse")
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
}