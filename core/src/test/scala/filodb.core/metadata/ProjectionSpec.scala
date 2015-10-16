package filodb.core.metadata

import filodb.core.columnstore.SegmentSpec

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}

class ProjectionSpec extends FunSpec with Matchers {
  import SegmentSpec._
  import RichProjection._

  describe("RichProjection") {
    it("should get BadSchema if cannot find sort column") {
      val resp = RichProjection.make(Dataset("a", "boo"), schema)
      resp.isFailure should be (true)
      resp.recover {
        case BadSchema(reason) => reason should include ("Sort column boo not in columns")
      }
    }

    it("should get BadSchema if sort column is not supported type") {
      val resp = RichProjection.make(Dataset("a", "first"), schema)
      resp.isFailure should be (true)
      resp.recover {
        case BadSchema(reason) => reason should include ("Unsupported sort column type")
      }
    }

    it("should get BadSchema if projection columns are missing from schema") {
      val missingColProj = Projection(0, "a", "age", columns = Seq("first", "yards"))
      val missingColDataset = Dataset("a", Seq(missingColProj), Dataset.DefaultPartitionColumn)
      val resp = RichProjection.make(missingColDataset, schema)
      resp.isFailure should be (true)
      resp.recover {
        case BadSchema(reason) => reason should include ("Specified projection columns are missing")
      }
    }

    it("should get RichProjection back with proper dataset and schema") {
      val resp = RichProjection(dataset, schema)
      resp.sortColumn should equal (schema(2))
      resp.sortColNo should equal (2)
      resp.columns should equal (schema)
    }
  }
}