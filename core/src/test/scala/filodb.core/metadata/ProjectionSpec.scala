package filodb.core.metadata

import filodb.core.Setup
import filodb.core.store.MetaStore.BadSchema
import filodb.core.store.{Dataset, MetaStore, Projection}
import org.scalatest.{FunSpec, Matchers}
import org.velvia.filo.TupleRowReader

class ProjectionSpec extends FunSpec with Matchers {

  import Setup._

  describe("Projection") {
    it("should get BadSchema if cannot find sort column") {
      val resp = MetaStore.makeProjection[Long,String](Dataset("a", "boo","far"), schema)
      resp.isFailure should be(true)
      resp.recover {
        case BadSchema(reason) => reason should include("Sort column boo not in columns")
      }
    }

    it("should get BadSchema if sort column is not a valid column") {
      val resp = MetaStore.makeProjection[Long,String](Dataset("a", "first","far"), schema)
      resp.isFailure should be(true)
      resp.recover {
        case BadSchema(reason) => reason should include("Unsupported sort column type")
      }
    }

    it("should get BadSchema if projection columns are missing from schema") {
      val missingColProj = Projection(0, "a", "age","dummy", columns = Seq("first", "yards"))
      val missingColDataset = Dataset("a", Seq(missingColProj), Dataset.DefaultPartitionColumn)
      val resp = MetaStore.makeProjection[String,Long](missingColDataset, schema)
      resp.isFailure should be(true)
      resp.recover {
        case BadSchema(reason) => reason should include("Specified projection columns are missing")
      }
    }

    it("apply() should throw exception for bad schema") {
      intercept[BadSchema] {
        MetaStore.projectionInfo[Long,String](Dataset("a", "boo","far"), schema)
      }
    }

    it("should get RichProjection back with proper dataset and schema") {
      val resp = MetaStore.projectionInfo[String,Long](dataset, schema)
      resp.sortColumn should equal(schema(2))
      resp.sortColNo should equal(2)
      resp.columns should equal(schema)
      resp.segmentType shouldBe a[filodb.core.LongKeyType]
      names.take(3).map(TupleRowReader).map(resp.segmentFunction) should equal(Seq(24L, 28L, 25L))
    }

    it("should create RichProjection properly for String sort key column") {
      val resp = MetaStore.projectionInfo[String,String](Dataset("a", "first","age","last"), schema)
      resp.sortColumn should equal(schema(2))
      resp.sortColNo should equal(2)
      resp.columns should equal(schema)
      resp.segmentType shouldBe a[filodb.core.LongKeyType]
      names.take(3).map(TupleRowReader).map(resp.segmentFunction) should equal(
        Seq(24, 28, 25))
    }

    it("should get working RichProjection back even if called with [Nothing]") {
      val resp = MetaStore.projectionInfo(dataset, schema)
      resp.sortColumn should equal(schema(2))
      resp.sortColNo should equal(2)
      resp.columns should equal(schema)
      resp.segmentType shouldBe a[filodb.core.LongKeyType]
      names.take(3).map(TupleRowReader).map(resp.segmentFunction) should equal(Seq(24L, 28L, 25L))
    }
  }
}
