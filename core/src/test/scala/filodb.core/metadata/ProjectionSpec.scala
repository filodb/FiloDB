package filodb.core.metadata

import org.velvia.filo.TupleRowReader

import filodb.core.columnstore.SegmentSpec
import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}

class ProjectionSpec extends FunSpec with Matchers {
  import SegmentSpec._
  import RichProjection._

  describe("RichProjection") {
    it("should get BadSchema if cannot find sort column") {
      val resp = RichProjection.make[Long](Dataset("a", "boo"), schema)
      resp.isFailure should be (true)
      resp.recover {
        case BadSchema(reason) => reason should include ("Sort column boo not in columns")
      }
    }

    // Now that String is a supported sort column type, this test doesn't make sense anymore.
    // Leave this here though because eventually we will add a column type that cannot be used
    // as a sort column.
    ignore("should get BadSchema if sort column is not supported type") {
      val resp = RichProjection.make[Long](Dataset("a", "first"), schema)
      resp.isFailure should be (true)
      resp.recover {
        case BadSchema(reason) => reason should include ("Unsupported sort column type")
      }
    }

    it("should get BadSchema if projection columns are missing from schema") {
      val missingColProj = Projection(0, "a", "age", columns = Seq("first", "yards"))
      val missingColDataset = Dataset("a", Seq(missingColProj), Dataset.DefaultPartitionColumn)
      val resp = RichProjection.make[Long](missingColDataset, schema)
      resp.isFailure should be (true)
      resp.recover {
        case BadSchema(reason) => reason should include ("Specified projection columns are missing")
      }
    }

    it("apply() should throw exception for bad schema") {
      intercept[BadSchema] { RichProjection[Long](Dataset("a", "boo"), schema) }
    }

    it("should get RichProjection back with proper dataset and schema") {
      val resp = RichProjection[Long](dataset, schema)
      resp.sortColumn should equal (schema(2))
      resp.sortColNo should equal (2)
      resp.columns should equal (schema)
      resp.helper shouldBe a[filodb.core.LongKeyHelper]
      names.take(3).map(TupleRowReader).map(resp.sortKeyFunc) should equal (Seq(24L, 28L, 25L))
    }

    it("should create RichProjection properly for String sort key column") {
      val resp = RichProjection[String](Dataset("a", "first"), schema)
      resp.sortColumn should equal (schema(0))
      resp.sortColNo should equal (0)
      resp.columns should equal (schema)
      resp.helper shouldBe a[filodb.core.StringKeyHelper]
      names.take(3).map(TupleRowReader).map(resp.sortKeyFunc) should equal (
                     Seq("Khalil", "Ndamukong", "Rodney"))
    }

    it("should get working RichProjection back even if called with [Nothing]") {
      val resp = RichProjection(dataset, schema)
      resp.sortColumn should equal (schema(2))
      resp.sortColNo should equal (2)
      resp.columns should equal (schema)
      resp.helper shouldBe a[filodb.core.LongKeyHelper]
      names.take(3).map(TupleRowReader).map(resp.sortKeyFunc) should equal (Seq(24L, 28L, 25L))
    }
  }
}