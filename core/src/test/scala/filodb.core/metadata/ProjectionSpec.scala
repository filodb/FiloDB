package filodb.core.metadata

import org.velvia.filo.TupleRowReader
import scala.language.existentials

import filodb.core._
import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}

class ProjectionSpec extends FunSpec with Matchers {
  import NamesTestData._
  import RichProjection._

  describe("RichProjection") {
    it("should get BadSchema if cannot find row key or segment key") {
      val resp = RichProjection.make(Dataset("a", "boo", "seg"), schema)
      resp.isFailure should be (true)
      resp.recover {
        case BadSchema(reason) => reason should include ("Key column boo not in columns")
      }

      val resp2 = RichProjection.make(Dataset("a", "age", "bar"), schema)
      resp2.isFailure should be (true)
      resp2.recover {
        case BadSchema(reason) => reason should include ("Segment column bar not in columns")
      }
    }

    it("should get BadSchema if any key is not supported type") {
      val schema2 = schema ++ Seq(DataColumn(99, "bool", "a", 0, Column.ColumnType.BitmapColumn))
      val resp = RichProjection.make(Dataset("a", "bool", "seg"), schema2)
      resp.isFailure should be (true)
      resp.recover {
        case BadSchema(reason) => reason should include ("is not supported")
      }
    }

    it("should get BadSchema if projection columns are missing from schema") {
      val missingColProj = Projection(0, "a", Seq("age"), "seg", columns = Seq("first", "yards"))
      val missingColDataset = Dataset("a", Seq(missingColProj), Seq(Dataset.DefaultPartitionColumn))
      val resp = RichProjection.make(missingColDataset, schema)
      resp.isFailure should be (true)
      resp.recover {
        case BadSchema(reason) => reason should include ("Specified projection columns are missing")
      }
    }

    it("should get BadSchema if cannot find partitioning column") {
      val resp = RichProjection.make(dataset.copy(partitionColumns = Seq("boo")), schema)
      resp.isFailure should be (true)
      resp.recover {
        case BadSchema(reason) => reason should include ("Partition column boo not in columns")
      }
    }

    it("should get back partitioning func for default key if partitioning column is default") {
      val resp = RichProjection.make(dataset, schema)
      resp.isSuccess should be (true)
      val partFunc = resp.get.partitionKeyFunc

      partFunc(names.map(TupleRowReader).head) should equal (Dataset.DefaultPartitionKey)
    }

    it("apply() should throw exception for bad schema") {
      intercept[BadSchema] { RichProjection(Dataset("a", "boo", "seg"), schema) }
    }

    it("should get RichProjection back with proper dataset and schema") {
      val resp = RichProjection(dataset, schema)
      resp.datasetName should equal (dataset.name)
      resp.columns should equal (schema)
      resp.rowKeyColumns should equal (Seq(schema(2)))
      resp.rowKeyColIndices should equal (Seq(2))
      resp.rowKeyType should equal (LongKeyType)
      resp.segmentColumn should equal (schema(3))
      resp.segmentColIndex should equal (3)
      resp.segmentType should equal (IntKeyType)
      names.take(3).map(TupleRowReader).map(resp.rowKeyFunc) should equal (Seq(24L, 28L, 25L))
    }

    it("should get RichProjection back with multiple partition and row key columns") (pending)

    it("should create RichProjection properly for String sort key column") {
      val resp = RichProjection(Dataset("a", "first", "seg"), schema)
      resp.rowKeyColumns should equal (Seq(schema(0)))
      resp.rowKeyColIndices should equal (Seq(0))
      resp.columns should equal (schema)
      resp.rowKeyType should equal (StringKeyType)
      names.take(3).map(TupleRowReader).map(resp.rowKeyFunc) should equal (
                     Seq("Khalil", "Ndamukong", "Rodney"))
    }
  }
}