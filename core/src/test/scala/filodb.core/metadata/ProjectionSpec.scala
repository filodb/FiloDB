package filodb.core.metadata

import org.velvia.filo.TupleRowReader
import scala.language.existentials

import filodb.core._
import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}

class ProjectionSpec extends FunSpec with Matchers {
  import NamesTestData._
  import RichProjection._
  import ComputedKeyTypes._
  import SingleKeyTypes._

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

    it("should get BadSchema if segment key is not supported type") {
      val schema2 = schema ++ Seq(DataColumn(99, "bool", "a", 0, Column.ColumnType.BitmapColumn))
      val resp = RichProjection.make(Dataset("a", "age", "bool"), schema2)
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

    it("should get BadSchema if key columns or partition columns are empty") {
      val emptyKeyColsDataset = Dataset("b", Seq(), "seg", Seq(":string 0"))
      val resp1 = RichProjection.make(emptyKeyColsDataset, schema)
      resp1.isFailure should be (true)
      resp1.recover {
        case BadSchema(reason) => reason should include ("cannot be empty")
      }

      val emptyPartDataset = Dataset("c", Seq("age"), "seg", Nil)
      val resp2 = RichProjection.make(emptyPartDataset, schema)
      resp2.isFailure should be (true)
      resp2.recover {
        case BadSchema(reason) => reason should include ("cannot be empty")
      }
    }

    it("should get BadSchema if cannot find partitioning column") {
      val resp = RichProjection.make(dataset.copy(partitionColumns = Seq("boo")), schema)
      resp.isFailure should be (true)
      resp.recover {
        case BadSchema(reason) => reason should include ("Partition column boo not in columns")
      }
    }

    it("should get back NoSuchFunction if computed column function not found") {
      val resp = RichProjection.make(dataset.copy(partitionColumns = Seq(":notAFunc 1")), schema)
      resp.isFailure should be (true)
      resp.recover {
        case NoSuchFunction(func) => func should equal ("notAFunc")
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
      resp.columns.take(4) should equal (schema)
      resp.columns.last shouldBe a[ComputedColumn]
      resp.columns.last.name should equal (Dataset.DefaultPartitionColumn)
      resp.rowKeyColumns should equal (Seq(schema(2)))
      resp.rowKeyColIndices should equal (Seq(2))
      resp.rowKeyType should equal (LongKeyType)
      resp.segmentColumn should equal (schema(3))
      resp.segmentColIndex should equal (3)
      resp.segmentType should equal (IntKeyType)
      resp.partitionColIndices should equal (Seq(4))
      names.take(3).map(TupleRowReader).map(resp.rowKeyFunc) should equal (Seq(24L, 28L, 25L))
    }

    it("should get RichProjection back with multiple partition and row key columns") {
      val multiDataset = Dataset(dataset.name, Seq("seg", "age"), "seg", Seq("first", "last"))
      val resp = RichProjection(multiDataset, schema)
      resp.columns should equal (schema)
      resp.rowKeyColumns should equal (Seq(schema(3), schema(2)))
      resp.rowKeyColIndices should equal (Seq(3, 2))
      resp.rowKeyType shouldBe a[CompositeKeyType]
      resp.segmentColumn should equal (schema(3))
      resp.segmentType should equal (IntKeyType)
      resp.partitionColIndices should equal (Seq(0, 1))
      resp.partitionType shouldBe a[CompositeKeyType]
    }

    it("should handle ComputedColumn in segment key") {
      val resp = RichProjection(Dataset("foo", "age", ":string 1"), schema)
      resp.columns should have length (schema.length + 2)    // extra 2 computed columns: partition and seg
      resp.segmentColumn shouldBe a[ComputedColumn]
      resp.segmentColIndex should equal (5)
      resp.segmentType shouldBe a[ComputedStringKeyType]
    }

    it("should create RichProjection properly for String sort key column") {
      val resp = RichProjection(Dataset("a", "first", "seg"), schema)
      resp.rowKeyColumns should equal (Seq(schema(0)))
      resp.rowKeyColIndices should equal (Seq(0))
      resp.columns.take(4) should equal (schema)
      resp.rowKeyType should equal (StringKeyType)
      names.take(3).map(TupleRowReader).map(resp.rowKeyFunc) should equal (
                     Seq("Khalil", "Ndamukong", "Rodney"))
    }
  }
}