package filodb.core.metadata

import org.scalactic._
import org.velvia.filo.RowReader._
import org.velvia.filo.{TupleRowReader, ZeroCopyUTF8String}
import scala.language.existentials

import filodb.core._
import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}

class ProjectionSpec extends FunSpec with Matchers {
  import NamesTestData._
  import RichProjection._
  import SingleKeyTypes._

  describe("RichProjection") {
    it("should get MissingColumnNames if cannot find row key or segment key") {
      val resp = RichProjection.make(Dataset("a", "boo", "seg"), schema)
      resp.isBad should be (true)
      resp.swap.get should equal (MissingColumnNames(Seq("boo"), "row"))

      val resp2 = RichProjection.make(Dataset("a", "age", "bar"), schema)
      resp2.isBad should be (true)
      resp2.swap.get should equal (MissingColumnNames(Seq("bar"), "segment"))
    }

    it("should get MissingColumnNames if projection columns are missing from schema") {
      val missingColProj = Projection(0, DatasetRef("a"), Seq("age"), "seg", columns = Seq("first", "yards"))
      val missingColDataset = Dataset("a", Seq(missingColProj), Seq(Dataset.DefaultPartitionColumn))
      val resp = RichProjection.make(missingColDataset, schema)
      resp.isBad should be (true)
      resp.swap.get should equal (MissingColumnNames(Seq("yards"), "projection"))
    }

    it("should get NoColumnsSpecified if key columns or partition columns are empty") {
      val emptyKeyColsDataset = Dataset("b", Seq(), "seg", Seq(":string 0"))
      val resp1 = RichProjection.make(emptyKeyColsDataset, schema)
      resp1.isBad should be (true)
      resp1.swap.get should equal (NoColumnsSpecified("row"))

      val emptyPartDataset = Dataset("c", Seq("age"), "seg", Nil)
      val resp2 = RichProjection.make(emptyPartDataset, schema)
      resp2.isBad should be (true)
      resp2.swap.get should equal (NoColumnsSpecified("partition"))
    }

    it("should get MissingColumnNames if cannot find partitioning column") {
      val resp = RichProjection.make(dataset.copy(partitionColumns = Seq("boo")), schema)
      resp.isBad should be (true)
      resp.swap.get should equal (MissingColumnNames(Seq("boo"), "partition"))
    }

    it("should get back NoSuchFunction if computed column function not found") {
      val resp = RichProjection.make(dataset.copy(partitionColumns = Seq(":notAFunc 1")), schema)
      resp.isBad should be (true)
      resp.swap.get should equal (ComputedColumnErrs(Seq(NoSuchFunction("notAFunc"))))
    }

    it("should return RowKeyComputedColumns err if try to use computed columns in row key") {
      val computedRowKeys = Dataset("b", Seq(":round age 2"), "seg", Seq(":string 0"))
      val resp1 = RichProjection.make(computedRowKeys, schema)
      resp1.isBad should be (true)
      resp1.swap.get should equal (RowKeyComputedColumns(Seq(":round age 2")))
    }

    it("should get back partitioning func for default key if partitioning column is default") {
      val resp = RichProjection.make(dataset, schema)
      resp.isGood should be (true)
      val partFunc = resp.get.partitionKeyFunc

      partFunc(names.map(TupleRowReader).head) should equal (defaultPartKey)
    }

    it("should change database with withDatabase") {
      val prj = RichProjection(dataset, schema)
      prj.datasetRef.database should equal (None)

      prj.withDatabase("db2").datasetRef.database should equal (Some("db2"))
    }

    it("apply() should throw exception for bad schema") {
      intercept[BadSchemaError] { RichProjection(Dataset("a", "boo", "seg"), schema) }
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
      resp.partitionColumns should equal (schema take 2)
      resp.partExtractors.toSeq should equal (Seq(UTF8StringFieldExtractor, UTF8StringFieldExtractor))
    }

    it("should create RichProjection properly for String row key column") {
      val resp = RichProjection(Dataset("a", "first", "seg"), schema)
      resp.rowKeyColumns should equal (Seq(schema(0)))
      resp.rowKeyColIndices should equal (Seq(0))
      resp.columns.take(4) should equal (schema)
      resp.rowKeyType should equal (StringKeyType)
      names.take(3).map(TupleRowReader).map(resp.rowKeyFunc) should equal (
                     Seq("Khalil", "Ndamukong", "Rodney").map(ZeroCopyUTF8String.apply))
    }

    it("should (de)serialize to/from readOnlyProjectionStrings") {
      val multiDataset = Dataset("a", Seq("age"), ":string 0", Seq("first", ":getOrElse last --"))
      val proj = RichProjection(multiDataset, schema)
      val serialized = proj.toReadOnlyProjString(Seq("first", "age", "last"))
      val readOnlyProj = RichProjection.readOnlyFromString(serialized)

      readOnlyProj.datasetName should equal (proj.datasetName)
      readOnlyProj.segmentType should equal (StringKeyType)
      readOnlyProj.columns.map(_.name) should equal (Seq("first", "age", "last"))
      readOnlyProj.partitionColumns.head should equal (schema(0).copy(dataset = proj.datasetName))
      readOnlyProj.partExtractors.toSeq should equal (Seq(UTF8StringFieldExtractor, UTF8StringFieldExtractor))
      readOnlyProj.rowKeyType should equal (LongKeyType)
      readOnlyProj.rowKeyColumns.map(_.name) should equal (Seq("age"))
    }

    it("should deserialize readOnlyProjectionStrings with empty columns") {
      val multiDataset = Dataset("a", Seq("age"), ":string /0", Seq("first", ":getOrElse last --"))
      val proj = RichProjection(multiDataset, schema)
      val serialized = proj.toReadOnlyProjString(Nil)
      val readOnlyProj = RichProjection.readOnlyFromString(serialized)

      readOnlyProj.datasetName should equal (proj.datasetName)
      readOnlyProj.segmentType should equal (StringKeyType)
      readOnlyProj.columns should equal (Nil)
    }

    it("should deserialize readOnlyProjectionStrings with database specified") {
      val ref = DatasetRef("a", Some("db2"))
      val multiDataset = Dataset(ref, Seq("age"), ":string /0", Seq("first", ":getOrElse last --"))
      val proj = RichProjection(multiDataset, schema)
      val serialized = proj.toReadOnlyProjString(Nil)
      val readOnlyProj = RichProjection.readOnlyFromString(serialized)

      readOnlyProj.datasetRef should equal (ref)
    }
  }
}