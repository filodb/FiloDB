package filodb.core.metadata

import org.scalatest.{FunSpec, Matchers}

import filodb.core._
import filodb.core.query.ColumnInfo

// DEPRECATED: remove soon
class DatasetSpec extends FunSpec with Matchers {
  import Column.ColumnType._
  import Dataset._
  import NamesTestData._

  describe("Dataset validation") {
    it("should return IDs for column names or seq of missing names") {
      val ds = Dataset("dataset", Seq("part:string"), dataColSpecs, DatasetOptions.DefaultOptions)
      ds.colIDs("first", "age").get shouldEqual Seq(1, 0)

      ds.colIDs("part").get shouldEqual Seq(Dataset.PartColStartIndex)

      val resp1 = ds.colIDs("last", "unknown")
      resp1.isBad shouldEqual true
      resp1.swap.get shouldEqual Seq("unknown")
    }

    it("should return ColumnInfos for colIDs") {
      val ds = Dataset("dataset", Seq("part:string"), dataColSpecs, DatasetOptions.DefaultOptions)
      val infos = ds.infosFromIDs(Seq(1, 0))
      infos shouldEqual Seq(ColumnInfo("first", StringColumn), ColumnInfo("age", LongColumn))

      val infos2 = ds.infosFromIDs(Seq(PartColStartIndex, 2))
      infos2 shouldEqual Seq(ColumnInfo("part", StringColumn), ColumnInfo("last", StringColumn))
    }

    it("should compute nonMetricShardColumns correctly") {
      val options = DatasetOptions.DefaultOptions.copy(shardKeyColumns = Seq("job", "__name__"))
      options.nonMetricShardColumns shouldEqual Seq("job")
      options.nonMetricShardKeyBytes.size shouldEqual 1
    }
  }

  describe("DatasetOptions serialization") {
    it("should serialize options successfully") {
      val options = DatasetOptions.DefaultOptions.copy(shardKeyColumns = Seq("job", "__name__"))
      DatasetOptions.fromString(options.toString) should equal (options)
    }
  }
}