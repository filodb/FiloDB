package filodb.core.metadata

import filodb.core.Setup
import filodb.core.store.{Dataset, DatasetOptions}
import org.scalatest.{FunSpec, Matchers}
import org.velvia.filo.TupleRowReader

class DatasetSpec extends FunSpec with Matchers {
  import Dataset._
  import Setup._

  describe("DatasetOptions serialization") {
    it("should serialize options successfully") {
      val options = DatasetOptions(chunkSize = 1000, segmentSize = "10000")
      DatasetOptions.fromString(options.toString) should equal (options)

      val options2 = options.copy(defaultPartitionKey = Some("<none>"))
      DatasetOptions.fromString(options2.toString) should equal (options2)
    }
  }

  describe("getPartitioningFunc") {
    it("should get BadPartitionColumn if cannot find partitioning column") {
      val resp = getPartitioningFunc(dataset.copy(partitionColumn = "boo"), schema)
      resp.isFailure should be (true)
      resp.recover {
        case BadPartitionColumn(reason) => reason should include ("Column boo not in schema")
      }
    }

    it("should get back partitioning func for default key if partitioning column is default") {
      val resp = getPartitioningFunc(dataset, schema)
      resp.isSuccess should be (true)
      val partFunc = resp.get

      partFunc(names.map(TupleRowReader).head) should equal (DefaultPartitionKey)
    }
  }
}
