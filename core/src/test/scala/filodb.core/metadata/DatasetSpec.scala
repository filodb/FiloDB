package filodb.core.metadata

import org.scalatest.{FunSpec, Matchers}

// DEPRECATED: remove soon
class DatasetSpec extends FunSpec with Matchers {
  describe("Dataset validation") {
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