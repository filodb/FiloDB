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
    }
  }

}
