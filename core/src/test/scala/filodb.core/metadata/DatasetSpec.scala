package filodb.core.metadata

import org.velvia.filo.TupleRowReader

import filodb.core.NamesTestData

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}

class DatasetSpec extends FunSpec with Matchers {
  import NamesTestData._
  import Dataset._

  describe("DatasetOptions serialization") {
    it("should serialize options successfully") {
      val options = Dataset.DefaultOptions.copy(chunkSize = 1000)
      DatasetOptions.fromString(options.toString) should equal (options)
    }
  }
}