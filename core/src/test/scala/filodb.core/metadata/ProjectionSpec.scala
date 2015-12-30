package filodb.core.metadata

import filodb.core.Setup
import filodb.core.store.Dataset
import filodb.core.store.MetaStore.BadSchema
import org.scalatest.{FunSpec, Matchers}
import org.velvia.filo.TupleRowReader

class ProjectionSpec extends FunSpec with Matchers {

  import Setup._

  describe("Projection") {
    it("should get BadSchema for invalid columns") {
      val resp = intercept[BadSchema] {
        Dataset("a", schema, "boo", "far","car")
      }
      resp.reason should include("Invalid column")
    }

    it("should get Projection back with proper dataset and schema") {
      val resp = Dataset("a", schema, "first", "age", "age").projections(0)
      val columnIndexes = resp.schema.zipWithIndex.map { case (col, i) => col.name -> i }.toMap
      resp.schema should equal(schema)
      resp.partitionType shouldBe a[filodb.core.StringKeyType]
      resp.keyType shouldBe a[filodb.core.LongKeyType]
      resp.segmentType shouldBe a[filodb.core.LongKeyType]
      names.take(3).map(TupleRowReader)
        .map(resp.segmentFunction(columnIndexes)) should equal(Seq(24L, 28L, 25L))
    }

  }
}
