package filodb.core.reprojector

import filodb.core.Setup._
import filodb.core.store.Dataset
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.velvia.filo.TupleRowReader


class ReprojectorSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {

  describe("Rows to Flush") {
    it("should correctly project rows to flush") {
      val rows = names2.map(TupleRowReader)
      val flushes = Reprojector.project(Dataset.DefaultPartitionKey, projection, rows)
      flushes.length should be(2)
      val classes = projection.columns.map(_.columnType.clazz).toArray
      val reader1 = readerFactory(flushes.head.columnVectors, classes)

      //there should be six rows
      (0 until 3).foreach { i =>
        reader1.rowNo = i
        reader1.getString(0) should not be null
        if (i == 0) {
          reader1.getString(0) should be("Rodney")
          reader1.getLong(2) should be(40)
        }
      }
      val reader2 = readerFactory(flushes.last.columnVectors, classes)
      (0 until 3).foreach { i =>
        reader2.rowNo = i
        if (i == 2) {
          reader2.getString(0) should be("Peyton")
          reader2.getLong(2) should be(24)
        }
      }
    }
  }
}
