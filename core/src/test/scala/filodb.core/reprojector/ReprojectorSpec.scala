package filodb.core.reprojector

import filodb.core.Setup._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.velvia.filo.TupleRowReader


class ReprojectorSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {

  describe("Rows to Flush") {
    it("should correctly project rows to flush") {
      val rows = names.map(TupleRowReader)
      val partitions = Reprojector.project(projection, rows).toSeq
      partitions.length should be(2)
      val classes = projection.schema.map(_.columnType.clazz).toArray
      val usFlushes = partitions.head._2
      usFlushes.length should be(2)

      val reader = readerFactory(usFlushes.head.columnVectors, classes)
      reader.rowNo = 0
      //part key should match
      reader.getString(0) should be("US")
      //segment should match
      reader.getString(1) should be("SF")
      reader.getString(2) should be("Khalil")
      reader.getLong(4) should be(24)

      val ukFlushes = partitions.last._2
      ukFlushes.length should be(1)
      val reader1 = readerFactory(ukFlushes.head.columnVectors, classes)

      //there should be six rows
      (0 until 3).foreach { i =>
        reader1.rowNo = i
        reader1.getString(0) should not be null
        if (i == 0) {
          //part key should be UK
          reader1.getString(0) should be("UK")
          //segment should be London
          reader1.getString(1) should be("LN")
          //sorted by age in UK segment
          reader1.getString(2) should be("Terrance")
          reader1.getLong(4) should be(29)
        }
      }

    }
  }

}
