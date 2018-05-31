package filodb.core.memstore

import com.googlecode.javaewah.{EWAHCompressedBitmap, IntIterator}
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}

import filodb.core._
import filodb.core.query.{ColumnFilter, Filter}
import filodb.memory.BinaryRegionConsumer

class PartitionKeyIndexSpec extends FunSpec with Matchers with BeforeAndAfter {
  import GdeltTestData._
  import Filter._
  import filodb.memory.format.UTF8Wrapper
  import filodb.memory.format.ZeroCopyUTF8String._

  val keyIndex = new PartitionKeyIndex(dataset6)

  before {
    keyIndex.reset()
  }

  implicit class RichIntIterator(ii: IntIterator) {
    def toSeq: Seq[Int] = {
      val newSeq = new collection.mutable.ArrayBuffer[Int]()
      while (ii.hasNext) { newSeq += ii.next }
      newSeq
    }
  }

  it("should add keys and parse filters correctly") {
    // Add the first ten keys and row numbers
    records(dataset6, readers.take(10)).records.consumeRecords(new BinaryRegionConsumer {
      var i = 0
      def onNext(base: Any, offset: Long): Unit = { keyIndex.addRecord(base, offset, i); i += 1 }
    })

    // Should get empty iterator when passing no filters
    val (partNums1, unFounded1) = keyIndex.parseFilters(Nil)
    partNums1.toSeq should have length (0)
    unFounded1 should have length (0)

    val filter2 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
    val (partNums2, unFounded2) = keyIndex.parseFilters(Seq(filter2))
    partNums2.toSeq should equal (Seq(7, 8, 9))
    unFounded2.toSeq should equal (Nil)

    val filter3 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val (partNums3, unFounded3) = keyIndex.parseFilters(Seq(filter3))
    partNums3.toSeq should equal (Seq(8, 9))
    unFounded3.toSeq should equal (Nil)
  }

  it("should parse filters with UTF8Wrapper and string correctly") {
    // Add the first ten keys and row numbers
    records(dataset6, readers.take(10)).records.consumeRecords(new BinaryRegionConsumer {
      var i = 0
      def onNext(base: Any, offset: Long): Unit = { keyIndex.addRecord(base, offset, i); i += 1 }
    })

    val filter2 = ColumnFilter("Actor2Name", Equals(UTF8Wrapper("REGIME".utf8)))
    val (partNums2, unFounded2) = keyIndex.parseFilters(Seq(filter2))
    partNums2.toSeq should equal (Seq(8, 9))
    unFounded2.toSeq should equal (Nil)

    val filter3 = ColumnFilter("Actor2Name", Equals("REGIME"))
    val (partNums3, unFounded3) = keyIndex.parseFilters(Seq(filter3))
    partNums3.toSeq should equal (Seq(8, 9))
    unFounded3.toSeq should equal (Nil)
  }

  it("should obtain indexed names and values") {
    // Add the first ten keys and row numbers
    records(dataset6, readers.take(10)).records.consumeRecords(new BinaryRegionConsumer {
      var i = 0
      def onNext(base: Any, offset: Long): Unit = { keyIndex.addRecord(base, offset, i); i += 1 }
    })

    keyIndex.indexNames.toSet should equal (Set("Actor2Code", "Actor2Name"))
    keyIndex.indexValues("not_found").toSeq should equal (Nil)

    val codes = Seq("AFR", "AGR", "CHN", "COP", "CVL", "EGYEDU", "GOV").map(_.utf8)
    keyIndex.indexValues("Actor2Code").toSeq should equal (codes)
  }

  it("should be able to AND multiple filters together") {
    // Add the first ten keys and row numbers
    records(dataset6, readers.take(10)).records.consumeRecords(new BinaryRegionConsumer {
      var i = 0
      def onNext(base: Any, offset: Long): Unit = { keyIndex.addRecord(base, offset, i); i += 1 }
    })

    val filters1 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)),
                       ColumnFilter("Actor2Name", Equals("REGIME".utf8)))
    val (partNums1, unFounded1) = keyIndex.parseFilters(filters1)
    partNums1.toSeq should equal (Seq(8, 9))
    unFounded1.toSeq should equal (Nil)

    val filters2 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)),
                       ColumnFilter("Actor2Name", Equals("CHINA".utf8)))
    val (partNums2, unFounded2) = keyIndex.parseFilters(filters2)
    partNums2.toSeq should equal (Nil)
    unFounded2.toSeq should equal (Nil)
  }

  it("should return unfound column names when calling parseFilters") {
    // Add the first ten keys and row numbers
    records(dataset6, readers.take(10)).records.consumeRecords(new BinaryRegionConsumer {
      var i = 0
      def onNext(base: Any, offset: Long): Unit = { keyIndex.addRecord(base, offset, i); i += 1 }
    })

    val filters1 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)),
                       ColumnFilter("MyName", Equals("REGIME".utf8)))
    val (partNums1, unFounded1) = keyIndex.parseFilters(filters1)
    partNums1.toSeq should equal (Seq(7, 8, 9))
    unFounded1.toSeq should equal (Seq("MyName"))
  }

  it("should ignore unsupported columns and return empty filter") {
    val index2 = new PartitionKeyIndex(dataset1)
    records(dataset1, readers.take(10)).records.consumeRecords(new BinaryRegionConsumer {
      var i = 0
      def onNext(base: Any, offset: Long): Unit = { index2.addRecord(base, offset, i); i += 1 }
    })

    val filters1 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)),
                       ColumnFilter("Year", Equals(1979)))
    val (partNums1, unFounded1) = index2.parseFilters(filters1)
    partNums1.toSeq should equal (Seq(7, 8, 9))
    unFounded1.toSeq should equal (Seq("Year"))
  }

  it("should remove entries correctly") {
    records(dataset6, readers.take(10)).records.consumeRecords(new BinaryRegionConsumer {
      var i = 0
      def onNext(base: Any, offset: Long): Unit = { keyIndex.addRecord(base, offset, i); i += 1 }
    })

    keyIndex.indexSize shouldEqual 15

    val entries = EWAHCompressedBitmap.bitmapOf(2, 3)
    keyIndex.removeEntries("Actor2Code".utf8, Seq("AGR".utf8, "CHN".utf8), entries)
    keyIndex.indexSize shouldEqual 14   // CHN entry removed, but not AGR

    val filters2 = Seq(ColumnFilter("Actor2Code", Equals("AGR".utf8)))
    val (partNums2, unFounded2) = keyIndex.parseFilters(filters2)
    partNums2.toSeq shouldEqual Seq(1)
    unFounded2.toSeq shouldEqual Nil
  }
}