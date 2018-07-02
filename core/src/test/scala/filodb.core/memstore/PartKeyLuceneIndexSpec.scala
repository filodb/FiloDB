package filodb.core.memstore

import com.googlecode.javaewah.IntIterator
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}

import filodb.core._
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.query.{ColumnFilter, Filter}

class PartKeyLuceneIndexSpec extends FunSpec with Matchers with BeforeAndAfter {
  import GdeltTestData._
  import Filter._
  import filodb.memory.format.UnsafeUtils.ZeroPointer
  import filodb.memory.format.UTF8Wrapper
  import filodb.memory.format.ZeroCopyUTF8String._

  val keyIndex = new PartKeyLuceneIndex(dataset6, TestData.storeConf)
  val partBuilder = new RecordBuilder(TestData.nativeMem, dataset6.partKeySchema)

  before {
    keyIndex.reset()
    keyIndex.commitBlocking()
  }

  after {
    partBuilder.removeAndFreeContainers(partBuilder.allContainers.length)
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
    partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.foreach { case (addr, i) => keyIndex.addPartKey(ZeroPointer, addr, i) }

    keyIndex.commitBlocking()

    // Should get empty iterator when passing no filters
    val partNums1 = keyIndex.parseFilters(Nil)
    partNums1.toSeq should have length (0)

    val filter2 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
    val partNums2 = keyIndex.parseFilters(Seq(filter2))
    partNums2.toSeq should equal (Seq(7, 8, 9))

    val filter3 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums3 = keyIndex.parseFilters(Seq(filter3))
    partNums3.toSeq should equal (Seq(8, 9))
  }

  it("should parse filters with UTF8Wrapper and string correctly") {
    // Add the first ten keys and row numbers
    partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.foreach { case (addr, i) => keyIndex.addPartKey(ZeroPointer, addr, i) }

    keyIndex.commitBlocking()

    val filter2 = ColumnFilter("Actor2Name", Equals(UTF8Wrapper("REGIME".utf8)))
    val partNums2 = keyIndex.parseFilters(Seq(filter2))
    partNums2.toSeq should equal (Seq(8, 9))

    val filter3 = ColumnFilter("Actor2Name", Equals("REGIME"))
    val partNums3 = keyIndex.parseFilters(Seq(filter3))
    partNums3.toSeq should equal (Seq(8, 9))
  }

  it("should obtain indexed names and values") {
    // Add the first ten keys and row numbers
    partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.foreach { case (addr, i) => keyIndex.addPartKey(ZeroPointer, addr, i) }

    keyIndex.commitBlocking()

    keyIndex.indexNames.toList shouldEqual Seq("Actor2Code", "Actor2Name")
    keyIndex.indexValues("not_found").toSeq should equal (Nil)

    val codes = Seq("AFR", "AGR", "CHN", "COP", "CVL", "EGYEDU", "GOV").map(_.utf8)
    keyIndex.indexValues("Actor2Code").toSeq should equal (codes)
  }

  it("should be able to AND multiple filters together") {
    // Add the first ten keys and row numbers
    partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.foreach { case (addr, i) => keyIndex.addPartKey(ZeroPointer, addr, i) }

    keyIndex.commitBlocking()

    val filters1 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)),
      ColumnFilter("Actor2Name", Equals("REGIME".utf8)))
    val partNums1 = keyIndex.parseFilters(filters1)
    partNums1.toSeq should equal (Seq(8, 9))

    val filters2 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)),
      ColumnFilter("Actor2Name", Equals("CHINA".utf8)))
    val partNums2 = keyIndex.parseFilters(filters2)
    partNums2.toSeq should equal (Nil)
  }

  it("should ignore unsupported columns and return empty filter") {
    val index2 = new PartKeyLuceneIndex(dataset1, TestData.storeConf)
    partKeyFromRecords(dataset1, records(dataset1, readers.take(10))).zipWithIndex.foreach { case (addr, i) =>
      index2.addPartKey(ZeroPointer, addr, i)
    }
    keyIndex.commitBlocking()

    val filters1 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)), ColumnFilter("Year", Equals(1979)))
    val partNums1 = index2.parseFilters(filters1)
    partNums1.toSeq shouldEqual Seq.empty
  }

}