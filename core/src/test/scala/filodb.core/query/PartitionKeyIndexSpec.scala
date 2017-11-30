package filodb.core.query

import com.googlecode.javaewah.IntIterator
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}

import filodb.core._
import filodb.core.memstore.IngestRecord

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
    records(dataset6, readers.take(10)).foreach { case IngestRecord(partReader, data, index) =>
      keyIndex.addKey(dataset6.partKey(partReader), index.toInt)
    }

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
    records(dataset6, readers.take(10)).foreach { case IngestRecord(partReader, data, index) =>
      keyIndex.addKey(dataset6.partKey(partReader), index.toInt)
    }

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
    records(dataset6, readers.take(10)).foreach { case IngestRecord(partReader, data, index) =>
      keyIndex.addKey(dataset6.partKey(partReader), index.toInt)
    }

    keyIndex.indexNames.toSet should equal (Set("Actor2Code", "Actor2Name"))
    keyIndex.indexValues("not_found").toSeq should equal (Nil)

    val codes = Seq("AFR", "AGR", "CHN", "COP", "CVL", "EGYEDU", "GOV").map(_.utf8)
    keyIndex.indexValues("Actor2Code").toSeq should equal (codes)
  }

  it("should be able to AND multiple filters together") {
    // Add the first ten keys and row numbers
    records(dataset6, readers.take(10)).foreach { case IngestRecord(partReader, data, index) =>
      keyIndex.addKey(dataset6.partKey(partReader), index.toInt)
    }

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
    records(dataset6, readers.take(10)).foreach { case IngestRecord(partReader, data, index) =>
      keyIndex.addKey(dataset6.partKey(partReader), index.toInt)
    }

    val filters1 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)),
                       ColumnFilter("MyName", Equals("REGIME".utf8)))
    val (partNums1, unFounded1) = keyIndex.parseFilters(filters1)
    partNums1.toSeq should equal (Seq(7, 8, 9))
    unFounded1.toSeq should equal (Seq("MyName"))
  }

  it("should ignore unsupported columns and return empty filter") {
    val index2 = new PartitionKeyIndex(dataset1)
    records(dataset1, readers.take(10)).foreach { case IngestRecord(partReader, data, index) =>
      index2.addKey(dataset1.partKey(partReader), index.toInt)
    }

    val filters1 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)),
                       ColumnFilter("Year", Equals(1979)))
    val (partNums1, unFounded1) = index2.parseFilters(filters1)
    partNums1.toSeq should equal (Seq(7, 8, 9))
    unFounded1.toSeq should equal (Seq("Year"))
  }
}