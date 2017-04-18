package filodb.core.query

import com.googlecode.javaewah.IntIterator

import filodb.core._

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}

class PartitionKeyIndexSpec extends FunSpec with Matchers with BeforeAndAfter {
  import GdeltTestData._
  import Filter._
  import org.velvia.filo.ZeroCopyUTF8String._

  val keyIndex = new PartitionKeyIndex(projection6)

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
    // Add the first nine keys and row numbers
    readers.zipWithIndex.take(10).foreach { case (row, index) =>
      keyIndex.addKey(projection6.partitionKeyFunc(row), index)
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

  it("should be able to AND multiple filters together") {
    readers.zipWithIndex.take(10).foreach { case (row, index) =>
      keyIndex.addKey(projection6.partitionKeyFunc(row), index)
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
    readers.zipWithIndex.take(10).foreach { case (row, index) =>
      keyIndex.addKey(projection6.partitionKeyFunc(row), index)
    }

    val filters1 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)),
                       ColumnFilter("MyName", Equals("REGIME".utf8)))
    val (partNums1, unFounded1) = keyIndex.parseFilters(filters1)
    partNums1.toSeq should equal (Seq(7, 8, 9))
    unFounded1.toSeq should equal (Seq("MyName"))
  }
}