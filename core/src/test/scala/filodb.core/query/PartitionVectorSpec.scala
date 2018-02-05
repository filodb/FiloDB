package filodb.core.query

import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.core.{MachineMetricsData, TestData}

class PartitionVectorSpec extends FunSpec with Matchers with ScalaFutures {
  import MachineMetricsData._
  import monix.execution.Scheduler.Implicits.global

  // 2 chunks of 10 samples each, (100000-109000), (110000-119000)
  val data = records(linearMultiSeries().take(20))
  val chunksets = TestData.toChunkSetStream(dataset1, defaultPartKey, data.map(_.data)).toListL
  val readers = chunksets.runAsync.futureValue.map(ChunkSetReader(_, dataset1, 0 to 2))
  val partVector = PartitionVector(Some(PartitionInfo(defaultPartKey, 0)), readers)
  val ordering = dataset1.rowKeyOrdering

  describe("rangedIterator") {
    it("should return empty iterator if key1 > key2") {
      val it1 = partVector.rangedIterator(dataset1.rowKey(110000L), dataset1.rowKey(105000L), ordering)
      it1.toSeq.length shouldEqual 0
    }

    it("should return empty iterators if keyrange outside all chunks") {
      // range completely outside both chunksets: keys too new
      val it1 = partVector.rangedIterator(dataset1.rowKey(120000L), dataset1.rowKey(125000L), ordering)
      it1.toSeq.length shouldEqual 0

      // range completely outside both chunksets: keys too old
      val it2 = partVector.rangedIterator(dataset1.rowKey(90000L), dataset1.rowKey(99999L), ordering)
      it2.toSeq.length shouldEqual 0
    }

    // List(100000, 1.0, 20.0, 100.0, 85.0, Series 0)
    // List(101000, 2.0, 21.0, 101.0, 86.0, Series 1)
    // List(102000, 3.0, 22.0, 102.0, 87.0, Series 2)
    // List(103000, 4.0, 23.0, 103.0, 88.0, Series 3)
    // List(104000, 5.0, 24.0, 104.0, 89.0, Series 4)
    // List(105000, 6.0, 25.0, 105.0, 90.0, Series 5)
    // List(106000, 7.0, 26.0, 106.0, 91.0, Series 6)
    // List(107000, 8.0, 27.0, 107.0, 92.0, Series 7)
    // List(108000, 9.0, 28.0, 108.0, 93.0, Series 8)
    // List(109000, 10.0, 29.0, 109.0, 94.0, Series 9)
    // List(110000, 11.0, 30.0, 110.0, 95.0, Series 0)
    // List(111000, 12.0, 31.0, 111.0, 96.0, Series 1)
    // List(112000, 13.0, 32.0, 112.0, 97.0, Series 2)
    // List(113000, 14.0, 33.0, 113.0, 98.0, Series 3)
    // List(114000, 15.0, 34.0, 114.0, 99.0, Series 4)
    // List(115000, 16.0, 35.0, 115.0, 100.0, Series 5)
    // List(116000, 17.0, 36.0, 116.0, 101.0, Series 6)
    // List(117000, 18.0, 37.0, 117.0, 102.0, Series 7)
    // List(118000, 19.0, 38.0, 118.0, 103.0, Series 8)
    // List(119000, 20.0, 39.0, 119.0, 104.0, Series 9)

    it("should return proper iterators if keyrange overlaps at least one chunk - exact key match") {
      // range overlaps chunk2 partially
      val it1 = partVector.rangedIterator(dataset1.rowKey(117000L), dataset1.rowKey(125000L), ordering)
      it1.map(_.getDouble(1)).toSeq shouldEqual Seq(18.0, 19.0, 20.0)

      // range overlaps both chunk1 and chunk2
      val it2 = partVector.rangedIterator(dataset1.rowKey(106000L), dataset1.rowKey(111000L), ordering)
      it2.map(_.getDouble(1)).toSeq shouldEqual (7 to 12).map(_.toDouble)

      // range overlaps chunk1 partially only
      val it3 = partVector.rangedIterator(dataset1.rowKey(99000L), dataset1.rowKey(102000L), ordering)
      it3.map(_.getDouble(1)).toSeq shouldEqual (1 to 3).map(_.toDouble)

      // range touches only last/first rows in both chunks
      val it4 = partVector.rangedIterator(dataset1.rowKey(109000L), dataset1.rowKey(110000L), ordering)
      it4.map(_.getDouble(1)).toSeq shouldEqual (10 to 11).map(_.toDouble)
    }

    it("should return proper iterators if keyrange overlaps but not exact match") {
      // range overlaps chunk2 partially
      val it1 = partVector.rangedIterator(dataset1.rowKey(116999L), dataset1.rowKey(125000L), ordering)
      it1.map(_.getDouble(1)).toSeq shouldEqual Seq(18.0, 19.0, 20.0)

      // range overlaps both chunk1 and chunk2
      val it2 = partVector.rangedIterator(dataset1.rowKey(106999L), dataset1.rowKey(111001L), ordering)
      it2.map(_.getDouble(1)).toSeq shouldEqual (8 to 12).map(_.toDouble)

      // range overlaps chunk1 partially only
      val it3 = partVector.rangedIterator(dataset1.rowKey(99000L), dataset1.rowKey(102008L), ordering)
      it3.map(_.getDouble(1)).toSeq shouldEqual (1 to 3).map(_.toDouble)

      // range touches only first rows in chunk2
      val it4 = partVector.rangedIterator(dataset1.rowKey(109001L), dataset1.rowKey(110001L), ordering)
      it4.map(_.getDouble(1)).toSeq shouldEqual Seq(11.0)

      // range is only in one chunk entirely
      val it5 = partVector.rangedIterator(dataset1.rowKey(105001L), dataset1.rowKey(107000L), ordering)
      it5.map(_.getDouble(1)).toSeq shouldEqual Seq(7.0, 8.0)
    }
  }
}