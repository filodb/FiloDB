package filodb.core.memstore

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.googlecode.javaewah.IntIterator
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}

import filodb.core._
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Dataset
import filodb.core.query.{ColumnFilter, Filter}
import filodb.memory.format.UnsafeUtils.ZeroPointer
import filodb.memory.format.UTF8Wrapper
import filodb.memory.format.ZeroCopyUTF8String._

class PartKeyLuceneIndexSpec extends FunSpec with Matchers with BeforeAndAfter {
  import GdeltTestData._
  import Filter._

  val keyIndex = new PartKeyLuceneIndex(dataset6.ref, dataset6.schema.partition, 0, TestData.storeConf)
  val partBuilder = new RecordBuilder(TestData.nativeMem)

  def partKeyOnHeap(dataset: Dataset,
                   base: Any,
                   offset: Long): Array[Byte] = dataset.partKeySchema.asByteArray(base, offset)

  before {
    keyIndex.reset()
    keyIndex.refreshReadersBlocking()
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

  it("should add part keys and parse filters correctly") {
    val start = System.currentTimeMillis()
    // Add the first ten keys and row numbers
    partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.foreach { case (addr, i) =>
      keyIndex.addPartKey(partKeyOnHeap(dataset6, ZeroPointer, addr), i, System.currentTimeMillis())()
    }
    val end = System.currentTimeMillis()
    keyIndex.refreshReadersBlocking()

    // Should get empty iterator when passing no filters
    val partNums1 = keyIndex.partIdsFromFilters(Nil, start, end)
    partNums1.toSeq shouldEqual Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

    val filter2 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
    val partNums2 = keyIndex.partIdsFromFilters(Seq(filter2),start, end)
    partNums2.toSeq shouldEqual Seq(7, 8, 9)

    val filter3 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums3 = keyIndex.partIdsFromFilters(Seq(filter3),start, end)
    partNums3.toSeq shouldEqual Seq(8, 9)

    val filter4 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums4 = keyIndex.partIdsFromFilters(Seq(filter4),10, start-1)
    partNums4.toSeq shouldEqual Seq.empty

    val filter5 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums5 = keyIndex.partIdsFromFilters(Seq(filter5),end + 100, end + 100000)
    partNums5.toSeq should not equal (Seq.empty)

    val filter6 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums6 = keyIndex.partIdsFromFilters(Seq(filter6),start - 10000, end )
    partNums6.toSeq should not equal (Seq.empty)

    val filter7 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums7 = keyIndex.partIdsFromFilters(Seq(filter7),(start + end)/2, end + 1000 )
    partNums7.toSeq should not equal (Seq.empty)

  }

  it("should upsert part keys with endtime and foreachPartKeyStillIngesting should work") {
    // Add the first ten keys and row numbers
    partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.foreach { case (addr, i) =>
        val time = System.currentTimeMillis()
        keyIndex.addPartKey(partKeyOnHeap(dataset6, ZeroPointer, addr), i, time)()
        if (i%2 == 0) keyIndex.upsertPartKey(partKeyOnHeap(dataset6, ZeroPointer, addr), i, time, time + 300)()
    }
    keyIndex.refreshReadersBlocking()

    keyIndex.indexNumEntries shouldEqual 10

    val partIdsIngesting = ArrayBuffer[Int]()
    val numHits = keyIndex.foreachPartKeyStillIngesting { (id, key) =>
      partIdsIngesting += id
    }
    numHits shouldEqual 5
    partIdsIngesting shouldEqual Seq(1, 3, 5, 7, 9)

  }

  it("should add part keys and fetch startTimes correctly for more than 1024 keys") {
    val numPartIds = 3000 // needs to be more than 1024 to test the lucene term limit
    val start = System.currentTimeMillis()
    // we dont care much about the partKey here, but the startTime against partId.
    val partKeys = Stream.continually(readers.head).take(numPartIds).toList
    partKeyFromRecords(dataset6, records(dataset6, partKeys), Some(partBuilder))
      .zipWithIndex.foreach { case (addr, i) =>
      keyIndex.addPartKey(partKeyOnHeap(dataset6, ZeroPointer, addr), i, start + i)()
    }
    keyIndex.refreshReadersBlocking()

    val startTimes = keyIndex.startTimeFromPartIds((0 until numPartIds).iterator)
    for { i <- 0 until numPartIds} {
      startTimes(i) shouldEqual start + i
    }
  }

  it("should add part keys and fetch partIdsEndedBefore and removePartKeys correctly for more than 1024 keys") {
    val numPartIds = 3000 // needs to be more than 1024 to test the lucene term limit
    val start = 1000
    // we dont care much about the partKey here, but the startTime against partId.
    val partKeys = Stream.continually(readers.head).take(numPartIds).toList
    partKeyFromRecords(dataset6, records(dataset6, partKeys), Some(partBuilder))
      .zipWithIndex.foreach { case (addr, i) =>
      keyIndex.addPartKey(partKeyOnHeap(dataset6, ZeroPointer, addr), i, start + i, start + i + 100)()
    }
    keyIndex.refreshReadersBlocking()

    val pIds = keyIndex.partIdsEndedBefore(start + 200)
    for { i <- 0 until numPartIds} {
      pIds.get(i) shouldEqual (if (i <= 100) true else false)
    }

    keyIndex.removePartKeys(pIds)
    keyIndex.refreshReadersBlocking()

    for { i <- 0 until numPartIds} {
      keyIndex.partKeyFromPartId(i).isDefined shouldEqual (if (i <= 100) false else true)
    }

  }

  it("should update part keys with endtime and parse filters correctly") {
    val start = System.currentTimeMillis()
    // Add the first ten keys and row numbers
    partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.foreach { case (addr, i) =>
      val time = System.currentTimeMillis()
      keyIndex.addPartKey(partKeyOnHeap(dataset6, ZeroPointer, addr), i, time)()
      keyIndex.refreshReadersBlocking() // updates need to be able to read startTime from index, so commit
      keyIndex.updatePartKeyWithEndTime(partKeyOnHeap(dataset6, ZeroPointer, addr), i, time + 10000)()
    }
    val end = System.currentTimeMillis()
    keyIndex.refreshReadersBlocking()

    // Should get empty iterator when passing no filters
    val partNums1 = keyIndex.partIdsFromFilters(Nil, start, end)
    partNums1.toSeq shouldEqual Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

    val filter2 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
    val partNums2 = keyIndex.partIdsFromFilters(Seq(filter2),start, end)
    partNums2.toSeq shouldEqual Seq(7, 8, 9)

    val filter3 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums3 = keyIndex.partIdsFromFilters(Seq(filter3), start, end)
    partNums3.toSeq shouldEqual Seq(8, 9)

    val filter4 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums4 = keyIndex.partIdsFromFilters(Seq(filter4),10, start-1)
    partNums4.toSeq shouldEqual Seq.empty

    val filter5 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums5 = keyIndex.partIdsFromFilters(Seq(filter5),end + 20000, end + 100000)
    partNums5.toSeq should equal (Seq.empty)

    val filter6 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums6 = keyIndex.partIdsFromFilters(Seq(filter6),start - 10000, end-1 )
    partNums6.toSeq should not equal (Seq.empty)

    val filter7 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums7 = keyIndex.partIdsFromFilters(Seq(filter7),(start + end)/2, end + 1000 )
    partNums7.toSeq should not equal (Seq.empty)
  }

  it("should parse filters with UTF8Wrapper and string correctly") {
    // Add the first ten keys and row numbers
    partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.foreach { case (addr, i) =>
      keyIndex.addPartKey(partKeyOnHeap(dataset6, ZeroPointer, addr), i, System.currentTimeMillis())()
    }

    keyIndex.refreshReadersBlocking()

    val filter2 = ColumnFilter("Actor2Name", Equals(UTF8Wrapper("REGIME".utf8)))
    val partNums2 = keyIndex.partIdsFromFilters(Seq(filter2), 0, Long.MaxValue)
    partNums2.toSeq shouldEqual Seq(8, 9)

    val filter3 = ColumnFilter("Actor2Name", Equals("REGIME"))
    val partNums3 = keyIndex.partIdsFromFilters(Seq(filter3), 0, Long.MaxValue)
    partNums3.toSeq shouldEqual Seq(8, 9)
  }

  it("should obtain indexed names and values") {
    // Add the first ten keys and row numbers
    partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.foreach { case (addr, i) =>
      keyIndex.addPartKey(partKeyOnHeap(dataset6, ZeroPointer, addr), i, System.currentTimeMillis())()
    }

    keyIndex.refreshReadersBlocking()

    keyIndex.indexNames(10).toList shouldEqual Seq("Actor2Code", "Actor2Name")
    keyIndex.indexValues("not_found").toSeq should equal (Nil)

    val infos = Seq("AFR", "CHN", "COP", "CVL", "EGYEDU").map(_.utf8).map(TermInfo(_, 1))
    val top2infos = Seq(TermInfo("GOV".utf8, 3), TermInfo("AGR".utf8, 2))
    // top 2 items by frequency
    keyIndex.indexValues("Actor2Code", 2) shouldEqual top2infos
    val allValues = keyIndex.indexValues("Actor2Code")
    allValues take 2 shouldEqual top2infos
    allValues.drop(2).toSet shouldEqual infos.toSet
  }

  it("should be able to AND multiple filters together") {
    // Add the first ten keys and row numbers
    partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.foreach { case (addr, i) =>
      keyIndex.addPartKey(partKeyOnHeap(dataset6, ZeroPointer, addr), i, System.currentTimeMillis())()
    }

    keyIndex.refreshReadersBlocking()

    val filters1 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)),
      ColumnFilter("Actor2Name", Equals("REGIME".utf8)))
    val partNums1 = keyIndex.partIdsFromFilters(filters1, 0, Long.MaxValue)
    partNums1.toSeq should equal (Seq(8, 9))

    val filters2 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)),
      ColumnFilter("Actor2Name", Equals("CHINA".utf8)))
    val partNums2 = keyIndex.partIdsFromFilters(filters2, 0, Long.MaxValue)
    partNums2.toSeq shouldEqual Nil
  }

  it("should ignore unsupported columns and return empty filter") {
    val index2 = new PartKeyLuceneIndex(dataset1.ref, dataset1.schema.partition, 0, TestData.storeConf)
    partKeyFromRecords(dataset1, records(dataset1, readers.take(10))).zipWithIndex.foreach { case (addr, i) =>
      index2.addPartKey(partKeyOnHeap(dataset6, ZeroPointer, addr), i, System.currentTimeMillis())()
    }
    keyIndex.refreshReadersBlocking()

    val filters1 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)), ColumnFilter("Year", Equals(1979)))
    val partNums1 = index2.partIdsFromFilters(filters1, 0, Long.MaxValue)
    partNums1.toSeq shouldEqual Seq.empty
  }

  it("should be able to fetch partKey from partId and partId from partKey") {
    // Add the first ten keys and row numbers
    partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.foreach { case (addr, i) =>
      val partKeyBytes = partKeyOnHeap(dataset6, ZeroPointer, addr)
      keyIndex.addPartKey(partKeyBytes, i, System.currentTimeMillis())()
      keyIndex.refreshReadersBlocking()
      keyIndex.partKeyFromPartId(i).get.bytes shouldEqual partKeyBytes
//      keyIndex.partIdFromPartKey(new BytesRef(partKeyBytes)) shouldEqual i
    }
  }

  it("should be able to sort results by endTime, startTime") {

    val addedKeys = partKeyFromRecords(dataset6, records(dataset6, readers.take(100)), Some(partBuilder))
      .zipWithIndex.map { case (addr, i) =>
        val start = Math.abs(Random.nextLong())
        keyIndex.addPartKey(partKeyOnHeap(dataset6, ZeroPointer, addr), i, start)()
        keyIndex.refreshReadersBlocking() // updates need to be able to read startTime from index, so commit
        val end = start + Random.nextInt()
        keyIndex.updatePartKeyWithEndTime(partKeyOnHeap(dataset6, ZeroPointer, addr), i, end)()
        (end, start, i)
      }
    keyIndex.refreshReadersBlocking()

    for {
      from <- 0 until 99 // for various from values
      limit <- 3 to 100-from // for various limit values
    } {
      val sortedKeys = addedKeys.sorted
      val dropFrom = sortedKeys.drop(from)
      val partNums3 = keyIndex.partIdsOrderedByEndTime(limit, fromEndTime = dropFrom(0)._1)
      partNums3.toArray.toSeq shouldEqual dropFrom.map(_._3).take(limit).sorted
      val untilTimePartIds = keyIndex.partIdsOrderedByEndTime(10, toEndTime = dropFrom(0)._1 - 1)
      untilTimePartIds.toArray.toSeq shouldEqual sortedKeys.take(from).map(_._3).take(10).sorted
    }
  }

}