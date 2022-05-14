package filodb.core.memstore

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.util.Random

import com.googlecode.javaewah.IntIterator
import org.apache.lucene.util.BytesRef
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core._
import filodb.core.binaryrecord2.{RecordBuilder, RecordSchema}
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter}
import filodb.memory.format.UnsafeUtils.ZeroPointer
import filodb.memory.format.UTF8Wrapper
import filodb.memory.format.ZeroCopyUTF8String._

class PartKeyLuceneIndexSpec extends AnyFunSpec with Matchers with BeforeAndAfter {
  import Filter._
  import GdeltTestData._

  val keyIndex = new PartKeyLuceneIndex(dataset6.ref, dataset6.schema.partition, true, true,0, 1.hour.toMillis,
    Some(new java.io.File(System.getProperty("java.io.tmpdir"), "part-key-lucene-index")))
  val partBuilder = new RecordBuilder(TestData.nativeMem)

  def partKeyOnHeap(partKeySchema: RecordSchema,
                    base: Any,
                    offset: Long): Array[Byte] = partKeySchema.asByteArray(base, offset)

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
      keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, System.currentTimeMillis())()
    }
    val end = System.currentTimeMillis()
    keyIndex.refreshReadersBlocking()

    // Should get empty iterator when passing no filters
    val partNums1 = keyIndex.partIdsFromFilters(Nil, start, end)
    partNums1 shouldEqual debox.Buffer(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

    val filter2 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
    val partNums2 = keyIndex.partIdsFromFilters(Seq(filter2), start, end)
    partNums2 shouldEqual debox.Buffer(7, 8, 9)

    val filter3 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums3 = keyIndex.partIdsFromFilters(Seq(filter3), start, end)
    partNums3 shouldEqual debox.Buffer(8, 9)

    val filter4 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums4 = keyIndex.partIdsFromFilters(Seq(filter4), 10, start-1)
    partNums4 shouldEqual debox.Buffer.empty[Int]

    val filter5 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums5 = keyIndex.partIdsFromFilters(Seq(filter5), end + 100, end + 100000)
    partNums5 should not equal debox.Buffer.empty[Int]

    val filter6 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums6 = keyIndex.partIdsFromFilters(Seq(filter6), start - 10000, end )
    partNums6 should not equal debox.Buffer.empty[Int]

    val filter7 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums7 = keyIndex.partIdsFromFilters(Seq(filter7), (start + end)/2, end + 1000 )
    partNums7 should not equal debox.Buffer.empty[Int]

  }

  it("should fetch part key records from filters correctly") {
    // Add the first ten keys and row numbers
    val pkrs = partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.map { case (addr, i) =>
      val pk = partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr)
      keyIndex.addPartKey(pk, i, i, i + 10)()
        PartKeyLuceneIndexRecord(pk, i, i + 10)
    }
    keyIndex.refreshReadersBlocking()

    val filter2 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
    val result = keyIndex.partKeyRecordsFromFilters(Seq(filter2), 0, Long.MaxValue)
    val expected = Seq(pkrs(7), pkrs(8), pkrs(9))

    result.map(_.partKey.toSeq) shouldEqual expected.map(_.partKey.toSeq)
    result.map( p => (p.startTime, p.endTime)) shouldEqual expected.map( p => (p.startTime, p.endTime))
  }

  it("should fetch records from filters correctly with a missing label != with a non empty value") {
    // Weird case in prometheus where if a non existent label is used with != check with an empty value,
    // the returned result includes all results where the label is missing and those where the label exists
    // and the value is not equal to the provided value.
    // Check next test case for the check for empty value

    // Add the first ten keys and row numbers
    val pkrs = partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.map { case (addr, i) =>
      val pk = partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr)
      keyIndex.addPartKey(pk, i, i, i + 10)()
      PartKeyLuceneIndexRecord(pk, i, i + 10)
    }
    keyIndex.refreshReadersBlocking()

    // Filter != condition on a field that doesn't exist must not affect the result

    val filter1 = ColumnFilter("some", NotEquals("t".utf8))
    val filter2 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
    val result = keyIndex.partKeyRecordsFromFilters(Seq(filter1, filter2), 0, Long.MaxValue)
    val expected = Seq(pkrs(7), pkrs(8), pkrs(9))

    result.map(_.partKey.toSeq) shouldEqual expected.map(_.partKey.toSeq)
    result.map( p => (p.startTime, p.endTime)) shouldEqual expected.map( p => (p.startTime, p.endTime))
  }

  it("should not fetch records from filters correctly with a missing label != with an empty value") {

    // Weird case in prometheus where if a non existent label is used with != check with an empty value,
    // the returned result is empty

    // Add the first ten keys and row numbers
    val pkrs = partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.map { case (addr, i) =>
      val pk = partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr)
      keyIndex.addPartKey(pk, i, i, i + 10)()
      PartKeyLuceneIndexRecord(pk, i, i + 10)
    }
    keyIndex.refreshReadersBlocking()

    // Filter != condition on a field that doesn't exist must not affect the result

    val filter1 = ColumnFilter("some", NotEquals("".utf8))
    val filter2 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
    val result = keyIndex.partKeyRecordsFromFilters(Seq(filter1, filter2), 0, Long.MaxValue)
    result.isEmpty shouldBe true
  }

    it("should upsert part keys with endtime and foreachPartKeyStillIngesting should work") {
    // Add the first ten keys and row numbers
    partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.foreach { case (addr, i) =>
        val time = System.currentTimeMillis()
        keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, time)()
        if (i%2 == 0) keyIndex.upsertPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, time, time + 300)()
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
      keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, start + i)()
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
      keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, start + i, start + i + 100)()
    }
    keyIndex.refreshReadersBlocking()

    val pIds = keyIndex.partIdsEndedBefore(start + 200)
    val pIdsList = pIds.toList()
    for { i <- 0 until numPartIds} {
      pIdsList.contains(i) shouldEqual (if (i <= 100) true else false)
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
      keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, time)()
      keyIndex.refreshReadersBlocking() // updates need to be able to read startTime from index, so commit
      keyIndex.updatePartKeyWithEndTime(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, time + 10000)()
    }
    val end = System.currentTimeMillis()
    keyIndex.refreshReadersBlocking()

    // Should get empty iterator when passing no filters
    val partNums1 = keyIndex.partIdsFromFilters(Nil, start, end)
    partNums1 shouldEqual debox.Buffer(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

    val filter2 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
    val partNums2 = keyIndex.partIdsFromFilters(Seq(filter2), start, end)
    partNums2 shouldEqual debox.Buffer(7, 8, 9)

    val filter3 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums3 = keyIndex.partIdsFromFilters(Seq(filter3), start, end)
    partNums3 shouldEqual debox.Buffer(8, 9)

    val filter4 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums4 = keyIndex.partIdsFromFilters(Seq(filter4), 10, start-1)
    partNums4 shouldEqual debox.Buffer.empty[Int]

    val filter5 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums5 = keyIndex.partIdsFromFilters(Seq(filter5), end + 20000, end + 100000)
    partNums5 shouldEqual debox.Buffer.empty[Int]

    val filter6 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums6 = keyIndex.partIdsFromFilters(Seq(filter6), start - 10000, end-1 )
    partNums6 should not equal debox.Buffer.empty[Int]

    val filter7 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
    val partNums7 = keyIndex.partIdsFromFilters(Seq(filter7), (start + end)/2, end + 1000 )
    partNums7 should not equal debox.Buffer.empty[Int]
  }

  it("should parse filters with UTF8Wrapper and string correctly") {
    // Add the first ten keys and row numbers
    partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.foreach { case (addr, i) =>
      keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, System.currentTimeMillis())()
    }

    keyIndex.refreshReadersBlocking()

    val filter2 = ColumnFilter("Actor2Name", Equals(UTF8Wrapper("REGIME".utf8)))
    val partNums2 = keyIndex.partIdsFromFilters(Seq(filter2), 0, Long.MaxValue)
    partNums2 shouldEqual debox.Buffer(8, 9)

    val filter3 = ColumnFilter("Actor2Name", Equals("REGIME"))
    val partNums3 = keyIndex.partIdsFromFilters(Seq(filter3), 0, Long.MaxValue)
    partNums3 shouldEqual debox.Buffer(8, 9)
  }

  it("should obtain indexed names and values") {
    // Add the first ten keys and row numbers
    partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.foreach { case (addr, i) =>
      keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, System.currentTimeMillis())()
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
      keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, System.currentTimeMillis())()
    }

    keyIndex.refreshReadersBlocking()

    val filters1 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)),
      ColumnFilter("Actor2Name", Equals("REGIME".utf8)))
    val partNums1 = keyIndex.partIdsFromFilters(filters1, 0, Long.MaxValue)
    partNums1 shouldEqual debox.Buffer(8, 9)

    val filters2 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)),
      ColumnFilter("Actor2Name", Equals("CHINA".utf8)))
    val partNums2 = keyIndex.partIdsFromFilters(filters2, 0, Long.MaxValue)
    partNums2 shouldEqual debox.Buffer.empty[Int]
  }

  it("should ignore unsupported columns and return empty filter") {
    val index2 = new PartKeyLuceneIndex(dataset1.ref, dataset1.schema.partition, true, true, 0, 1.hour.toMillis)
    partKeyFromRecords(dataset1, records(dataset1, readers.take(10))).zipWithIndex.foreach { case (addr, i) =>
      index2.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, System.currentTimeMillis())()
    }
    keyIndex.refreshReadersBlocking()

    val filters1 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)), ColumnFilter("Year", Equals(1979)))
    val partNums1 = index2.partIdsFromFilters(filters1, 0, Long.MaxValue)
    partNums1 shouldEqual debox.Buffer.empty[Int]
  }

  it("should be able to fetch partKey from partId and partId from partKey") {
    // Add the first ten keys and row numbers
    partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.foreach { case (addr, i) =>
      val partKeyBytes = partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr)
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
        keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, start)()
        keyIndex.refreshReadersBlocking() // updates need to be able to read startTime from index, so commit
        val end = start + Random.nextInt()
        keyIndex.updatePartKeyWithEndTime(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, end)()
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

  it("should be able to fetch label values efficiently using facets") {
    val index3 = new PartKeyLuceneIndex(DatasetRef("prometheus"), Schemas.promCounter.partition,
      true, true, 0, 1.hour.toMillis)
    val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8,
                         "_ns_".utf8 -> "my_ns".utf8)

    // create 1000 time series with 10 metric names
    for { i <- 0 until 1000} {
      val counterNum = i % 10
      val partKey = partBuilder.partKeyFromObjects(Schemas.promCounter, s"counter$counterNum",
          seriesTags + ("instance".utf8 -> s"instance$i".utf8))
      index3.addPartKey(partKeyOnHeap(Schemas.promCounter.partition.binSchema, ZeroPointer, partKey), i, 5)()
    }
    index3.refreshReadersBlocking()
    val filters1 = Seq(ColumnFilter("_ws_", Equals("my_ws")))

    val partNums1 = index3.partIdsFromFilters(filters1, 0, Long.MaxValue)
    partNums1.length shouldEqual 1000

    val labelValues1 = index3.labelValuesEfficient(filters1, 0, Long.MaxValue, "_metric_")
    labelValues1.toSet shouldEqual (0 until 10).map(c => s"counter$c").toSet

    val filters2 = Seq(ColumnFilter("_ws_", Equals("NotExist")))
    val labelValues2 = index3.labelValuesEfficient(filters2, 0, Long.MaxValue, "_metric_")
    labelValues2.size shouldEqual 0

    val filters3 = Seq(ColumnFilter("_metric_", Equals("counter1")))
    val labelValues3 = index3.labelValuesEfficient(filters3, 0, Long.MaxValue, "_metric_")
    labelValues3 shouldEqual Seq("counter1")

    val labelValues4 = index3.labelValuesEfficient(filters1, 0, Long.MaxValue, "instance", 1000)
    labelValues4.toSet shouldEqual (0 until 1000).map(c => s"instance$c").toSet
  }

  it("should be able to do regular operations when faceting is disabled") {
    val index3 = new PartKeyLuceneIndex(DatasetRef("prometheus"), Schemas.promCounter.partition,
      false, false, 0, 1.hour.toMillis)
    val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8,
      "_ns_".utf8 -> "my_ns".utf8)

    // create 1000 time series with 10 metric names
    for { i <- 0 until 1000} {
      val counterNum = i % 10
      val partKey = partBuilder.partKeyFromObjects(Schemas.promCounter, s"counter$counterNum",
        seriesTags + ("instance".utf8 -> s"instance$i".utf8))
      index3.addPartKey(partKeyOnHeap(Schemas.promCounter.partition.binSchema, ZeroPointer, partKey), i, 5)()
    }
    index3.refreshReadersBlocking()
    val filters1 = Seq(ColumnFilter("_ws_", Equals("my_ws")))

    val partNums1 = index3.partIdsFromFilters(filters1, 0, Long.MaxValue)
    partNums1.length shouldEqual 1000

    intercept[IllegalArgumentException] {
      index3.labelValuesEfficient(filters1, 0, Long.MaxValue, "_metric_")
    }

    index3.partKeyFromPartId(0).isInstanceOf[Some[BytesRef]] shouldEqual true
    index3.startTimeFromPartId(0) shouldEqual 5
    index3.endTimeFromPartId(0) shouldEqual Long.MaxValue

  }


  it("must clean the input directory for Index state apart from Synced") {
    import java.io.File
    val events = ArrayBuffer.empty[(IndexState.Value, Long)]
    IndexState.values.foreach {
      indexState =>
        val indexDirectory = new File(
          System.getProperty("java.io.tmpdir"), "part-key-lucene-index-event")
        val shardDirectory = new File(indexDirectory, "0")
        shardDirectory.mkdirs()
        new File(shardDirectory, "empty").createNewFile()
        // Validate the file named empty exists
        assert(shardDirectory.list().exists(_.equals("empty")))
        val index = new PartKeyLuceneIndex(dataset6.ref, dataset6.schema.partition, true, true,0, 1.hour.toMillis,
          Some(indexDirectory),
          Some( new IndexLifecycleManager {
            def currentState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long]) =
              (indexState, None)

            def updateState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long): Unit = {
              events.append((state, time))
            }
          }))
        index.closeIndex()
        events.toList match {
          case (IndexState.Empty, _) :: Nil if indexState != IndexState.Synced =>
                                                // The file originally present must not be available
                                                assert(!shardDirectory.list().exists(_.equals("empty")))
                                                events.clear()
          case Nil                          if indexState == IndexState.Synced =>
                                                // The file originally present "must" be available
                                                assert(shardDirectory.list().exists(_.equals("empty")))
          case _                                                               =>
                                                fail("Expected an index state Empty after directory cleanup")
        }
    }
  }


  it("Should update the state as empty after the cleanup is from a corrupt index") {


  }

  it("Should update the state as Unknown and throw an exception for any error other than CorruptIndexException") {


  }
}