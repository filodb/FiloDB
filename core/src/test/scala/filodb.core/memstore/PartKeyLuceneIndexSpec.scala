package filodb.core.memstore

import com.googlecode.javaewah.IntIterator
import filodb.core._
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.ratelimit.{CardinalityTracker, RocksDbCardinalityStore}
import filodb.core.metadata.{Dataset, DatasetOptions, PartitionSchema, Schemas}
import filodb.core.query.{ColumnFilter, Filter}
import filodb.memory.{BinaryRegionConsumer, MemFactory}
import filodb.memory.format.UnsafeUtils.ZeroPointer
import filodb.memory.format.ZeroCopyUTF8String._
import org.apache.lucene.util.BytesRef
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.util.Random

class PartKeyLuceneIndexSpec extends AnyFunSpec with Matchers with BeforeAndAfter with PartKeyIndexRawSpec {
  import Filter._
  import GdeltTestData._

  val keyIndex = new PartKeyLuceneIndex(dataset6.ref, dataset6.schema.partition, true, true,0, 1.hour.toMillis,
    Some(new java.io.File(System.getProperty("java.io.tmpdir"), "part-key-lucene-index")))

  val partBuilder = new RecordBuilder(TestData.nativeMem)

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

  protected def createNewIndex(ref: DatasetRef,
                               schema: PartitionSchema,
                               facetEnabledAllLabels: Boolean,
                               facetEnabledShardKeyLabels: Boolean,
                               shardNum: Int,
                               retentionMillis: Long,
                               diskLocation: Option[File],
                               lifecycleManager: Option[IndexMetadataStore]): PartKeyIndexRaw = {
    new PartKeyLuceneIndex(ref, schema, facetEnabledAllLabels, facetEnabledShardKeyLabels, shardNum, retentionMillis,
      diskLocation, lifecycleManager)
  }

  it should behave like commonPartKeyTests(keyIndex, partBuilder)

  it("should fetch part key records from filters correctly with index caching disabled") {
    // Add the first ten keys and row numbers
    val keyIndexNoCache =
      new PartKeyLuceneIndex(dataset6.ref, dataset6.schema.partition,
        true,
        true,
        0,
        1.hour.toMillis,
        disableIndexCaching = true)
    val pkrs = partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.map { case (addr, i) =>
      val pk = partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr)
      keyIndexNoCache.addPartKey(pk, i, i, i + 10)()
      PartKeyLuceneIndexRecord(pk, i, i + 10)
    }
    keyIndexNoCache.refreshReadersBlocking()

    val filter2 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
    Range(1, 100).foreach(_ => {
      val result = keyIndexNoCache.partKeyRecordsFromFilters(Seq(filter2), 0, Long.MaxValue)
      val expected = Seq(pkrs(7), pkrs(8), pkrs(9))

      result.map(_.partKey.toSeq) shouldEqual expected.map(_.partKey.toSeq)
      result.map(p => (p.startTime, p.endTime)) shouldEqual expected.map(p => (p.startTime, p.endTime))
    })

  }

  it("should fetch part key iterator records from filters correctly") {
    // Add the first ten keys and row numbers
    val pkrs = partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
      .zipWithIndex.map { case (addr, i) =>
      val pk = partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr)
      keyIndex.addPartKey(pk, i, i, i + 10)()
      PartKeyLuceneIndexRecord(pk, i, i + 10)
    }
    keyIndex.refreshReadersBlocking()

    val filter2 = ColumnFilter("Actor2Code", Equals("GOV".utf8))

    val pks = ArrayBuffer.empty[Array[Byte]]
    val matches = keyIndex.foreachPartKeyMatchingFilter(Seq(filter2), 0, Long.MaxValue, b => {

      val copy = new Array[Byte](b.length)
      Array.copy(b.bytes, b.offset, copy, 0, b.length)
      pks.append(copy)
    })
    matches shouldEqual 3
    pks.zip(Seq(pkrs(7), pkrs(8), pkrs(9))).foreach{
      case (b, pk) => pk.partKey shouldEqual b
    }
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

  it("should be able to do regular operations when faceting is disabled and additional faceting in dataset") {
    val facetIndex = new PartKeyLuceneIndex(dataset7.ref, dataset7.schema.partition,
      false, true, 0, 1.hour.toMillis)
    val addedKeys = partKeyFromRecords(dataset7, records(dataset7, readers.take(10)), Some(partBuilder))
      .zipWithIndex.map { case (addr, i) =>
      val start = Math.abs(Random.nextLong())
      facetIndex.addPartKey(partKeyOnHeap(dataset7.partKeySchema, ZeroPointer, addr), i, start)()
    }
    facetIndex.refreshReadersBlocking()
    val filters1 = Seq.empty

    val partNums1 = facetIndex.partIdsFromFilters(filters1, 0, Long.MaxValue)
    partNums1.length shouldEqual 10

    the [IllegalArgumentException] thrownBy {
      facetIndex.labelValuesEfficient(filters1, 0, Long.MaxValue, "Actor2Code-Actor2Name")
    } should have message "requirement failed: Faceting not enabled for label Actor2Code-Actor2Name; labelValuesEfficient should not have been called"
  }

  it("toStringPairsMap should return a map of label key value pairs") {
    val columns = Seq("timestamp:ts", "min:double", "avg:double", "max:double", "count:long", "tags:map")
    val options = DatasetOptions.DefaultOptions.copy(metricColumn = "_metric_")
    val dataset1 = Dataset("metrics1",
      Seq("timestamp:ts", "min:double", "avg:double", "max:double", "count:long", "_metric_:string", "tags:map"),
      columns, options)
    val partSchema = dataset1.schema.partition
    val builder = new RecordBuilder(MemFactory.onHeapFactory)

    val extraTags = Map(
      "_ws_".utf8 -> "my_ws".utf8,
      "_ns_".utf8 -> "my_ns".utf8,
      "job".utf8 -> "test_job".utf8
    )
    val binRecords = new collection.mutable.ArrayBuffer[(Any, Long)]
    val binConsumer = new BinaryRegionConsumer {
      def onNext(base: Any, offset: Long): Unit = binRecords += ((base, offset))
    }
    val data = MachineMetricsData.withMap(
      MachineMetricsData.linearMultiSeries(numSeries = 1),
      extraTags = extraTags).take(1)
    MachineMetricsData.addToBuilder(builder, data, dataset1.schema)
    val containers = builder.allContainers
    containers.head.consumeRecords(binConsumer)

    for (elem <- binRecords) {
      val labelKV = partSchema.binSchema.toStringPairs(elem._1, elem._2).toMap
      for(tag <- extraTags){
        labelKV(tag._1.toString) shouldEqual tag._2.toString
      }
    }
  }

  it("should correctly build cardinality count") {
    // build card tracker
    val rocksDBStore = new RocksDbCardinalityStore(MachineMetricsData.dataset2.ref, 10)
    val cardTracker = new CardinalityTracker(MachineMetricsData.dataset2.ref, 10, 3,
      Seq(5000, 5000, 5000, 5000), rocksDBStore, flushCount = Some(100))

    // build lucene index and add records
    val idx = new PartKeyLuceneIndex(
      MachineMetricsData.dataset2.ref,
      MachineMetricsData.dataset2.schema.partition, true, true,
      10, 1.hour.toMillis,
      Some(new java.io.File(System.getProperty("java.io.tmpdir"), "part-key-lucene-index")))
    // create 1000 time series with different metric names
    for {i <- 1 until 1001} {
      val counterNum = i % 10
      val tags = Map("_ws_".utf8 -> s"my_ws$counterNum".utf8,
        "_ns_".utf8 -> s"my_ns$counterNum".utf8,
        "instance".utf8 -> s"instance$counterNum".utf8,
        "_metric_".utf8 -> s"counter$i".utf8)
      val partKey = partBuilder.partKeyFromObjects(MachineMetricsData.dataset2.schema, s"counter$i", tags)
      idx.addPartKey(
        partKeyOnHeap(MachineMetricsData.dataset2.schema.partition.binSchema, ZeroPointer, partKey), i, 5)()
    }
    idx.refreshReadersBlocking()

    // trigger cardinality count calculation
    idx.calculateCardinality(MachineMetricsData.dataset2.schema.partition, cardTracker)

    cardTracker.scan(Seq(), 0)(0).value.tsCount shouldEqual 1000
    for {i <- 1 until 1001} {
      val counterNum = i % 10
      cardTracker.scan(Seq(s"my_ws$counterNum"), 1)(0).value.tsCount shouldEqual 100
      cardTracker.scan(Seq(s"my_ws$counterNum", s"my_ns$counterNum"), 2)(0).value.tsCount shouldEqual 100
      cardTracker.scan(Seq(s"my_ws$counterNum", s"my_ns$counterNum", s"counter$i"), 3)(0)
        .value.tsCount shouldEqual 1
    }

    // close CardinalityTracker to avoid leaking of resources
    cardTracker.close()
  }

}