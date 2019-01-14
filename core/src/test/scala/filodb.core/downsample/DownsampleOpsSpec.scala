package filodb.core.downsample

import scala.collection.mutable

import java.util
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

import filodb.core.TestData
import filodb.core.binaryrecord2.{MapItemConsumer, RecordBuilder}
import filodb.core.memstore.{TimeSeriesPartition, TimeSeriesPartitionSpec, WriteBufferPool}
import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.core.metadata.Column.ColumnType._
import filodb.core.query.RawDataRangeVector
import filodb.core.store.AllChunkScan
import filodb.memory._
import filodb.memory.format.{TupleRowReader, ZeroCopyUTF8String}

// scalastyle:off null
class DownsampleOpsSpec extends FunSpec with Matchers  with BeforeAndAfterAll {

  val maxChunkSize = 200

  val promDataset = Dataset.make("prom",
    Seq("tags:map"),
    Seq("timestamp:ts:timestamp", "value:double:min,max,sum,count,avg"),
    Seq("timestamp"),
    DatasetOptions(Seq("__name__", "job"), "__name__", "value")).get

  val customDataset = Dataset.make("custom",
    Seq("name:string", "namespace:string","instance:string"),
    Seq("timestamp:ts:timestamp", "count:long:sum", "min:double:min","max:double:max","total:double:sum"),
    Seq("timestamp"),
    DatasetOptions(Seq("name", "namespace"), "name", "total")).get

  private val blockStore = new PageAlignedBlockManager(100 * 1024 * 1024,
    new MemoryStats(Map("test"-> "test")), null, 16)

  protected val ingestBlockHolder = new BlockMemFactory(blockStore, None, promDataset.blockMetaSize, true)

  protected val tsBufferPool = new WriteBufferPool(TestData.nativeMem, promDataset, maxChunkSize, 100)

  override def afterAll(): Unit = {
    blockStore.releaseBlocks()
  }

  val partKeyTags = new util.ArrayList[(String, String)]()
  partKeyTags.add("dc"->"dc1")
  partKeyTags.add("instance"->"instance1")

  val partKeyBuilder = new RecordBuilder(TestData.nativeMem, promDataset.partKeySchema, 4096)
  partKeyBuilder.startNewRecord()
  partKeyBuilder.addSortedPairsAsMap(partKeyTags, RecordBuilder.sortAndComputeHashes(partKeyTags))
  partKeyBuilder.endRecord(true)
  val partKeyBase = partKeyBuilder.allContainers.head.base
  val partKeyOffset = partKeyBuilder.allContainers.head.allOffsets(0)

  // Creates a RawDataRangeVector using Prometheus time-value schema and a given chunk size etc.
  def timeValueRV(tuples: Seq[(Long, Double)]): RawDataRangeVector = {
    val part = TimeSeriesPartitionSpec.makePart(0, promDataset, partKeyOffset, bufferPool = tsBufferPool)
    val readers = tuples.map { case (ts, d) => TupleRowReader((Some(ts), Some(d))) }
    readers.foreach { row => part.ingest(row, ingestBlockHolder) }
    // Now flush and ingest the rest to ensure two separate chunks
    part.switchBuffers(ingestBlockHolder, encode = true)
//    part.encodeAndReleaseBuffers(ingestBlockHolder)
    RawDataRangeVector(null, part, AllChunkScan, Array(0, 1))
  }

  it ("should formulate downsample ingest schema correctly for prom schema") {
    val dsSchema = DownsampleOps.downsampleIngestSchema(promDataset)
    dsSchema.columns.map(_.name) shouldEqual
      Seq("timestamp", "value-min", "value-max", "value-sum", "value-count","value-avg", "tags")
    dsSchema.columns.map(_.colType) shouldEqual
      Seq(TimestampColumn, DoubleColumn, DoubleColumn, DoubleColumn, DoubleColumn, DoubleColumn, MapColumn)
  }

  it ("should formulate downsample ingest schema correctly for non prom schema") {
    val dsSchema = DownsampleOps.downsampleIngestSchema(customDataset)
    dsSchema.columns.map(_.name) shouldEqual
      Seq("timestamp", "count-sum", "min-min", "max-max", "total-sum", "name", "namespace", "instance")
    dsSchema.columns.map(_.colType) shouldEqual
      Seq(TimestampColumn, LongColumn, DoubleColumn, DoubleColumn, DoubleColumn,
        StringColumn, StringColumn, StringColumn)
  }

  it ("should downsample sum,count,avg,min,max of prom dataset for multiple resolutions properly") {
    val data = (100000L until 200000L by 1000).map(i => (i, i*5d))
    val rv = timeValueRV(data)
    val chunkInfos = rv.chunkInfos(0L, Long.MaxValue)
    val dsSchema = DownsampleOps.downsampleIngestSchema(promDataset)
    val dsStates = DownsampleOps.initializeDownsamplerStates(promDataset,
                                                 Seq(5000, 10000), MemFactory.onHeapFactory)

    DownsampleOps.downsample(promDataset, rv.partition.asInstanceOf[TimeSeriesPartition],
                                chunkInfos, dsStates)

    // with resolution 5000
    val downsampledData1 = dsStates(0).builder.allContainers.flatMap { c =>

      c.allOffsets.foreach { off =>

        // validate tags on the partition key
        val partKeyInRecord = new mutable.HashMap[String, String]()
        val consumer = new MapItemConsumer {
          def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
            val key = new ZeroCopyUTF8String(keyBase, keyOffset + 2, UTF8StringMedium.numBytes(keyBase, keyOffset))
            val value = new ZeroCopyUTF8String(valueBase, valueOffset + 2,
                               UTF8StringMedium.numBytes(valueBase, valueOffset))
            partKeyInRecord.put(key.toString, value.toString)
          }
        }
        dsSchema.consumeMapItems(c.base, off, 6, consumer)
        partKeyInRecord shouldEqual Map("dc"->"dc1", "instance"->"instance1")

        // validate partition hash on the record
        promDataset.partKeySchema.partitionHash(partKeyBase, partKeyOffset) shouldEqual
          dsSchema.partitionHash(c.base, off)
      }

      c.iterate(dsSchema).map {r =>
        val timestamp = r.getLong(0)
        val min = r.getDouble(1)
        val max = r.getDouble(2)
        val sum = r.getDouble(3)
        val count = r.getDouble(4)
        val avg = r.getDouble(5)
        (timestamp, min, max, sum, count, avg)
      }
    }

    // timestamps
    val expectedTimestamps = (100000L to 195000L by 5000) ++ Seq(199000L)
    downsampledData1.map(_._1) shouldEqual expectedTimestamps
    // mins
    val expectedMins = Seq(500000d) ++ (505000d to 980000d by 25000)
    downsampledData1.map(_._2) shouldEqual expectedMins
    // maxes
    val expectedMaxes = (100000d to 195000d by 5000).map(_ * 5) ++ Seq(995000d)
    downsampledData1.map(_._3) shouldEqual expectedMaxes
    // sums = (min to max).sum
    val expectedSums = expectedMins.zip(expectedMaxes).map { case (min,max) => (min to max by 5000d).sum }
    downsampledData1.map(_._4) shouldEqual expectedSums
    // counts
    val expectedCounts = Seq(1d) ++ Seq.fill(19)(5d) ++ Seq(4d)
    downsampledData1.map(_._5) shouldEqual expectedCounts
    // avg
    val expectedAvgs = expectedSums.zip(expectedCounts).map { case (sum,count) => sum/count }
    downsampledData1.map(_._6) shouldEqual expectedAvgs

    // with resolution 10000
    val downsampledData2 = dsStates(1).builder.allContainers.flatMap { c =>

      c.allOffsets.foreach { off =>

        // validate tags on the partition key
        val partKeyInRecord = new mutable.HashMap[String, String]()
        val consumer = new MapItemConsumer {
          def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
            val key = new ZeroCopyUTF8String(keyBase, keyOffset + 2, UTF8StringMedium.numBytes(keyBase, keyOffset))
            val value = new ZeroCopyUTF8String(valueBase, valueOffset + 2,
              UTF8StringMedium.numBytes(valueBase, valueOffset))
            partKeyInRecord.put(key.toString, value.toString)
          }
        }
        dsSchema.consumeMapItems(c.base, off, 6, consumer)
        partKeyInRecord shouldEqual Map("dc"->"dc1", "instance"->"instance1")

        // validate partition hash on the record
        promDataset.partKeySchema.partitionHash(partKeyBase, partKeyOffset) shouldEqual
          dsSchema.partitionHash(c.base, off)
      }

      c.iterate(dsSchema).map {r =>
        val timestamp = r.getLong(0)
        val min = r.getDouble(1)
        val max = r.getDouble(2)
        val sum = r.getDouble(3)
        val count = r.getDouble(4)
        val avg = r.getDouble(5)
        (timestamp, min, max, sum, count, avg)
      }
    }

    // timestamps
    val expectedTimestamps2 = (100000L to 195000L by 10000) ++ Seq(199000L)
    downsampledData2.map(_._1) shouldEqual expectedTimestamps2
    // mins
    val expectedMins2 = Seq(500000d) ++ (505000d to 980000d by 50000)
    downsampledData2.map(_._2) shouldEqual expectedMins2
    // maxes
    val expectedMaxes2 = (100000d to 195000d by 10000).map(_ * 5) ++ Seq(995000d)
    downsampledData2.map(_._3) shouldEqual expectedMaxes2
    // sums = (min to max).sum
    val expectedSums2 = expectedMins2.zip(expectedMaxes2).map { case (min,max) => (min to max by 5000d).sum }
    downsampledData2.map(_._4) shouldEqual expectedSums2
    // counts
    val expectedCounts2 = Seq(1d) ++ Seq.fill(9)(10d) ++ Seq(9d)
    downsampledData2.map(_._5) shouldEqual expectedCounts2
    // avg
    val expectedAvgs2 = expectedSums2.zip(expectedCounts2).map { case (sum,count) => sum/count }
    downsampledData2.map(_._6) shouldEqual expectedAvgs2

  }
}
