package filodb.core.downsample

import scala.collection.mutable

import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

import filodb.core.{TestData, MachineMetricsData => MMD}
import filodb.core.binaryrecord2.{MapItemConsumer, RecordBuilder, RecordContainer}
import filodb.core.memstore.{TimeSeriesPartition, TimeSeriesPartitionSpec, TimeSeriesShardStats, WriteBufferPool}
import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.core.metadata.Column.ColumnType._
import filodb.core.query.RawDataRangeVector
import filodb.core.store.AllChunkScan
import filodb.memory._
import filodb.memory.format.{TupleRowReader, ZeroCopyUTF8String, vectors => bv}

// scalastyle:off null
class ShardDownsamplerSpec extends FunSpec with Matchers  with BeforeAndAfterAll {

  val promDataset = Dataset.make("custom1",
    Seq("someStr:string", "tags:map"),
    Seq("timestamp:ts", "value:double"),
    Seq("timestamp"),
    Seq("tTime(0)", "dMin(1)", "dMax(1)", "dSum(1)", "dCount(1)", "dAvg(1)"),
    DatasetOptions(Seq("__name__", "job"), "__name__", "value")).get

  val customDataset = Dataset.make("custom2",
    Seq("name:string", "namespace:string", "instance:string"),
    Seq("timestamp:ts", "count:double", "min:double", "max:double", "total:double", "avg:double", "h:hist:counter=false"),
    Seq("timestamp"),
    Seq("tTime(0)", "dSum(1)", "dMin(2)", "dMax(3)", "dSum(4)", "dAvgAc(5@1)", "hSum(6)"),
    DatasetOptions(Seq("name", "namespace"), "name", "total")).get

  private val blockStore = MMD.blockStore
  protected val ingestBlockHolder = new BlockMemFactory(blockStore, None, promDataset.blockMetaSize, true)

  val storeConf = TestData.storeConf.copy(maxChunksSize = 200)
  protected val tsBufferPool = new WriteBufferPool(TestData.nativeMem, promDataset, storeConf)

  override def afterAll(): Unit = {
    blockStore.releaseBlocks()
  }

  import ZeroCopyUTF8String._

  val partKeyTags = Map("dc".utf8 -> "dc1".utf8, "instance".utf8 -> "instance1".utf8)

  val partKeyBuilder = new RecordBuilder(TestData.nativeMem, promDataset.partKeySchema, 4096)
  partKeyBuilder.startNewRecord()
  partKeyBuilder.addString("someStringValue")
  partKeyBuilder.addMap(partKeyTags)
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

  val downsampleOps = new ShardDownsampler(promDataset, 0, true, Seq(5000, 10000), NoOpDownsamplePublisher,
    new TimeSeriesShardStats(promDataset.ref, 0))

  it ("should formulate downsample ingest schema correctly for custom1 schema") {
    val dsSchema = downsampleOps.downsampleIngestSchema()
    dsSchema.columns.map(_.name) shouldEqual
      Seq("tTime", "dMin", "dMax", "dSum", "dCount","dAvg", "someStr", "tags")
    dsSchema.columns.map(_.colType) shouldEqual
      Seq(TimestampColumn, DoubleColumn, DoubleColumn, DoubleColumn, DoubleColumn, DoubleColumn,
        StringColumn, MapColumn)
  }

  it ("should formulate downsample ingest schema correctly for custom2 schema") {
    val downsampleOps = new ShardDownsampler(customDataset, 0, true, Seq(5000, 10000), NoOpDownsamplePublisher,
      new TimeSeriesShardStats(customDataset.ref, 0))
    val dsSchema = downsampleOps.downsampleIngestSchema()
    dsSchema.columns.map(_.name) shouldEqual
      Seq("tTime", "dSum", "dMin", "dMax", "dSum", "dAvgAc", "hSum", "name", "namespace", "instance")
    dsSchema.columns.map(_.colType) shouldEqual
      Seq(TimestampColumn, DoubleColumn, DoubleColumn, DoubleColumn, DoubleColumn, DoubleColumn, HistogramColumn,
        StringColumn, StringColumn, StringColumn)
  }

  it ("should downsample sum,count,avg,min,max of prom dataset for multiple resolutions properly") {
    val data = (100000L until 200000L by 1000).map(i => (i, i*5d))
    val rv = timeValueRV(data)
    val chunkInfos = rv.chunkInfos(0L, Long.MaxValue)
    val dsSchema = downsampleOps.downsampleSchema
    val dsRecords = downsampleOps.newEmptyDownsampleRecords

    downsampleOps.populateDownsampleRecords(rv.partition.asInstanceOf[TimeSeriesPartition], chunkInfos, dsRecords)

    // with resolution 5000
    val downsampledData1 = dsRecords(0).builder.optimalContainerBytes().flatMap { con =>
      val c = RecordContainer(con)
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
        dsSchema.asZCUTF8Str(c.base, off, 6).toString shouldEqual "someStringValue"
        dsSchema.consumeMapItems(c.base, off, 7, consumer)
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
    val downsampledData2 = dsRecords(1).builder.optimalContainerBytes().flatMap { con =>
      val c = RecordContainer(con)
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
        dsSchema.asZCUTF8Str(c.base, off, 6).toString shouldEqual "someStringValue"
        dsSchema.consumeMapItems(c.base, off, 7, consumer)
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

  val histDSDownsamplers = Seq("tTime(0)", "tTime(1)", "tTime(2)", "hSum(3)")
  val histDSDataset = MMD.histDataset.copy(
                        downsamplers = Dataset.validateDownsamplers(histDSDownsamplers).get)

  // Create downsampleOps for histogram dataset.  Samples every 10s, downsample freq 60s/1min
  val downsampleOpsH = new ShardDownsampler(histDSDataset, 0, true, Seq(60000), NoOpDownsamplePublisher,
    new TimeSeriesShardStats(histDSDataset.ref, 0))

  def emptyAggHist: bv.MutableHistogram = bv.MutableHistogram.empty(MMD.histBucketScheme)

  it("should downsample histogram schema/dataset correctly") {
    val startTS = 610000L   // So we can group by minute easily
    val (data, rv) = MMD.histogramRV(startTS, numSamples = 200)
    val chunkInfos = rv.chunkInfos(0L, Long.MaxValue)
    val dsSchema = downsampleOpsH.downsampleSchema
    val dsRecords = downsampleOpsH.newEmptyDownsampleRecords

    downsampleOpsH.populateDownsampleRecords(rv.partition.asInstanceOf[TimeSeriesPartition], chunkInfos, dsRecords)

    val downsampledData1 = dsRecords(0).builder.optimalContainerBytes().flatMap { con =>
      val c = RecordContainer(con)

      c.iterate(dsSchema).map {r =>
        val timestamp = r.getLong(0)
        val count= r.getLong(1)
        val sum = r.getLong(2)
        val hist = r.getHistogram(3)
        (timestamp, count, sum, hist)
      }
    }

    val expectedSums = data.grouped(6).toSeq.map { dataRows =>
      dataRows.map(_(3).asInstanceOf[bv.MutableHistogram])
              .foldLeft(emptyAggHist) { case (agg, h) => agg.add(h); agg }
    }

    // Skip comparing the last sample because end of chunk=100 rows is not evenly divisible by 6
    downsampledData1.zip(expectedSums).take(100/6).foreach { case (dsData, expected) =>
      dsData._4 shouldEqual expected
    }
  }
}
