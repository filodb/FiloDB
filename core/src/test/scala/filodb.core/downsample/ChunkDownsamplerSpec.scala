package filodb.core.downsample

import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

import filodb.core.TestData
import filodb.core.memstore.{TimeSeriesPartition, TimeSeriesPartitionSpec, WriteBufferPool}
import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.core.metadata.Column.ColumnType._
import filodb.core.query.RawDataRangeVector
import filodb.core.store.AllChunkScan
import filodb.memory.{BlockMemFactory, MemFactory, MemoryStats, PageAlignedBlockManager}
import filodb.memory.format.TupleRowReader

// scalastyle:off null
class ChunkDownsamplerSpec extends FunSpec with Matchers  with BeforeAndAfterAll {

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

  // Creates a RawDataRangeVector using Prometheus time-value schema and a given chunk size etc.
  def timeValueRV(tuples: Seq[(Long, Double)]): RawDataRangeVector = {
    val part = TimeSeriesPartitionSpec.makePart(0, promDataset, bufferPool = tsBufferPool)
    val readers = tuples.map { case (ts, d) => TupleRowReader((Some(ts), Some(d))) }
    readers.foreach { row => part.ingest(row, ingestBlockHolder) }
    // Now flush and ingest the rest to ensure two separate chunks
    part.switchBuffers(ingestBlockHolder, encode = true)
//    part.encodeAndReleaseBuffers(ingestBlockHolder)
    RawDataRangeVector(null, part, AllChunkScan, Array(0, 1))
  }

  it ("should formulate downsample ingest schema correctly for prom schema") {
    val dsSchema = ChunkDownsampler.downsampleIngestSchema(promDataset)
    dsSchema.columns.map(_.name) shouldEqual
      Seq("tags", "timestamp", "value-min", "value-max", "value-sum", "value-count","value-avg")
    dsSchema.columns.map(_.colType) shouldEqual
      Seq(MapColumn, TimestampColumn, DoubleColumn, DoubleColumn, DoubleColumn, DoubleColumn)
  }

  it ("should formulate downsample ingest schema correctly for non prom schema") {
    val dsSchema = ChunkDownsampler.downsampleIngestSchema(customDataset)
    dsSchema.columns.map(_.name) shouldEqual
      Seq("name", "namespace", "instance", "timestamp", "count-sum", "min-min", "max-max", "total-sum")
    dsSchema.columns.map(_.colType) shouldEqual
      Seq(StringColumn, StringColumn, StringColumn, TimestampColumn,
        LongColumn, DoubleColumn, DoubleColumn, DoubleColumn)
  }

  it ("should downsample prom dataset for multiple resolutions properly") {
    val data = (100000L until 200000L by 1000).map(i => (i, i*5d))
    val rv = timeValueRV(data)
    val chunkInfos = rv.chunkInfos(0L, Long.MaxValue)
    val dsSchema = ChunkDownsampler.downsampleIngestSchema(promDataset)
    val dsStates = ChunkDownsampler.initializeDownsamplerStates(promDataset,
                                                 Seq(5000, 10000), MemFactory.onHeapFactory)

    ChunkDownsampler.downsample(promDataset, rv.partition.asInstanceOf[TimeSeriesPartition],
                                chunkInfos, dsStates)

    // with resolution 5000
    val downsampledData1 = dsStates(0).builder.allContainers.flatMap { c =>
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
