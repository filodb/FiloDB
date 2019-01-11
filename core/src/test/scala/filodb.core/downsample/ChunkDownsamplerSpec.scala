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
    Seq("timestamp:ts:timestamp", "value:double:min,max,sum,count"),
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
    part.encodeAndReleaseBuffers(ingestBlockHolder)
    RawDataRangeVector(null, part, AllChunkScan, Array(0, 1))
  }

  it ("should formulate downsample ingest schema correctly for prom schema") {
    val dsSchema = ChunkDownsampler.downsampleIngestSchema(promDataset)
    dsSchema.columns.map(_.name) shouldEqual
      Seq("tags", "timestamp", "value-min", "value-max", "value-sum", "value-count")
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

  ignore ("should downsample prom dataset for multiple resolutions properly") {
    val data = (100000L until 200000L by 1000).map(i => (i, i*5d))
    val rv = timeValueRV(data)
    val chunkInfos = rv.chunkInfos(0L, Long.MaxValue)
    val dsSchema = ChunkDownsampler.downsampleIngestSchema(promDataset)
    val dsStates = ChunkDownsampler.initializeDownsamplerStates(promDataset,
                                                 Seq(5000, 10000), MemFactory.onHeapFactory)

    ChunkDownsampler.downsample(promDataset, rv.partition.asInstanceOf[TimeSeriesPartition],
                                chunkInfos, dsStates)
    val downsampledData1 = dsStates(0).builder.allContainers.flatMap { c =>
      c.iterate(dsSchema).map {r =>
        val timestamp = r.getLong(1)
        val min = r.getDouble(2)
        val max = r.getDouble(3)
        val sum = r.getDouble(4)
        val count = r.getDouble(5)
        (timestamp, min, max, sum, count)
      }
    }

    val downsampledData2 = dsStates(1).builder.allContainers.flatMap { c =>
      c.iterate(dsSchema).map {r =>
        val timestamp = r.getLong(1)
        val min = r.getDouble(2)
        val max = r.getDouble(3)
        val sum = r.getDouble(4)
        val count = r.getDouble(5)
        (timestamp, min, max, sum, count)
      }
    }

    // TODO validate records
  }
}
