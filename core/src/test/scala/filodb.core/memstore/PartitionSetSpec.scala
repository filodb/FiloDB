package filodb.core.memstore

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.store._
import filodb.memory._
import filodb.memory.format.UnsafeUtils

class PartitionSetSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  import MachineMetricsData._

  implicit override val patienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(50, Millis))

  import monix.execution.Scheduler.Implicits.global
  import TimeSeriesShard.BlockMetaAllocSize

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val chunkRetentionHours = 72
  // implemented by concrete test sub class
  val colStore: ColumnStore = new NullColumnStore()

  var part: TimeSeriesPartition = null

  val reclaimer = new ReclaimListener {
    def onReclaim(metaAddr: Long, numBytes: Int): Unit = {
      assert(numBytes == BlockMetaAllocSize)
      val partID = UnsafeUtils.getInt(metaAddr)
      val chunkID = UnsafeUtils.getLong(metaAddr + 4)
      part.removeChunksAt(chunkID)
    }
  }

  private val blockStore = new PageAlignedBlockManager(100 * 1024 * 1024,
    new MemoryStats(Map("test"-> "test")), reclaimer, 1, chunkRetentionHours)
  val memFactory = new NativeMemoryManager(10 * 1024 * 1024)
  val maxChunkSize = 100
  protected val bufferPool = new WriteBufferPool(memFactory, dataset2, maxChunkSize, 50)
  protected val pagedChunkStore = new DemandPagedChunkStore(dataset2, blockStore,
                                    BlockMetaAllocSize, chunkRetentionHours, 1)
  private val ingestBlockHolder = new BlockMemFactory(blockStore, None, BlockMetaAllocSize, true)

  val builder = new RecordBuilder(memFactory, dataset2.ingestionSchema)
  val partSet = PartitionSet.empty(dataset2.ingestionSchema, dataset2.comparator)

  before {
    colStore.truncate(dataset2.ref).futureValue
    partSet.clear()
  }

  val tenRecords = withMap(linearMultiSeries(), extraTags=extraTags).take(10)
  addToBuilder(builder, tenRecords)
  val ingestRecordAddrs = builder.allContainers.head.allOffsets
  // println(s"XXX container base = ${builder.allContainers.head.base}")
  // println(s"ingestRecordAddrs=$ingestRecordAddrs")
  // println(s"\n---\n${ingestRecordAddrs.foreach(a => println(dataset2.ingestionSchema.stringify(a)))}")
  val partKeyBuilder = new RecordBuilder(memFactory, dataset2.partKeySchema)
  ingestRecordAddrs.foreach { addr =>
    dataset2.comparator.buildPartKeyFromIngest(null, addr, partKeyBuilder)
  }
  val partKeyAddrs = partKeyBuilder.allContainers.head.allOffsets
  // println(s"partKeyAddrs=$partKeyAddrs")
  // println(s"\n---\n${partKeyAddrs.foreach(a => println(dataset2.partKeySchema.stringify(a)))}")

  it("+=/add should add TSPartitions only if its not already part of the set") {
    partSet.size shouldEqual 0
    partSet.isEmpty shouldEqual true

    val part = new TimeSeriesPartition(0, dataset2, partKeyAddrs(0), 0, colStore, bufferPool,
                 pagedChunkStore, new TimeSeriesShardStats(dataset1.ref, 0))

    partSet += part
    partSet.size shouldEqual 1
    partSet.isEmpty shouldEqual false
    partSet(part) shouldEqual true

    // Now adding it again should not succeed
    partSet.add(part) shouldEqual false
    partSet.size shouldEqual 1
    partSet.isEmpty shouldEqual false
    partSet(part) shouldEqual true
  }

  it("should get existing TSPartitions with getOrAddWithIngestBR") {
    val part = new TimeSeriesPartition(0, dataset2, partKeyAddrs(0), 0, colStore, bufferPool,
                 pagedChunkStore, new TimeSeriesShardStats(dataset1.ref, 0))
    partSet += part
    partSet.size shouldEqual 1

    val got = partSet.getOrAddWithIngestBR(null, ingestRecordAddrs(0), { throw new RuntimeException("error")} )
    got shouldEqual part
    partSet.size shouldEqual 1
  }

  it("should add new TSPartition if one doesnt exist with getOrAddWithIngestBR") {
    partSet.isEmpty shouldEqual true
    partSet.getWithPartKeyBR(null, partKeyAddrs(0)) shouldEqual None

    val part = new TimeSeriesPartition(0, dataset2, partKeyAddrs(0), 0, colStore, bufferPool,
                 pagedChunkStore, new TimeSeriesShardStats(dataset1.ref, 0))
    val got = partSet.getOrAddWithIngestBR(null, ingestRecordAddrs(0), part)

    partSet.size shouldEqual 1
    partSet.isEmpty shouldEqual false
    got shouldEqual part
    partSet.getWithPartKeyBR(null, partKeyAddrs(0)) shouldEqual Some(part)
  }

  it("should remove TSPartitions correctly") {
    val part = new TimeSeriesPartition(0, dataset2, partKeyAddrs(0), 0, colStore, bufferPool,
                 pagedChunkStore, new TimeSeriesShardStats(dataset1.ref, 0))
    partSet += part
    partSet.size shouldEqual 1

    partSet.remove(part)
    partSet.size shouldEqual 0
    partSet.isEmpty shouldEqual true
  }
}