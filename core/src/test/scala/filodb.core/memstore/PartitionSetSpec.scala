package filodb.core.memstore

import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._
import filodb.core.binaryrecord2.RecordBuilder
import filodb.memory._
import filodb.memory.format.UnsafeUtils

class PartitionSetSpec extends MemFactoryCleanupTest with ScalaFutures {
  import MachineMetricsData._
  import TimeSeriesPartitionSpec._

  implicit override val patienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(50, Millis))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val chunkRetentionHours = 72

  var part: TimeSeriesPartition = null

  val reclaimer = new ReclaimListener {
    def onReclaim(metaAddr: Long, numBytes: Int): Unit = {
      assert(numBytes == dataset2.blockMetaSize)
      val partID = UnsafeUtils.getInt(metaAddr)
      val chunkID = UnsafeUtils.getLong(metaAddr + 4)
      part.removeChunksAt(chunkID)
    }
  }

  private val blockStore = new PageAlignedBlockManager(100 * 1024 * 1024,
    new MemoryStats(Map("test"-> "test")), reclaimer, 1)
  protected val bufferPool = new WriteBufferPool(memFactory, dataset2, TestData.storeConf)
  private val ingestBlockHolder = new BlockMemFactory(blockStore, None, dataset2.blockMetaSize, true)

  val builder = new RecordBuilder(memFactory, dataset2.ingestionSchema)
  val partSet = PartitionSet.empty(dataset2.ingestionSchema, dataset2.comparator)

  before {
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

    val part = makePart(0, dataset2, partKeyAddrs(0), bufferPool)

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
    val part = makePart(0, dataset2, partKeyAddrs(0), bufferPool)
    partSet += part
    partSet.size shouldEqual 1

    val got = partSet.getOrAddWithIngestBR(null, ingestRecordAddrs(0), { throw new RuntimeException("error")} )
    got shouldEqual part
    partSet.size shouldEqual 1
  }

  it("should add new TSPartition if one doesnt exist with getOrAddWithIngestBR") {
    partSet.isEmpty shouldEqual true
    partSet.getWithPartKeyBR(null, partKeyAddrs(0)) shouldEqual None
    partSet.getWithIngestBR(null, ingestRecordAddrs(0)) shouldEqual null

    val part = makePart(0, dataset2, partKeyAddrs(0), bufferPool)
    val got = partSet.getOrAddWithIngestBR(null, ingestRecordAddrs(0), part)

    partSet.size shouldEqual 1
    partSet.isEmpty shouldEqual false
    got shouldEqual part
    partSet.getWithPartKeyBR(null, partKeyAddrs(0)) shouldEqual Some(part)
    partSet.getWithIngestBR(null, ingestRecordAddrs(0)) shouldEqual part
  }

  it("should not add new TSPartition if function returns null") {
    partSet.isEmpty shouldEqual true
    partSet.getWithPartKeyBR(null, partKeyAddrs(0)) shouldEqual None

    val got = partSet.getOrAddWithIngestBR(null, ingestRecordAddrs(0), null)
    got shouldEqual null
    partSet.isEmpty shouldEqual true
    partSet.getWithPartKeyBR(null, partKeyAddrs(0)) shouldEqual None
  }

  it("should remove TSPartitions correctly") {
    val part = makePart(0, dataset2, partKeyAddrs(0), bufferPool)
    partSet += part
    partSet.size shouldEqual 1

    partSet.remove(part)
    partSet.size shouldEqual 0
    partSet.isEmpty shouldEqual true
  }
}