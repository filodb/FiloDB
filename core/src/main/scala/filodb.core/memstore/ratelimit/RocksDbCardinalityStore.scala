package filodb.core.memstore.ratelimit

import java.io.File
import java.nio.charset.StandardCharsets

import scala.concurrent.duration._
import scala.reflect.io.Directory

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.metric.MeasurementUnit
import kamon.tag.TagSet
import monix.reactive.Observable
import org.rocksdb._
import spire.syntax.cfor._

import filodb.core.{DatasetRef, GlobalScheduler}
import filodb.memory.format.UnsafeUtils

/**
 * Stored as values in the RocksDb database.
 * Identical to Cardinality, except that only the least-significant prefix name is stored.
 */
case class CardinalityNode(name: String, tsCount: Int, activeTsCount: Int,
                           childrenCount: Int, childrenQuota: Int)

case object CardinalityNode {
  def fromCardinality(card: Cardinality): CardinalityNode = {
    CardinalityNode(if (card.prefix.nonEmpty) card.prefix.last else "",
                    card.tsCount, card.activeTsCount, card.childrenCount, card.childrenQuota)
  }

  def toCardinality(card: CardinalityNode, prefix: Seq[String]): Cardinality = {
    Cardinality(prefix, card.tsCount, card.activeTsCount, card.childrenCount, card.childrenQuota)
  }
}

class CardinalityNodeSerializer extends Serializer[CardinalityNode] {
  def write(kryo: Kryo, output: Output, card: CardinalityNode): Unit = {
    output.writeString(card.name)
    output.writeInt(card.tsCount, true)
    output.writeInt(card.activeTsCount, true)
    output.writeInt(card.childrenCount, true)
    output.writeInt(card.childrenQuota, true)
  }

  def read(kryo: Kryo, input: Input, t: Class[CardinalityNode]): CardinalityNode = {
    CardinalityNode(input.readString(), input.readInt(true), input.readInt(true),
                    input.readInt(true), input.readInt(true))
  }
}

object RocksDbCardinalityStore {
  private lazy val loadRocksDbLibrary = RocksDB.loadLibrary()
  private val KeySeparator: Char = 0x1E
  private val NotFound = UnsafeUtils.ZeroPointer.asInstanceOf[Array[Byte]]

  // ======= DB Tuning ===========
  // not making them config intentionally since RocksDB tuning needs more care
  private[ratelimit] val TOTAL_OFF_HEAP_SIZE = 32L << 20 // 32 MB
  private[ratelimit] val LRU_CACHE_SIZE = 16L << 20 // 16 MB
  private val BLOCK_SIZE = 4096L // 4 KB
  private val NUM_WRITE_BUFFERS = 4
  private val WRITE_BUF_SIZE = 4L << 20 // 4 MB

}

class RocksDbCardinalityStore(ref: DatasetRef, shard: Int) extends CardinalityStore with StrictLogging {

  import RocksDbCardinalityStore._
  loadRocksDbLibrary

  // ======= DB Config ===========
  private val cache = new LRUCache(LRU_CACHE_SIZE)
  // caps total memory used by rocksdb memTables, blockCache
  private val writeBufferManager = new WriteBufferManager(TOTAL_OFF_HEAP_SIZE, cache)
  private val options = {
    val opts = new Options().setCreateIfMissing(true)

    val tableConfig = new BlockBasedTableConfig()
    tableConfig.setBlockCache(cache)
    tableConfig.setCacheIndexAndFilterBlocks(true)
    tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true)
    tableConfig.setPinTopLevelIndexAndFilter(true)
    tableConfig.setBlockSize(BLOCK_SIZE)
    opts.setTableFormatConfig(tableConfig)

    opts.setWriteBufferManager(writeBufferManager)
    opts.setMaxWriteBufferNumber(NUM_WRITE_BUFFERS) // number of memtables
    opts.setWriteBufferSize(WRITE_BUF_SIZE) // size of each memtable

    opts
  }

  private val baseDir = new File(System.getProperty("java.io.tmpdir"))
  private val baseName = s"cardStore-$ref-$shard-${System.currentTimeMillis()}"
  private val dbDirInTmp = new File(baseDir, baseName)
  private val db = RocksDB.open(options, dbDirInTmp.getAbsolutePath)
  @volatile private var closed = false;
  logger.info(s"Opening new Cardinality DB for shard=$shard dataset=$ref at ${dbDirInTmp.getAbsolutePath}")

  private val kryo = new ThreadLocal[Kryo]() {
    override def initialValue(): Kryo = {
      val k = new Kryo()
      k.addDefaultSerializer(classOf[CardinalityNode], classOf[CardinalityNodeSerializer])
      k
    }
  }

  // ======= Metrics ===========
  private val tags = Map("shard" -> shard.toString, "dataset" -> ref.toString)
  private val diskSpaceUsedMetric = Kamon.gauge("card-store-disk-space-used", MeasurementUnit.information.bytes)
    .withTags(TagSet.from(tags))
  private val memoryUsedMetric = Kamon.gauge("card-store-offheap-mem-used")
    .withTags(TagSet.from(tags))
  private val compactionBytesPendingMetric = Kamon.gauge("card-store-compaction-pending",
    MeasurementUnit.information.bytes).withTags(TagSet.from(tags))
  private val numRunningCompactionsMetric = Kamon.gauge("card-store-num-running-compactions")
    .withTags(TagSet.from(tags))
  private val numKeysMetric = Kamon.gauge("card-store-est-num-keys")
    .withTags(TagSet.from(tags))

  private val metricsReporter = Observable.interval(1.minute)
    .onErrorRestart(Int.MaxValue)
    .foreach(_ => updateMetrics())(GlobalScheduler.globalImplicitScheduler)

  var lastMetricsReportTime = 0L
  private def updateMetrics(): Unit = {
    if (!closed) {
      val now = System.currentTimeMillis()
      // dump DB stats every 5 minutes
      if (now - lastMetricsReportTime > 1000 * 60 * 5) {
        logger.info(s"Card Store Stats dataset=$ref shard=$shard $statsAsString")
        lastMetricsReportTime = now
      }
      diskSpaceUsedMetric.update(diskSpaceUsed)
      numKeysMetric.update(estimatedNumKeys)
      memoryUsedMetric.update(memTablesSize + blockCacheSize + tableReadersSize)
      compactionBytesPendingMetric.update(compactionBytesPending)
      numRunningCompactionsMetric.update(numRunningCompactions)
    }
  }

  //  List of all RocksDB properties at https://github.com/facebook/rocksdb/blob/6.12.fb/include/rocksdb/db.h#L720
  def statsAsString: String = db.getProperty("rocksdb.stats")
  def estimatedNumKeys: Long = db.getLongProperty("rocksdb.estimate-num-keys")
  // Returns the total size, in bytes, of all the SST files.
  // WAL files are not included in the calculation.
  def diskSpaceUsed: Long = db.getLongProperty("rocksdb.total-sst-files-size")
  def memTablesSize: Long = db.getLongProperty("rocksdb.size-all-mem-tables")
  def blockCacheSize: Long = db.getLongProperty("rocksdb.block-cache-usage")
  def tableReadersSize: Long = db.getLongProperty("rocksdb.estimate-table-readers-mem")
  def compactionBytesPending: Long = db.getLongProperty("rocksdb.estimate-pending-compaction-bytes")
  def numRunningCompactions: Long = db.getLongProperty("rocksdb.num-running-compactions")
  // consider compaction-pending yes/no

  /**
   * In order to enable quick prefix search, we formulate string based keys to the RocksDB
   * key-value store.
   *
   * For example, here is the list of rocksDb keys for a few shard keys. {KeySeparator} is a
   * special character chosen as the separator char between shard key elements. The value
   * at the beginning of each key indicates the size of the prefix it represents.
   *
   * This model helps with fast prefix searches to do top-k scans.
   *
   * BTW, Remove quote chars from actual string key.
   * They are there just to emphasize the shard key element in the string. "0" represents the root.
   *
   * <pre>
   * 0
   * 1{KeySeparator}"myWs1"
   * 1{KeySeparator}"myWs2"
   * 2{KeySeparator}"myWs1"{KeySeparator}"myNs11"
   * 2{KeySeparator}"myWs1"{KeySeparator}"myNs12"
   * 2{KeySeparator}"myWs2"{KeySeparator}"myNs21"
   * 2{KeySeparator}"myWs2"{KeySeparator}"myNs22"
   * 3{KeySeparator}"myWs1"{KeySeparator}"myNs11"{KeySeparator}"heap_usage"
   * 3{KeySeparator}"myWs1"{KeySeparator}"myNs11"{KeySeparator}"cpu_usage"
   * 3{KeySeparator}"myWs2"{KeySeparator}"myNs21"{KeySeparator}"network_usage"
   * </pre>
   *
   * In the above tree, we simply do a prefix search on <pre> 2{KeySeparator}"myWs1"{KeySeparator} </pre>
   * to get namespaces under workspace myWs1.
   *
   * @param shardKeyPrefix Zero or more elements that make up shard key prefix
   * @param keyDepth size to append at the beginning of the key
   *   Note: When keyDepth > shardKeyPrefix.size, builds keys as:
   *         <keyDepth>{KeySeparator}<name-1>{KeySeparator}<name-2> ... <name-n>{KeySeparator}
   *         When keyDepth == shardKeyPrefix.size, the final KeySeparator is omitted.
   * @return string key to use to perform reads and writes of entries into RocksDB
   */
  private def toStringKeyPrefix(shardKeyPrefix: Seq[String], keyDepth: Int): String = {
    import RocksDbCardinalityStore._

    val b = new StringBuilder
    b.append(keyDepth)
    cforRange { 0 until shardKeyPrefix.length } { i =>
      b.append(KeySeparator)
      b.append(shardKeyPrefix(i))
    }
    if (keyDepth > shardKeyPrefix.size) {
      b.append(KeySeparator)
    }
    b.toString()
  }

  /**
   * Builds keys as:
   *   <keyDepth>{KeySeparator}<name-1>{KeySeparator}<name-2> ... <name-n>
   * The final {KeySeparator} is always omitted.
   */
  private def toStringKey(shardKeyPrefix: Seq[String]): String = {
    toStringKeyPrefix(shardKeyPrefix, shardKeyPrefix.size)
  }

  private def cardinalityNodeToBytes(card: CardinalityNode): Array[Byte] = {
    val out = new Output(500)
    kryo.get().writeObject(out, card)
    out.close()
    out.toBytes
  }

  private def bytesToCardinalityNode(bytes: Array[Byte]): CardinalityNode = {
    val inp = new Input(bytes)
    val c = kryo.get().readObject(inp, classOf[CardinalityNode])
    inp.close()
    c
  }

  override def store(card: Cardinality): Unit = {
    val key = toStringKey(card.prefix).getBytes(StandardCharsets.UTF_8)
    logger.debug(s"Storing shard=$shard dataset=$ref ${new String(key)} with $card")
    db.put(key, cardinalityNodeToBytes(CardinalityNode.fromCardinality(card)))
  }

  def getOrZero(shardKeyPrefix: Seq[String], zero: Cardinality): Cardinality = {
    val value = db.get(toStringKey(shardKeyPrefix).getBytes(StandardCharsets.UTF_8))
    if (value == NotFound) zero else CardinalityNode.toCardinality(bytesToCardinalityNode(value), shardKeyPrefix)
  }

  override def remove(shardKeyPrefix: Seq[String]): Unit = {
    db.delete(toStringKey(shardKeyPrefix).getBytes(StandardCharsets.UTF_8))
  }

  override def scanChildren(shardKeyPrefix: Seq[String], depth: Int): CardIter = {
    require(depth > shardKeyPrefix.size,
      s"scan depth $depth must be greater than the size of the prefix ${shardKeyPrefix.size}")

    new CardIter {
      val it_ = db.newIterator()
      var nextCard_ = Cardinality(Seq("should be overwritten before exposed"), -1, -1, -1, -1)
      val strPrefix_ = toStringKeyPrefix(shardKeyPrefix, depth)
      var isSeeked = false

      override def seek(): Unit = {
        logger.debug(s"Scanning shard=$shard dataset=$ref ${new String(strPrefix_)}")
        it_.seek(strPrefix_.getBytes(StandardCharsets.UTF_8))
        isSeeked = true
      }

      // note: must be called exactly once before each next() call
      override def hasNext: Boolean = {
        assert(isSeeked, "must call seek() before use")
        if (it_.isValid) {
          // store the next matching key and increment the iterator
          val key = new String(it_.key(), StandardCharsets.UTF_8)
          if (key.startsWith(strPrefix_)) {
            val node = bytesToCardinalityNode(it_.value())
            val prefix = key.split(KeySeparator).drop(1)
            nextCard_ = CardinalityNode.toCardinality(node, prefix)
            it_.next()
            return true
          }
        }
        it_.close()
        false
      }

      override def next(): Cardinality = nextCard_

      override def close(): Unit = it_.close()
    }
  }

  def close(): Unit = {
    closed = true
    metricsReporter.cancel()
    db.cancelAllBackgroundWork(true)
    db.close()
    writeBufferManager.close()
    cache.close()
    options.close()
    val directory = new Directory(dbDirInTmp)
    directory.deleteRecursively()
  }
}
