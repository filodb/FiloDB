package filodb.core.memstore.ratelimit

import java.io.File
import java.nio.charset.StandardCharsets

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.reflect.io.Directory

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.typesafe.scalalogging.StrictLogging
import monix.reactive.Observable
import org.rocksdb._
import spire.syntax.cfor._

import filodb.core.{DatasetRef, GlobalScheduler}
import filodb.core.memstore.ratelimit.CardinalityStore._
import filodb.core.metrics.FilodbMetrics
import filodb.memory.format.UnsafeUtils

/**
 * Stored as values in the RocksDb database.
 * Identical to CardinalityRecord, except that:
 *   (1) only the least-significant prefix name is stored.
 *   (2) no shard ID is store
 */
case class CardinalityValue(tsCount: Long, activeTsCount: Long,
                            childrenCount: Long, childrenQuota: Long)

case object CardinalityValue {
  def toCardinalityRecord(card: CardinalityValue,
                          prefix: Seq[String],
                          shard: Int): CardinalityRecord = {
    CardinalityRecord(shard, prefix, card)
  }
}

class CardinalityNodeSerializer extends Serializer[CardinalityValue] {
  def write(kryo: Kryo, output: Output, card: CardinalityValue): Unit = {
    output.writeLong(card.tsCount, true)
    output.writeLong(card.activeTsCount, true)
    output.writeLong(card.childrenCount, true)
    output.writeLong(card.childrenQuota, true)
  }

  def read(kryo: Kryo, input: Input, t: Class[CardinalityValue]): CardinalityValue = {
    CardinalityValue(input.readLong(true), input.readLong(true),
                    input.readLong(true), input.readLong(true))
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
      k.addDefaultSerializer(classOf[CardinalityValue], classOf[CardinalityNodeSerializer])
      k
    }
  }

  // ======= Metrics ===========
  private val tags = Map("shard" -> shard.toString, "dataset" -> ref.toString)
  private val diskSpaceUsedMetric = FilodbMetrics.bytesGauge("card-store-disk-space-used",
    tags)
  private val memoryUsedMetric = FilodbMetrics.gauge("card-store-offheap-mem-used",
    tags)
  private val compactionBytesPendingMetric = FilodbMetrics.bytesGauge("card-store-compaction-pending", tags)
  private val numRunningCompactionsMetric = FilodbMetrics.gauge("card-store-num-running-compactions", tags)
  private val numKeysMetric = FilodbMetrics.gauge("card-store-est-num-keys", tags)

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
   *         The result key should be used for a prefix search.
   *   Note: When keyDepth == shardKeyPrefix.size, the final KeySeparator is omitted.
   *         In this case, the result key cannot be used for a prefix search.
   *
   * @return string key to use to perform reads and writes of entries into RocksDB
   */
  private def toStringKey(shardKeyPrefix: Seq[String], keyDepth: Int): String = {
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
   * Builds a fully-specified search key as:
   *   <keyDepth>{KeySeparator}<name-1>{KeySeparator}<name-2> ... <name-n>
   * Keys returned from this function can only be used to lookup a single value.
   * They cannot be used for a prefix search.
   *
   * @param shardKey must be a complete (non-prefix) shard key.
   */
  private def toCompleteStringKey(shardKey: Seq[String]): String = {
    // When the keyDepth argument equals shardKey.size, the final {KeySeparator}
    //   normally attached to a prefix key is omitted.
    toStringKey(shardKey, shardKey.size)
  }

  private def cardinalityValueToBytes(card: CardinalityValue): Array[Byte] = {
    val out = new Output(500)
    kryo.get().writeObject(out, card)
    out.close()
    out.toBytes
  }

  private def bytesToCardinalityValue(bytes: Array[Byte]): CardinalityValue = {
    val inp = new Input(bytes)
    val c = kryo.get().readObject(inp, classOf[CardinalityValue])
    inp.close()
    c
  }

  override def store(card: CardinalityRecord): Unit = {
    val key = toCompleteStringKey(card.prefix).getBytes(StandardCharsets.UTF_8)
    logger.debug(s"Storing shard=$shard dataset=$ref ${new String(key)} with $card")
    db.put(key, cardinalityValueToBytes(card.value))
  }

  def getOrZero(shardKeyPrefix: Seq[String], zero: CardinalityRecord): CardinalityRecord = {
    val value = db.get(toCompleteStringKey(shardKeyPrefix).getBytes(StandardCharsets.UTF_8))
    if (value == NotFound) zero
    else CardinalityValue.toCardinalityRecord(bytesToCardinalityValue(value), shardKeyPrefix, shard)
  }

  override def remove(shardKeyPrefix: Seq[String]): Unit = {
    db.delete(toCompleteStringKey(shardKeyPrefix).getBytes(StandardCharsets.UTF_8))
  }

  // scalastyle:off method.length
  override def scanChildren(shardKeyPrefix: Seq[String],
                            depth: Int): Seq[CardinalityRecord] = {

    require(depth > shardKeyPrefix.size,
      s"scan depth $depth must be greater than the size of the prefix ${shardKeyPrefix.size}")

    val it = db.newIterator()
    val buf = new ArrayBuffer[CardinalityRecord]()
    try {
      val searchPrefix = toStringKey(shardKeyPrefix, depth)
      logger.debug(s"Scanning shard=$shard dataset=$ref ${new String(searchPrefix)}")
      it.seek(searchPrefix.getBytes(StandardCharsets.UTF_8))
      import scala.util.control.Breaks._

      var complete = false
      breakable {
        while (it.isValid() && (buf.size < MAX_RESULT_SIZE)) {
          val key = new String(it.key(), StandardCharsets.UTF_8)
          if (key.startsWith(searchPrefix)) {
            val node = bytesToCardinalityValue(it.value())
            // Drop the first element, since it's just the size of the prefix.
            // Ex: 2{KeySeparator}A{KeySeparator}B ==(split)==> [2, A, B] ==(drop)==> [A, B]
            val prefix = key.split(KeySeparator).drop(1)
            buf += CardinalityValue.toCardinalityRecord(node, prefix, shard)
          } else {
            // no matching prefixes remain
            complete = true
            break
          }
          it.next()
        }
      }

      // result reached MAX_RESULT_SIZE, but still more cardinalities
      if (it.isValid() && !complete) {
        // sum the remaining counts into these values
        var tsCount, activeTsCount, childrenCount, childrenQuota = 0L
        breakable {
          do {
            // note: the iterator is valid here on the first iteration
            val key = new String(it.key(), StandardCharsets.UTF_8)
            if (key.startsWith(searchPrefix)) {
              val node = bytesToCardinalityValue(it.value())
              tsCount = tsCount + node.tsCount
              activeTsCount = activeTsCount + node.activeTsCount
              childrenCount = childrenCount + node.childrenCount
              childrenQuota = childrenQuota + node.childrenQuota
            } else {
              break  // don't continue beyond valid results
            }
            it.next()
          } while (it.isValid())
        }
        buf.append(CardinalityRecord(shard, OVERFLOW_PREFIX, CardinalityValue(
          tsCount, activeTsCount, childrenCount, childrenQuota)))
      }
    } finally {
      it.close();
    }
    buf
  }
  // scalastyle:on method.length

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
