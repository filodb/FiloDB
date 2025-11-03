package filodb.core.memstore

import java.io.File
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import com.typesafe.scalalogging.StrictLogging
import debox.Buffer
import kamon.Kamon
import org.apache.commons.lang3.SystemUtils
import org.apache.lucene.util.BytesRef
import spire.implicits.cforRange

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore.PartKeyIndexRaw.{bytesRefToUnsafeOffset, ignoreIndexNames, FACET_FIELD_PREFIX,
  PART_ID_FIELD}
import filodb.core.metadata.{PartitionSchema, Schemas}
import filodb.core.metadata.Column.ColumnType.{MapColumn, StringColumn}
import filodb.core.metrics.FilodbMetrics
import filodb.core.query.{ColumnFilter, Filter}
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}

object PartKeyTantivyIndex {
  def startMemoryProfiling(): Unit = {
    TantivyNativeMethods.startMemoryProfiling()
  }

  def stopMemoryProfiling(): Unit = {
    TantivyNativeMethods.stopMemoryProfiling()
  }
}

class PartKeyTantivyIndex(ref: DatasetRef,
                          schema: PartitionSchema,
                          shardNum: Int,
                          retentionMillis: Long, // only used to calculate fallback startTime
                          diskLocation: Option[File] = None,
                          lifecycleManager: Option[IndexMetadataStore] = None,
                          columnCacheCount: Long = 1000,
                          queryCacheMaxSize: Long = 50 * 1000 * 1000,
                          queryCacheEstimatedItemSize: Long = 31250,
                          deletedDocMergeThreshold: Float = 0.1f,
                          addMetricTypeField: Boolean = true
                         ) extends PartKeyIndexRaw(ref, shardNum, schema, diskLocation, lifecycleManager,
                              addMetricTypeField = addMetricTypeField) {

  private val cacheHitRate = FilodbMetrics.gauge("index-tantivy-cache-hit-rate",
    Map("dataset" -> ref.dataset, "shard" -> shardNum.toString))

  private val refreshLatency = FilodbMetrics.timeHistogram("index-tantivy-commit-refresh-latency",
      TimeUnit.NANOSECONDS, Map("dataset" -> ref.dataset, "shard" -> shardNum.toString))

  // Compute field names for native schema code
  private val schemaFields = schema.columns.filter { c =>
    c.columnType == StringColumn
  }.map { c =>
    c.name
  }.toArray

  private val schemaMapFields = schema.columns.filter { c =>
    c.columnType == MapColumn
  }.map { c =>
    c.name
  }.toArray

  private val schemaMultiColumnFacets = schema.options.multiColumnFacets.keys.toArray

  // Native handle for cross JNI operations
  private var indexHandle: Long = loadIndexData(() => TantivyNativeMethods.newIndexHandle(indexDiskLocation.toString,
    schemaFields, schemaMapFields, schemaMultiColumnFacets, columnCacheCount, queryCacheMaxSize,
    queryCacheEstimatedItemSize, deletedDocMergeThreshold))

  logger.info(s"Created tantivy index for dataset=$ref shard=$shardNum at $indexDiskLocation")

  override def reset(): Unit = {
    TantivyNativeMethods.reset(indexHandle)
  }

  private var flushThreadPool: ScheduledThreadPoolExecutor = _

  override def startFlushThread(flushDelayMinSeconds: Int, flushDelayMaxSeconds: Int): Unit = {
    if (flushThreadPool != UnsafeUtils.ZeroPointer) {
      // Already running
      logger.warn("startFlushThread called when already running, ignoring")
      return
    }

    flushThreadPool = new ScheduledThreadPoolExecutor(1)

    flushThreadPool.scheduleAtFixedRate(() => {
      // Commit / refresh
      val start = System.nanoTime()
      refreshReadersBlocking()
      val elapsed = System.nanoTime() - start
      refreshLatency.record(elapsed)

      // Emit cache stats
      val cache_stats = TantivyNativeMethods.getCacheHitRates(indexHandle)

      cacheHitRate.update(cache_stats(0), Map("label" -> "query"))
      cacheHitRate.update(cache_stats(1), Map("label" -> "column"))
    }, flushDelayMinSeconds,
      flushDelayMinSeconds, TimeUnit.SECONDS)
  }

  override def startStatsThread(): Unit = {
    // Tantivy already publishes stats in the flush thread, no separate stats thread needed
    logger.debug(s"Tantivy index stats are published via flush thread for dataset=${ref.dataset} shard=$shardNum")
  }

  override def partIdsEndedBefore(endedBefore: Long): Buffer[Int] = {
    val result: debox.Buffer[Int] = debox.Buffer.empty[Int]
    val partIds = TantivyNativeMethods.partIdsEndedBefore(indexHandle, endedBefore)

    result.extend(partIds)

    result
  }

  override def removePartitionsEndedBefore(endedBefore: Long, returnApproxDeletedCount: Boolean): Int = {
    TantivyNativeMethods.removePartitionsEndedBefore(indexHandle, endedBefore, returnApproxDeletedCount)
  }

  override def removePartKeys(partIds: Buffer[Int]): Unit = {
    if (!partIds.isEmpty) {
      TantivyNativeMethods.removePartKeys(indexHandle, partIds.toArray())
    }
  }

  override def indexRamBytes: Long = {
    TantivyNativeMethods.indexRamBytes(indexHandle)
  }

  override def indexNumEntries: Long = {
    TantivyNativeMethods.indexNumEntries(indexHandle)
  }

  override def indexMmapBytes: Long = {
    TantivyNativeMethods.indexMmapBytes(indexHandle)
  }

  override def closeIndex(): Unit = {
    logger.info(s"Closing index on dataset=$ref shard=$shardNum")

    if (flushThreadPool != UnsafeUtils.ZeroPointer) {
      flushThreadPool.shutdown()
      flushThreadPool.awaitTermination(60, TimeUnit.SECONDS)
    }

    commit()
    TantivyNativeMethods.freeIndexHandle(indexHandle)
    indexHandle = 0
  }

  override def indexNames(limit: Int): Seq[String] = {
    decodeStringArray(TantivyNativeMethods.indexNames(indexHandle)).filterNot {
      n => ignoreIndexNames.contains(n) || n.startsWith(FACET_FIELD_PREFIX)
    }
  }

  override def indexValues(fieldName: String, topK: Int): Seq[TermInfo] = {
    val results = TantivyNativeMethods.indexValues(indexHandle, fieldName, topK)

    val buffer = ByteBuffer.wrap(results)
    buffer.order(ByteOrder.LITTLE_ENDIAN)

    val parsedResults = new ArrayBuffer[TermInfo]()

    while (buffer.hasRemaining) {
      val count = buffer.getLong
      val strLen = buffer.getInt
      val strBytes = new Array[Byte](strLen)
      buffer.get(strBytes)

      parsedResults += TermInfo(ZeroCopyUTF8String.apply(strBytes), count.toInt)
    }

    parsedResults.toSeq
  }

  private def decodeStringArray(arr: Array[Byte]): Seq[String] = {
    val buffer = ByteBuffer.wrap(arr)
    buffer.order(ByteOrder.LITTLE_ENDIAN)

    val parsedResults = new ArrayBuffer[String]()

    while (buffer.hasRemaining) {
      val strLen = buffer.getInt
      val strBytes = new Array[Byte](strLen)
      buffer.get(strBytes)

      parsedResults += new String(strBytes, StandardCharsets.UTF_8)
    }

    parsedResults.toSeq
  }

  override def labelNamesEfficient(colFilters: Seq[ColumnFilter], startTime: Long, endTime: Long): Seq[String] = {
    val start = System.nanoTime()
    val queryBuilder = new TantivyQueryBuilder()
    val query = queryBuilder.buildQuery(colFilters)

    val results = TantivyNativeMethods.labelNames(indexHandle, query, LABEL_NAMES_AND_VALUES_DEFAULT_LIMIT,
      startTime, endTime)

    labelValuesQueryLatency.record(System.nanoTime() - start)

    decodeStringArray(results)
  }

  override def labelValuesEfficient(colFilters: Seq[ColumnFilter], startTime: Long, endTime: Long,
                                    colName: String, limit: Int): Seq[String] = {
    val start = System.nanoTime()
    val queryBuilder = new TantivyQueryBuilder()
    val query = queryBuilder.buildQuery(colFilters)

    val results = TantivyNativeMethods.labelValues(indexHandle, query, colName, limit, startTime, endTime)

    labelValuesQueryLatency.record(System.nanoTime() - start)

    decodeStringArray(results)
  }

  override def addPartKey(partKeyOnHeapBytes: Array[Byte], partId: Int, startTime: Long, endTime: Long,
                          partKeyBytesRefOffset: Int)(partKeyNumBytes: Int, documentId: String): Unit = {
    logger.debug(s"Adding document ${partKeyString(documentId, partKeyOnHeapBytes, partKeyBytesRefOffset)} " +
      s"with startTime=$startTime endTime=$endTime into dataset=$ref shard=$shardNum")
    makeDocument(partKeyOnHeapBytes, partKeyBytesRefOffset, partKeyNumBytes, partId, documentId, startTime, endTime,
      upsert = false)
  }

  override def upsertPartKey(partKeyOnHeapBytes: Array[Byte], partId: Int, startTime: Long, endTime: Long,
                             partKeyBytesRefOffset: Int)(partKeyNumBytes: Int, documentId: String): Unit = {
    logger.debug(s"Upserting document ${partKeyString(documentId, partKeyOnHeapBytes, partKeyBytesRefOffset)} " +
      s"with startTime=$startTime endTime=$endTime into dataset=$ref shard=$shardNum")
    makeDocument(partKeyOnHeapBytes, partKeyBytesRefOffset, partKeyNumBytes, partId, documentId, startTime, endTime,
      upsert = true)
  }

  override def partKeyFromPartId(partId: Int): Option[BytesRef] = {
    val results = searchFromFilters(Seq(ColumnFilter(PART_ID_FIELD, Filter.Equals(partId.toString))),
      0, Long.MaxValue, 1, TantivyNativeMethods.queryPartKey)

    if (results == null) {
      None
    } else {
      Some(new BytesRef(results, 0, results.length))
    }
  }

  private val NOT_FOUND = -1

  override def startTimeFromPartId(partId: Int): Long = {
    val rawResult = TantivyNativeMethods.startTimeFromPartIds(indexHandle, Seq(partId).toArray)

    if (rawResult.length == 0) {
      NOT_FOUND
    } else {
      rawResult(1)
    }
  }

  override def endTimeFromPartId(partId: Int): Long = {
    TantivyNativeMethods.endTimeFromPartId(indexHandle, partId)
  }

  override def startTimeFromPartIds(partIds: Iterator[Int]): debox.Map[Int, Long] = {
    val startExecute = System.nanoTime()
    val span = Kamon.currentSpan()
    val partIdsArray = partIds.toArray

    val result = debox.Map.empty[Int, Long]
    val rawResult = TantivyNativeMethods.startTimeFromPartIds(indexHandle, partIdsArray)
    var idx = 0
    while (idx < rawResult.length) {
      result.update(rawResult(idx).toInt, rawResult(idx + 1))
      idx += 2
    }

    span.tag(s"num-partitions-to-page", partIdsArray.length)
    val latency = System.nanoTime - startExecute
    span.mark(s"index-startTimes-for-odp-lookup-latency=${latency}ns")
    startTimeLookupLatency.record(latency)

    result
  }

  override def commit(): Unit = {
    TantivyNativeMethods.commit(indexHandle)
  }

  override def updatePartKeyWithEndTime(partKeyOnHeapBytes: Array[Byte], partId: Int, endTime: Long,
                                        partKeyBytesRefOffset: Int)(partKeyNumBytes: Int, documentId: String): Unit = {
    var startTime = startTimeFromPartId(partId) // look up index for old start time
    if (startTime == NOT_FOUND) {
      startTime = System.currentTimeMillis() - retentionMillis
      logger.warn(s"Could not find in Lucene startTime for partId=$partId in dataset=$ref. Using " +
        s"$startTime instead.", new IllegalStateException()) // assume this time series started retention period ago
    }
    logger.debug(s"Updating document ${partKeyString(documentId, partKeyOnHeapBytes, partKeyBytesRefOffset)} " +
      s"with startTime=$startTime endTime=$endTime into dataset=$ref shard=$shardNum")

    makeDocument(partKeyOnHeapBytes, partKeyBytesRefOffset, partKeyNumBytes,
      partId, documentId, startTime, endTime, upsert = true)
  }

  override def refreshReadersBlocking(): Unit = {
    TantivyNativeMethods.refreshReaders(indexHandle)
  }

  private def searchFromFilters[T](columnFilters: Seq[ColumnFilter], startTime: Long, endTime: Long,
                                   limit: Int,
                                   searchFunc: (Long, Array[Byte], Long, Long, Long) => Array[T]): Array[T] = {
    val startExecute = System.nanoTime()
    val span = Kamon.currentSpan()
    val queryBuilder = new TantivyQueryBuilder()
    val query = queryBuilder.buildQuery(columnFilters)
    val results = searchFunc(indexHandle, query, limit, startTime, endTime)
    val latency = System.nanoTime - startExecute
    span.mark(s"index-partition-lookup-latency=${latency}ns")
    queryIndexLookupLatency.record(latency)

    results
  }

  override def partIdsFromFilters(columnFilters: Seq[ColumnFilter], startTime: Long, endTime: Long,
                                  limit: Int): Buffer[Int] = {
    val results = searchFromFilters(columnFilters, startTime, endTime, limit, TantivyNativeMethods.queryPartIds)

    // "unsafe" means you must not modify the array you're passing in after creating the buffer
    // We don't, so this is more performant
    debox.Buffer.unsafe(results)
  }

  override def partKeyRecordsFromFilters(columnFilters: Seq[ColumnFilter], startTime: Long, endTime: Long,
                                         limit: Int): Seq[PartKeyLuceneIndexRecord] = {
    val results = searchFromFilters(columnFilters, startTime, endTime, limit, TantivyNativeMethods.queryPartKeyRecords)

    val buffer = ByteBuffer.wrap(results)
    buffer.order(ByteOrder.LITTLE_ENDIAN)

    val parsedResults = new ArrayBuffer[PartKeyLuceneIndexRecord]()

    while (buffer.hasRemaining) {
      val start = buffer.getLong
      val end = buffer.getLong
      val pkLen = buffer.getInt
      val pk = new Array[Byte](pkLen)
      buffer.get(pk)

      parsedResults += PartKeyLuceneIndexRecord(pk, start, end)
    }

    parsedResults.toSeq
  }

  override def partIdFromPartKeySlow(partKeyBase: Any, partKeyOffset: Long): Option[Int] = {
    val partKey = schema.binSchema.asByteArray(partKeyBase, partKeyOffset)
    val startExecute = System.nanoTime()

    val id = TantivyNativeMethods.partIdFromPartKey(indexHandle, partKey)

    partIdFromPartKeyLookupLatency.record(System.nanoTime - startExecute)

    if (id == -1) {
      None
    } else {
      Some(id)
    }
  }

  override def singlePartKeyFromFilters(columnFilters: Seq[ColumnFilter], startTime: Long,
                                        endTime: Long): Option[Array[Byte]] = {
    val results = searchFromFilters(columnFilters, 0, Long.MaxValue, 1, TantivyNativeMethods.queryPartKey)

    Option(results)
  }

  override protected def addIndexedField(key: String, value: String): Unit = {
    val buffer = docBufferLocal.get()

    // 1 - indexed field
    buffer += 1
    ByteBufferEncodingUtils.writeStringToBuffer(key, buffer)
    ByteBufferEncodingUtils.writeStringToBuffer(value, buffer)
  }

  protected def addIndexedMapField(mapColumn: String, key: String, value: String): Unit = {
    val buffer = docBufferLocal.get()

    // 2 - map field
    buffer += 2
    ByteBufferEncodingUtils.writeStringToBuffer(mapColumn, buffer)
    ByteBufferEncodingUtils.writeStringToBuffer(key, buffer)
    ByteBufferEncodingUtils.writeStringToBuffer(value, buffer)
  }

  protected override def addMultiColumnFacet(key: String, value: String): Unit = {
    val buffer = docBufferLocal.get()

    // 3 - mc field
    buffer += 3
    ByteBufferEncodingUtils.writeStringToBuffer(key, buffer)
    ByteBufferEncodingUtils.writeStringToBuffer(value, buffer)
  }

  // Ideally this would be a map of field -> value or something similar.
  // However, passing a Map to the Rust code generates a much more expensive
  // back and forth between JVM code and Rust code to get data.
  //
  // To solve this efficiency problem we pack into a byte buffer with a simple
  // serialization format that the Rust side can decode quickly without JVM
  // callbacks.
  private val docBufferLocal = new ThreadLocal[ArrayBuffer[Byte]]() {
    override def initialValue(): ArrayBuffer[Byte] = new ArrayBuffer[Byte](4096)
  }

  private def makeDocument(partKeyOnHeapBytes: Array[Byte],
                           partKeyBytesRefOffset: Int,
                           partKeyNumBytes: Int,
                           partId: Int,
                           documentId: String,
                           startTime: Long,
                           endTime: Long,
                           upsert: Boolean): Unit = {
    docBufferLocal.get().clear()

    // If configured and enabled, Multi-column facets will be created on "partition-schema" columns
    createMultiColumnFacets(partKeyOnHeapBytes, partKeyBytesRefOffset)

    val schemaName = Schemas.global.schemaName(RecordSchema.schemaID(partKeyOnHeapBytes, UnsafeUtils.arayOffset))
    if (addMetricTypeField)
      addIndexedField(Schemas.TypeLabel, schemaName)

    cforRange {
      0 until numPartColumns
    } { i =>
      indexers(i).fromPartKey(partKeyOnHeapBytes, bytesRefToUnsafeOffset(partKeyBytesRefOffset), partId)
    }

    TantivyNativeMethods.ingestDocument(indexHandle, partKeyOnHeapBytes, partKeyBytesRefOffset, partKeyNumBytes,
      partId, documentId, startTime, endTime, docBufferLocal.get().toArray, upsert)
  }

  def dumpCacheStats(): String = {
    TantivyNativeMethods.dumpCacheStats(indexHandle)
  }
}

object ByteBufferEncodingUtils {
  def writeStringToBuffer(s: String, buffer: ArrayBuffer[Byte]): Unit = {
    val bytes = s.getBytes
    writeLengthToBuffer(bytes.length, buffer)
    buffer ++= bytes
  }

  def writeLengthToBuffer(len: Int, buffer: ArrayBuffer[Byte]): Unit = {
    buffer += len.toByte
    buffer += (len >> 8).toByte
  }
}

object TantivyQueryBuilder {
  private val bufferLocal = new ThreadLocal[ArrayBuffer[Byte]]() {
    override def initialValue(): ArrayBuffer[Byte] = new ArrayBuffer[Byte](4096)
  }
}

class TantivyQueryBuilder extends PartKeyQueryBuilder with StrictLogging {
  private final val TERMINATOR_BYTE: Byte = 0
  private final val BOOLEAN_TYPE_BYTE: Byte = 1
  private final val EQUALS_TYPE_BYTE: Byte = 2
  private final val REGEX_TYPE_BYTE: Byte = 3
  private final val TERM_IN_TYPE_BYTE: Byte = 4
  private final val PREFIX_TYPE_BYTE: Byte = 5
  private final val MATCH_ALL_TYPE_BYTE: Byte = 6
  private final val LONG_RANGE_TYPE_BYTE: Byte = 7

  private final val OCCUR_MUST: Byte = 1
  private final val OCCUR_MUST_NOT: Byte = 2

  private val buffer = {
    val buffer = TantivyQueryBuilder.bufferLocal.get()
    buffer.clear()

    buffer
  }

  private def writeString(s: String): Unit = {
    ByteBufferEncodingUtils.writeStringToBuffer(s, buffer)
  }

  private def writeLong(v: Long): Unit = {
    buffer += v.toByte
    buffer += (v >> 8).toByte
    buffer += (v >> 16).toByte
    buffer += (v >> 24).toByte
    buffer += (v >> 32).toByte
    buffer += (v >> 40).toByte
    buffer += (v >> 48).toByte
    buffer += (v >> 56).toByte
  }

  private def writeOccur(occur: PartKeyQueryOccur): Unit = {
    occur match {
      case OccurMust => buffer += OCCUR_MUST
      case OccurMustNot => buffer += OCCUR_MUST_NOT
    }
  }

  override protected def visitStartBooleanQuery(): Unit = {
    if (buffer.nonEmpty) {
      // Nested, add occur byte
      buffer += OCCUR_MUST
    }
    buffer += BOOLEAN_TYPE_BYTE
  }

  override protected def visitEndBooleanQuery(): Unit = {
    buffer += TERMINATOR_BYTE
  }

  override protected def visitEqualsQuery(column: String, term: String, occur: PartKeyQueryOccur): Unit = {
    writeOccur(occur)

    // Type byte, col len, col, term len, term
    buffer += EQUALS_TYPE_BYTE
    writeString(column)
    writeString(term)
  }

  override protected def visitRegexQuery(column: String, pattern: String, occur: PartKeyQueryOccur): Unit = {
    writeOccur(occur)

    // Type byte, col len, col, pattern len, pattern
    buffer += REGEX_TYPE_BYTE
    writeString(column)
    writeString(pattern)
  }

  override protected def visitTermInQuery(column: String, terms: Seq[String], occur: PartKeyQueryOccur): Unit = {
    writeOccur(occur)

    // Type byte, column len, column bytes, (for each term -> term len, term), 0 length
    buffer += TERM_IN_TYPE_BYTE
    writeString(column)

    ByteBufferEncodingUtils.writeLengthToBuffer(terms.length, buffer)
    for (term <- terms) {
      writeString(term)
    }
  }

  override protected def visitPrefixQuery(column: String, prefix: String, occur: PartKeyQueryOccur): Unit = {
    writeOccur(occur)

    // Type byte, col len, col, prefix len, prefix
    buffer += PREFIX_TYPE_BYTE
    writeString(column)
    writeString(prefix)
  }

  override protected def visitMatchAllQuery(): Unit = {
    buffer += OCCUR_MUST
    buffer += MATCH_ALL_TYPE_BYTE
  }

  override protected def visitRangeQuery(column: String, start: Long, end: Long, occur: PartKeyQueryOccur): Unit = {
    writeOccur(occur)

    // Type byte, col len, col, start, end
    buffer += LONG_RANGE_TYPE_BYTE
    writeString(column)
    writeLong(start)
    writeLong(end)
  }

  def buildQuery(columnFilters: Seq[ColumnFilter]): Array[Byte] = {
    visitQuery(columnFilters)

    buffer.toArray
  }

  def buildQueryWithStartAndEnd(columnFilters: Seq[ColumnFilter], start: Long, end: Long): Array[Byte] = {
    visitQueryWithStartAndEnd(columnFilters, start, end)

    buffer.toArray
  }
}

// JNI methods
// Thread safety -
// * Index handle creation / cleanup is not thread safe.
// * Other operations are thread safe and may involve an internal mutex
protected object TantivyNativeMethods {
  // Load native library from jar
  private def loadLibrary(): Unit = {
    val tempDir = Files.createTempDirectory("filodb-native-")

    val lib = System.mapLibraryName("filodb_core")

    val arch = SystemUtils.OS_ARCH
    val kernel = if (SystemUtils.IS_OS_LINUX) {
      "linux"
    } else if (SystemUtils.IS_OS_MAC) {
      "darwin"
    } else if (SystemUtils.IS_OS_WINDOWS) {
      "windows"
    } else {
      sys.error(s"Unhandled platform ${SystemUtils.OS_NAME}")
    }

    val resourcePath: String = "/native/" + kernel + "/" + arch + "/" + lib
    val resourceStream = Option(TantivyNativeMethods.getClass.getResourceAsStream(resourcePath)).get

    val finalPath = tempDir.resolve(lib)
    Files.copy(resourceStream, finalPath)

    System.load(finalPath.toAbsolutePath.toString)
  }

  loadLibrary()

  @native
  def newIndexHandle(diskLocation: String, schemaFields: Array[String],
                     schemaMapFields: Array[String], schemaMultiColumnFacets: Array[String],
                     columnCacheSize: Long, queryCacheMaxSize: Long, queryCacheItemSize: Long,
                     deletedDocMergeThreshold: Float): Long

  // Free memory used by an index handle
  @native
  def freeIndexHandle(handle: Long): Unit

  // Force refresh any readers to be up to date (primarily used by tests)
  @native
  def refreshReaders(handle: Long): Unit

  // Reset index data (delete all docs)
  @native
  def reset(handle: Long): Unit

  // Commit changes to the index
  @native
  def commit(handle: Long): Unit

  // Ingest a new document
  // scalastyle:off parameter.number
  @native
  def ingestDocument(handle: Long, partKeyData: Array[Byte], partKeyOffset: Int,
                        partKeyNumBytes: Int, partId: Int, documentId: String,
                        startTime: Long, endTime: Long, fields: Array[Byte],
                        upsert: Boolean): Unit
  // scalastyle:on parameter.number

  // Get the estimated amount of RAM being used by this index
  @native
  def indexRamBytes(handle: Long): Long

  // Get the estimated amount of Mmap space being used by this index
  @native
  def indexMmapBytes(handle: Long): Long

  // Get the number of entries (docs) in the index
  @native
  def indexNumEntries(handle: Long): Long

  // Get part IDs that ended before a given time
  @native
  def partIdsEndedBefore(handle: Long, endedBefore: Long): Array[Int]

  // Remove docs with given part keys
  @native
  def removePartKeys(handle: Long, keys: Array[Int]): Unit

  // Get the list of unique indexed field names
  @native
  def indexNames(handle: Long): Array[Byte]

  // Get the list of unique values for a field
  @native
  def indexValues(handle: Long, fieldName: String, topK: Int): Array[Byte]

  // Get the list of unique indexed field names
  @native
  def labelNames(handle: Long, query: Array[Byte], limit: Int, start: Long, end: Long): Array[Byte]

  // Get the list of unique values for a field
  @native
  def labelValues(handle: Long, query: Array[Byte], colName: String, limit: Int, start: Long, end: Long): Array[Byte]

  // Get the list of part IDs given a query
  @native
  def queryPartIds(handle: Long, query: Array[Byte], limit: Long, start: Long, end: Long): Array[Int]

  // Get the list of part IDs given a query
  @native
  def queryPartKeyRecords(handle: Long, query: Array[Byte], limit: Long, start: Long,
                          end: Long): Array[Byte]

  // Get a part key by query
  @native
  def queryPartKey(handle: Long, query: Array[Byte], limit: Long, start: Long, end: Long): Array[Byte]

  /// Get a part ID from a part key
  @native
  def partIdFromPartKey(handle: Long, partKey : Array[Byte]): Int

  // Get map of start times from partition ID list
  @native
  def startTimeFromPartIds(handle: Long, partIds: Array[Int]): Array[Long]

  // Get end time from part ID
  @native
  def endTimeFromPartId(handle: Long, partId: Int): Long

  // Remove partition IDs and return approximate deleted count
  @native
  def removePartitionsEndedBefore(handle: Long, endedBefore: Long, returnApproxDeletedCount: Boolean): Int

  // Get cache hit rates for stats
  // Array of (query cache, column cache)
  @native
  def getCacheHitRates(handle: Long): Array[Double]

  // Dump stats - mainly meant for testing
  @native
  def dumpCacheStats(handle: Long): String

  // Start memory profiling if enabled for this build, or no-op
  @native
  def startMemoryProfiling(): Unit

  // Start memory profiling if enabled for this build, or no-op
  @native
  def stopMemoryProfiling(): Unit
}