package filodb.core.memstore

import java.io.File
import java.nio.file.Files
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import debox.Buffer
import org.apache.commons.lang3.SystemUtils
import org.apache.lucene.util.BytesRef
import spire.implicits.cforRange

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore.PartKeyIndexRaw.bytesRefToUnsafeOffset
import filodb.core.metadata.{PartitionSchema, Schemas}
import filodb.core.metadata.Column.ColumnType.{MapColumn, StringColumn}
import filodb.core.query.ColumnFilter
import filodb.memory.format.UnsafeUtils

class PartKeyTantivyIndex(ref: DatasetRef,
                          schema: PartitionSchema,
                          shardNum: Int,
                          retentionMillis: Long, // only used to calculate fallback startTime
                          diskLocation: Option[File] = None,
                          lifecycleManager: Option[IndexMetadataStore] = None
                         ) extends PartKeyIndexRaw(ref, shardNum, schema, diskLocation, lifecycleManager) {

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
    schemaFields, schemaMapFields, schemaMultiColumnFacets))

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

    flushThreadPool.scheduleAtFixedRate(() => refreshReadersBlocking(), flushDelayMinSeconds,
      flushDelayMinSeconds, TimeUnit.SECONDS)
  }

  override def partIdsEndedBefore(endedBefore: Long): Buffer[Int] = {
    ???
  }

  override def removePartitionsEndedBefore(endedBefore: Long, returnApproxDeletedCount: Boolean): Int = {
    TantivyNativeMethods.removePartitionsEndedBefore(indexHandle, endedBefore, returnApproxDeletedCount)
  }

  override def removePartKeys(partIds: Buffer[Int]): Unit = {
    if (!partIds.isEmpty) {
      TantivyNativeMethods.removePartKeys(indexHandle, partIds.toArray)
    }
  }

  override def indexRamBytes: Long = {
    ???
  }

  override def indexNumEntries: Long = {
    ???
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
    ???
  }

  override def indexValues(fieldName: String, topK: Int): Seq[TermInfo] = {
    ???
  }

  override def labelNamesEfficient(colFilters: Seq[ColumnFilter], startTime: Long, endTime: Long): Seq[String] = {
    ???
  }

  override def labelValuesEfficient(colFilters: Seq[ColumnFilter], startTime: Long, endTime: Long,
                                    colName: String, limit: Int): Seq[String] = {
    ???
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
    ???
  }

  private val NOT_FOUND = -1

  override def startTimeFromPartId(partId: Int): Long = {
    ???
  }

  override def endTimeFromPartId(partId: Int): Long = {
    ???
  }

  override def startTimeFromPartIds(partIds: Iterator[Int]): debox.Map[Int, Long] = {
    ???
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
    ???
  }

  override def partIdsFromFilters(columnFilters: Seq[ColumnFilter], startTime: Long, endTime: Long,
                                  limit: Int): Buffer[Int] = {
    ???
  }

  override def partKeyRecordsFromFilters(columnFilters: Seq[ColumnFilter], startTime: Long, endTime: Long,
                                         limit: Int): Seq[PartKeyLuceneIndexRecord] = {
    ???
  }

  override def partIdFromPartKeySlow(partKeyBase: Any, partKeyOffset: Long): Option[Int] = {
    ???
  }

  override def singlePartKeyFromFilters(columnFilters: Seq[ColumnFilter], startTime: Long,
                                        endTime: Long): Option[Array[Byte]] = {
    ???
  }

  override protected def addIndexedField(key: String, value: String): Unit = {
    val buffer = docBufferLocal.get()

    // 1 - indexed field
    buffer += 1
    TantivyQueryBuilder.writeStringToBuffer(key, buffer)
    TantivyQueryBuilder.writeStringToBuffer(value, buffer)
  }

  protected def addIndexedMapField(mapColumn: String, key: String, value: String): Unit = {
    val buffer = docBufferLocal.get()

    // 2 - map field
    buffer += 2
    TantivyQueryBuilder.writeStringToBuffer(mapColumn, buffer)
    TantivyQueryBuilder.writeStringToBuffer(key, buffer)
    TantivyQueryBuilder.writeStringToBuffer(value, buffer)
  }

  protected override def addMultiColumnFacet(key: String, value: String): Unit = {
    val buffer = docBufferLocal.get()

    // 3 - mc field
    buffer += 3
    TantivyQueryBuilder.writeStringToBuffer(key, buffer)
    TantivyQueryBuilder.writeStringToBuffer(value, buffer)
  }

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
    addIndexedField(Schemas.TypeLabel, schemaName)

    cforRange {
      0 until numPartColumns
    } { i =>
      indexers(i).fromPartKey(partKeyOnHeapBytes, bytesRefToUnsafeOffset(partKeyBytesRefOffset), partId)
    }

    TantivyNativeMethods.ingestDocument(indexHandle, partKeyOnHeapBytes, partKeyBytesRefOffset, partKeyNumBytes,
      partId, documentId, startTime, endTime, docBufferLocal.get().toArray, upsert)
  }
}

object TantivyQueryBuilder {
  def writeStringToBuffer(s: String, buffer: ArrayBuffer[Byte]): Unit = {
    val bytes = s.getBytes
    writeLengthToBuffer(bytes.length, buffer)
    buffer ++= bytes
  }

  private def writeLengthToBuffer(len: Int, buffer: ArrayBuffer[Byte]): Unit = {
    buffer += len.toByte
    buffer += (len >> 8).toByte
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
                     schemaMapFields: Array[String], schemaMultiColumnFacets: Array[String]): Long

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

  // Remove docs with given part keys
  @native
  def removePartKeys(handle: Long, keys: Array[Int]): Unit

  // Remove partition IDs and return approximate deleted count
  @native
  def removePartitionsEndedBefore(handle: Long, endedBefore: Long, returnApproxDeletedCount: Boolean): Int
}