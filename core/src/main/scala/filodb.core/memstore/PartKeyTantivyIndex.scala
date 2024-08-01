package filodb.core.memstore

import java.io.File
import java.nio.file.Files

import debox.Buffer
import org.apache.commons.lang3.SystemUtils
import org.apache.lucene.util.BytesRef

import filodb.core.DatasetRef
import filodb.core.metadata.Column.ColumnType.{MapColumn, StringColumn}
import filodb.core.metadata.PartitionSchema
import filodb.core.query.ColumnFilter

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

  override def startFlushThread(flushDelayMinSeconds: Int, flushDelayMaxSeconds: Int): Unit = {
    ???
  }

  override def partIdsEndedBefore(endedBefore: Long): Buffer[Int] = {
    ???
  }

  override def removePartitionsEndedBefore(endedBefore: Long, returnApproxDeletedCount: Boolean): Int = {
    ???
  }

  override def removePartKeys(partIds: Buffer[Int]): Unit = {
    ???
  }

  override def indexRamBytes: Long = {
    ???
  }

  override def indexNumEntries: Long = {
    ???
  }

  override def closeIndex(): Unit = {
    logger.info(s"Closing index on dataset=$ref shard=$shardNum")

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
    ???
  }

  override def upsertPartKey(partKeyOnHeapBytes: Array[Byte], partId: Int, startTime: Long, endTime: Long,
                             partKeyBytesRefOffset: Int)(partKeyNumBytes: Int, documentId: String): Unit = {
    ???
  }

  override def partKeyFromPartId(partId: Int): Option[BytesRef] = {
    ???
  }

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
    ???
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
    ???
  }

  protected def addIndexedMapField(mapColumn: String, key: String, value: String): Unit = {
    ???
  }

  protected override def addMultiColumnFacet(key: String, value: String): Unit = {
    ???
  }
}

// JNI methods
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

  // Reset index data (delete all docs)
  @native
  def reset(handle: Long): Unit

  // Commit changes to the index
  @native
  def commit(handle: Long): Unit
}