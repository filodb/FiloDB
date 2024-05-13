package filodb.core.memstore

import org.apache.lucene.util.BytesRef

import filodb.core.memstore.ratelimit.CardinalityTracker
import filodb.core.metadata.PartitionSchema
import filodb.core.query.ColumnFilter

trait PartKeyIndexRaw {

  /**
   * Clear the index by deleting all documents and commit
   */
  def reset(): Unit

  /**
   * Start the asynchronous thread to automatically flush
   * new writes to readers at the given min and max delays
   */
  def startFlushThread(flushDelayMinSeconds: Int, flushDelayMaxSeconds: Int): Unit

  /**
   * Find partitions that ended ingesting before a given timestamp. Used to identify partitions that can be purged.
   * @return matching partIds
   */
  def partIdsEndedBefore(endedBefore: Long): debox.Buffer[Int]

  /**
   * Method to delete documents from index that ended before the provided end time
   *
   * @param endedBefore the cutoff timestamp. All documents with time <= this time will be removed
   * @param returnApproxDeletedCount a boolean flag that requests the return value to be an approximate count of the
   *                                 documents that got deleted, if value is set to false, 0 is returned
   */
  def removePartitionsEndedBefore(endedBefore: Long, returnApproxDeletedCount: Boolean = true): Int

  /**
   * Delete partitions with given partIds
   */
  def removePartKeys(partIds: debox.Buffer[Int]): Unit

  /**
   * Memory used by index, esp for unflushed data
   */
  def indexRamBytes: Long

  /**
   * Number of documents in flushed index, excludes tombstones for deletes
   */
  def indexNumEntries: Long

  /**
   * Number of documents in flushed index, includes tombstones for deletes
   */
  def indexNumEntriesWithTombstones: Long

  /**
   * Closes the index for read by other clients. Check for implementation if commit would be done
   * automatically.
   */
  def closeIndex(): Unit

  /**
   * Return user field/dimension names in index, except those that are created internally
   */
  def indexNames(limit: Int): Seq[String]

  /**
   * Fetch values/terms for a specific column/key/field, in order from most frequent on down.
   * Note that it iterates through all docs up to a certain limit only, so if there are too many terms
   * it will not report an accurate top k in exchange for not running too long.
   * @param fieldName the name of the column/field/key to get terms for
   * @param topK the number of top k results to fetch
   */
  def indexValues(fieldName: String, topK: Int = 100): Seq[TermInfo]

  /**
   * Use faceting to get field/index names given a column filter and time range
   */
  def labelNamesEfficient(colFilters: Seq[ColumnFilter], startTime: Long, endTime: Long): Seq[String]

  /**
   * Use faceting to get field/index values given a column filter and time range
   */
  def labelValuesEfficient(colFilters: Seq[ColumnFilter], startTime: Long, endTime: Long,
                           colName: String, limit: Int = 100): Seq[String]

  /**
   * Add new part key to index
   */
  def addPartKey(partKeyOnHeapBytes: Array[Byte],
                 partId: Int,
                 startTime: Long,
                 endTime: Long = Long.MaxValue,
                 partKeyBytesRefOffset: Int = 0)
                (partKeyNumBytes: Int = partKeyOnHeapBytes.length,
                 documentId: String = partId.toString): Unit

  /**
   * Update or create part key to index
   */
  def upsertPartKey(partKeyOnHeapBytes: Array[Byte],
                    partId: Int,
                    startTime: Long,
                    endTime: Long = Long.MaxValue,
                    partKeyBytesRefOffset: Int = 0)
                   (partKeyNumBytes: Int = partKeyOnHeapBytes.length,
                    documentId: String = partId.toString): Unit

  /**
   * Called when TSPartition needs to be created when on-demand-paging from a
   * partId that does not exist on heap
   */
  def partKeyFromPartId(partId: Int): Option[BytesRef]

  /**
   * Called when a document is updated with new endTime
   */
  def startTimeFromPartId(partId: Int): Long

  /**
   * Called when a document is updated with new endTime
   */
  def endTimeFromPartId(partId: Int): Long

  /**
   * Called when a document is updated with new endTime
   */
  def startTimeFromPartIds(partIds: Iterator[Int]): debox.Map[Int, Long]

  /**
   * Commit index contents to disk
   */
  def commit(): Long

  /**
   * Update existing part key document with new endTime.
   */
  def updatePartKeyWithEndTime(partKeyOnHeapBytes: Array[Byte],
                               partId: Int,
                               endTime: Long = Long.MaxValue,
                               partKeyBytesRefOffset: Int = 0)
                              (partKeyNumBytes: Int = partKeyOnHeapBytes.length,
                               documentId: String = partId.toString): Unit
  /**
   * Refresh readers with updates to index. May be expensive - use carefully.
   * @return
   */
  def refreshReadersBlocking(): Unit

  /**
   * Fetch list of partIds for given column filters
   */
  def partIdsFromFilters(columnFilters: Seq[ColumnFilter],
                         startTime: Long,
                         endTime: Long,
                         limit: Int = Int.MaxValue): debox.Buffer[Int]

  /**
   * Fetch list of part key records for given column filters
   */
  def partKeyRecordsFromFilters(columnFilters: Seq[ColumnFilter],
                                startTime: Long,
                                endTime: Long,
                                limit: Int = Int.MaxValue): Seq[PartKeyLuceneIndexRecord]

  /**
   * Fetch partId given partKey. This is slower since it would do an index search
   * instead of a key-lookup.
   */

  def partIdFromPartKeySlow(partKeyBase: Any,
                            partKeyOffset: Long): Option[Int]

  /**
   * Fetch one partKey matching filters
   */
  def singlePartKeyFromFilters(columnFilters: Seq[ColumnFilter],
                               startTime: Long,
                               endTime: Long): Option[Array[Byte]]

}


trait PartKeyIndexDownsampled extends PartKeyIndexRaw {

  def getCurrentIndexState(): (IndexState.Value, Option[Long])

  def  notifyLifecycleListener(state: IndexState.Value, time: Long): Unit

  /**
   * Iterate through the LuceneIndex and calculate cardinality count
   */
  def calculateCardinality(partSchema: PartitionSchema, cardTracker: CardinalityTracker): Unit

  /**
   * Run some code for each ingesting partKey which has endTime != Long.MaxValue
   */
  def foreachPartKeyStillIngesting(func: (Int, BytesRef) => Unit): Int

  /**
   * Run some code for each partKey matchin column filter
   */
  def foreachPartKeyMatchingFilter(columnFilters: Seq[ColumnFilter],
                                   startTime: Long,
                                   endTime: Long, func: (BytesRef) => Unit): Int

}
