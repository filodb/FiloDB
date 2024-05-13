package filodb.core.memstore

import org.apache.lucene.util.BytesRef

import filodb.core.memstore.ratelimit.CardinalityTracker
import filodb.core.metadata.PartitionSchema
import filodb.core.query.ColumnFilter



trait PartKeyIndexRaw {

  def reset(): Unit
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

  def indexRamBytes: Long

  /**
   * Number of documents in flushed index, excludes tombstones for deletes
   */
  def indexNumEntries: Long

  /**
   * Number of documents in flushed index, includes tombstones for deletes
   */
  def indexNumEntriesWithTombstones: Long

  def closeIndex(): Unit

  def indexNames(limit: Int): Seq[String]

  /**
   * Fetch values/terms for a specific column/key/field, in order from most frequent on down.
   * Note that it iterates through all docs up to a certain limit only, so if there are too many terms
   * it will not report an accurate top k in exchange for not running too long.
   * @param fieldName the name of the column/field/key to get terms for
   * @param topK the number of top k results to fetch
   */
  def indexValues(fieldName: String, topK: Int = 100): Seq[TermInfo]


  def labelNamesEfficient(colFilters: Seq[ColumnFilter], startTime: Long, endTime: Long): Seq[String]
  def labelValuesEfficient(colFilters: Seq[ColumnFilter], startTime: Long, endTime: Long,
                           colName: String, limit: Int = 100): Seq[String]

  def addPartKey(partKeyOnHeapBytes: Array[Byte],
                 partId: Int,
                 startTime: Long,
                 endTime: Long = Long.MaxValue,
                 partKeyBytesRefOffset: Int = 0)
                (partKeyNumBytes: Int = partKeyOnHeapBytes.length,
                 documentId: String = partId.toString): Unit

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
  def commit(): Long

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
  def partIdsFromFilters(columnFilters: Seq[ColumnFilter],
                         startTime: Long,
                         endTime: Long,
                         limit: Int = Int.MaxValue): debox.Buffer[Int]

  def partKeyRecordsFromFilters(columnFilters: Seq[ColumnFilter],
                                startTime: Long,
                                endTime: Long,
                                limit: Int = Int.MaxValue): Seq[PartKeyLuceneIndexRecord]

  def partIdFromPartKeySlow(partKeyBase: Any,
                            partKeyOffset: Long): Option[Int]

}


trait PartKeyIndexDownsampled extends PartKeyIndexRaw {

  def getCurrentIndexState(): (IndexState.Value, Option[Long])

  def  notifyLifecycleListener(state: IndexState.Value, time: Long): Unit

  /**
   * Iterate through the LuceneIndex and calculate cardinality count
   */
  def calculateCardinality(partSchema: PartitionSchema, cardTracker: CardinalityTracker): Unit

  def singlePartKeyFromFilters(columnFilters: Seq[ColumnFilter],
                               startTime: Long,
                               endTime: Long): Option[Array[Byte]]

  def foreachPartKeyStillIngesting(func: (Int, BytesRef) => Unit): Int

  def foreachPartKeyMatchingFilter(columnFilters: Seq[ColumnFilter],
                                   startTime: Long,
                                   endTime: Long, func: (BytesRef) => Unit): Int

}
