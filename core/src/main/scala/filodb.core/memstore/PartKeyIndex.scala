package filodb.core.memstore

import filodb.core.query.ColumnFilter
import org.apache.lucene.util.BytesRef

trait PartKeyIndex {

  def reset(): Unit
  def startFlushThread(flushDelayMinSeconds: Int, flushDelayMaxSeconds: Int): Unit
  def partIdsEndedBefore(endedBefore: Long): debox.Buffer[Int]
  def removePartitionsEndedBefore(endedBefore: Long, returnApproxDeletedCount: Boolean = true): Int
  def removePartKeys(partIds: debox.Buffer[Int])
  def indexRamBytes: Long
  def indexNumEntries: Long
  def indexNumEntriesWithTombstones: Long
  def closeIndex(): Unit
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

  def partKeyFromPartId(partId: Int): Option[BytesRef]

  def startTimeFromPartId(partId: Int): Long
  def endTimeFromPartId(partId: Int): Long

  def startTimeFromPartIds(partIds: Iterator[Int]): debox.Map[Int, Long]
  def commit(): Long

  def updatePartKeyWithEndTime(partKeyOnHeapBytes: Array[Byte],
                               partId: Int,
                               endTime: Long = Long.MaxValue,
                               partKeyBytesRefOffset: Int = 0)
                              (partKeyNumBytes: Int = partKeyOnHeapBytes.length,
                               documentId: String = partId.toString): Unit
  def refreshReadersBlocking(): Unit
  def partIdsFromFilters(columnFilters: Seq[ColumnFilter],
                         startTime: Long,
                         endTime: Long,
                         limit: Int = Int.MaxValue): debox.Buffer[Int]

  def foreachPartKeyMatchingFilter(columnFilters: Seq[ColumnFilter],
                                   startTime: Long,
                                   endTime: Long, func: (BytesRef) => Unit): Int

}
