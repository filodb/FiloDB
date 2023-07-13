package filodb.core.query

import spire.syntax.cfor._

import filodb.core.metadata.Dataset
import filodb.core.store.{ChunkInfoIterator, ChunkSetInfoReader, ReadablePartition}
import filodb.memory.format.{vectors => bv, RowReader, TypedIterator, UnsafeUtils, ZeroCopyUTF8String}

/**
 * A RowReader iterator which iterates over a time range in the ReadablePartition.  Designed to be relatively memory
 * efficient - thus no per-chunkset data structures.
 * One of these is instantiated for each separate query through each TSPartition.
 * NOTE: this reader assumes that you read consistently from every vector at every row. Or don't read that column.
 */
final class PartitionTimeRangeReader(part: ReadablePartition,
                                     startTime: Long,
                                     endTime: Long,
                                     infos: ChunkInfoIterator,
                                     columnIDs: Array[Int]) extends RangeVectorCursor {
  // MinValue = no current chunk
  private var curChunkID = Long.MinValue
  private final val vectorIts = new Array[TypedIterator](columnIDs.size)
  private var rowNo = -1
  private var endRowNo = -1
  private final val timestampCol = 0

  private val rowReader = new RowReader {
    // TODO: fix this for blobs/UTF8 strings?
    def notNull(columnNo: Int): Boolean = columnNo < columnIDs.size   // time series data, never null
    def getBoolean(columnNo: Int): Boolean = ???
    def getInt(columnNo: Int): Int = vectorIts(columnNo).asIntIt.next
    def getLong(columnNo: Int): Long = vectorIts(columnNo).asLongIt.next
    def getDouble(columnNo: Int): Double = vectorIts(columnNo).asDoubleIt.next
    def getFloat(columnNo: Int): Float = ???
    def getString(columnNo: Int): String = ???
    override def getHistogram(columnNo: Int): bv.Histogram = vectorIts(columnNo).asHistIt.next
    def getAny(columnNo: Int): Any = ???

    override def filoUTF8String(columnNo: Int): ZeroCopyUTF8String = vectorIts(columnNo).asUTF8It.next

    override def getBlobBase(columnNo: Int): Any = ???
    override def getBlobOffset(columnNo: Int): Long = ???
    override def getBlobNumBytes(columnNo: Int): Int = ???
  }

  private def populateIterators(info: ChunkSetInfoReader): Unit = {
    setChunkStartEnd(info)
    cforRange { 0 until columnIDs.size } { pos =>
      val colID = columnIDs(pos)
      if (Dataset.isPartitionID(colID)) {
        // Look up the TypedIterator for that partition key
        vectorIts(pos) = part.schema.partColIterator(colID, part.partKeyBase, part.partKeyOffset)
      } else {
        val vectorAcc = info.vectorAccessor(colID)
        val vectorPtr = info.vectorAddress(colID)
        require(vectorPtr != UnsafeUtils.ZeroPointer, s"Column ID $colID is NULL")
        val reader    = part.chunkReader(colID, vectorAcc, vectorPtr)
        vectorIts(pos) = reader.iterate(vectorAcc, vectorPtr, rowNo)
      }
    }
  }

  private def setChunkStartEnd(info: ChunkSetInfoReader): Unit = {
    // Get reader for timestamp vector
    val timeVector = info.vectorAddress(timestampCol)
    val timeAcc = info.vectorAccessor(timestampCol)
    require(timeVector != UnsafeUtils.ZeroPointer, s"NULL timeVector - did you read the timestamp column?")
    val timeReader = part.chunkReader(timestampCol, timeAcc, timeVector).asLongReader

    // info intersection, compare start and end, do binary search if needed
    rowNo = if (startTime <= info.startTime) 0
            else timeReader.binarySearch(timeAcc, timeVector, startTime) & 0x7fffffff
    endRowNo = if (endTime >= info.endTime) {
                 info.numRows - 1
               } else {
                 timeReader.ceilingIndex(timeAcc, timeVector, endTime)
               }
  }

  final def hasNext: Boolean = {
    try {
      // Fetch the next chunk if no chunk yet, or we're at end of current chunk
      while (curChunkID == Long.MinValue || rowNo > endRowNo) {
        // No more chunksets
        if (!infos.hasNext) return false
        val nextInfo = infos.nextInfoReader
        curChunkID = nextInfo.id
        populateIterators(nextInfo)
      }
      true
    } catch {
      case e: Throwable => infos.close(); throw e;
    }
  }

  final def next: RowReader = {
    rowNo += 1
    rowReader
  }

  final def close(): Unit = {
    infos.close()
  }
}
