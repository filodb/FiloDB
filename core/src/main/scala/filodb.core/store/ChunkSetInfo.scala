package filodb.core.store

import java.nio.ByteBuffer

import com.googlecode.javaewah.EWAHCompressedBitmap
import com.typesafe.scalalogging.StrictLogging

import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset}
import filodb.memory.format._

/**
  * A ChunkSet is the set of chunks for all columns, one per column, serialized from a set of rows.
  * Chunk is the unit of encoded data that is stored in memory or in a column store.
  *
  * @param info      records common metadata about a ChunkSet
  * @param partition 64-bit native address of the BinaryRecord partition key
  * @param skips
  * @param chunks    each item in the Seq encodes a column's values in the chunk's dataset. First
  *                  value in the tuple identifies the column, the second is a reference to the
  *                  off-heap memory store where the contents of the chunks can be obtained
  * @param listener a callback for when that chunkset is successfully flushed
  */
case class ChunkSet(info: ChunkSetInfo,
                    partition: PartitionKey,
                    skips: Seq[ChunkRowSkipIndex],
                    chunks: Seq[(ColumnId, ByteBuffer)],
                    listener: ChunkSetInfo => Unit = info => {}) {
  def invokeFlushListener(): Unit = listener(info)
}

object ChunkSet {
  /**
   * Create a ChunkSet out of a set of rows easily.  Mostly for testing.
   * @param rows a RowReader for the data columns only - partition columns at end might be OK
   */
  def apply(dataset: Dataset, part: PartitionKey, rows: Seq[RowReader]): ChunkSet = {
    require(rows.nonEmpty)
    val firstKey = dataset.timestamp(rows.head)
    val info = ChunkSetMeta(timeUUID64, rows.length, firstKey, dataset.timestamp(rows.last))
    val filoSchema = Column.toFiloSchema(dataset.dataColumns)
    val chunkMap = RowToVectorBuilder.buildFromRows(rows.toIterator, filoSchema)
    val idsAndBytes = chunkMap.map { case (colName, buf) => (dataset.colIDs(colName).get.head, buf) }.toSeq
    ChunkSet(info, part, Nil, idsAndBytes)
  }
}

/**
  * Records metadata about a chunk set, including its time range
  */
trait ChunkSetInfo {
  // chunk id (usually a timeuuid)
  def id: ChunkID
  // number of rows encoded by this chunkset
  def numRows: Int
  // the starting timestamp of this chunkset
  def startTime: Long
  // The ending timestamp of this chunkset
  def endTime: Long

  /**
   * Finds intersection key ranges between two ChunkSetInfos.
   * Scenario A:    [       ]
   *                    [ other  ]
   * Scenario B:    [              ]
   *                    [ other ]
   * Scenario C:        [        ]
   *                 [  other ]
   * Scenario D:        [        ]
   *                 [  other      ]
   */
  def intersection(other: ChunkSetInfo): Option[(Long, Long)] =
    try {
      intersection(other.startTime, other.endTime)
    } catch {
      case e: Exception =>
        ChunkSetInfo.log.warn(s"Got error comparing $this and $other...", e)
        None
    }

  /**
   * Finds the intersection between this ChunkSetInfo and a time range (startTime, endTime).
   */
  def intersection(time1: Long, time2: Long): Option[(Long, Long)] = {
    if (time1 > time2) {
      None
    } else if (time1 <= endTime && time2 >= startTime) {
      Some((if (startTime < time1) time1 else startTime,
            if (time2 > endTime) endTime else time2))
    } else {
      None
    }
  }
}

final case class ChunkSetMeta(id: ChunkID, numRows: Int, startTime: Long, endTime: Long) extends ChunkSetInfo

case class ChunkRowSkipIndex(id: ChunkID, overrides: EWAHCompressedBitmap)

object ChunkRowSkipIndex {
  def apply(id: ChunkID, overrides: Array[Int]): ChunkRowSkipIndex =
    ChunkRowSkipIndex(id, EWAHCompressedBitmap.bitmapOf(overrides.sorted: _*))
}

object ChunkSetInfo extends StrictLogging {
  type ChunkSkips = Seq[ChunkRowSkipIndex]
  type SkipMap    = EWAHCompressedBitmap
  type ChunkInfosAndSkips = Seq[(ChunkSetInfo, SkipMap)]
  type InfosSkipsIt = Iterator[(ChunkSetInfo, SkipMap)]

  val emptySkips = new SkipMap()
  val log = logger

  /**
   * Serializes the info in a ChunkSetInfo to a region of memory.
   * 28 bytes (ID + numRows + start/endTimes) is required.
   */
  def toMemRegion(info: ChunkSetInfo, destBase: Any, destOffset: Long): Unit = {
    UnsafeUtils.setLong(destBase, destOffset, info.id)
    UnsafeUtils.setInt(destBase, destOffset + 8, info.numRows)
    UnsafeUtils.setLong(destBase, destOffset + 12, info.startTime)
    UnsafeUtils.setLong(destBase, destOffset + 20, info.endTime)
  }

  def fromMemRegion(base: Any, offset: Long): ChunkSetInfo =
    ChunkSetMeta(UnsafeUtils.getLong(base, offset),
                 UnsafeUtils.getInt(base, offset + 8),
                 UnsafeUtils.getLong(base, offset + 12),
                 UnsafeUtils.getLong(base, offset + 20))

  def toBytes(chunkSetInfo: ChunkSetInfo): Array[Byte] = {
    val bytes = new Array[Byte](28)
    toMemRegion(chunkSetInfo, bytes, UnsafeUtils.arayOffset)
    bytes
  }

  def fromBytes(bytes: Array[Byte]): ChunkSetInfo = fromMemRegion(bytes, UnsafeUtils.arayOffset)
}