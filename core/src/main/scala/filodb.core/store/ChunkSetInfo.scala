package filodb.core.store

import java.nio.ByteBuffer

import com.googlecode.javaewah.EWAHCompressedBitmap
import com.typesafe.scalalogging.StrictLogging

import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset}
import filodb.memory.BinaryRegion.NativePointer
import filodb.memory.MemFactory
import filodb.memory.format._

/**
  * A ChunkSet is the set of chunks for all columns, one per column, serialized from a set of rows.
  * Chunk is the unit of encoded data that is stored in memory or in a column store.
  *
  * @param info      records common metadata about a ChunkSet
  * @param partition 64-bit native address of the BinaryRecord partition key
  * @param skips
  * @param chunks    each item in the Seq is a ByteBuffer for the encoded chunks.  First item = data column 0,
  *                  second item = data column 1, and so forth
  * @param listener a callback for when that chunkset is successfully flushed
  */
case class ChunkSet(info: ChunkSetInfo,
                    partition: PartitionKey,
                    skips: Seq[ChunkRowSkipIndex],
                    chunks: Seq[ByteBuffer],
                    listener: ChunkSetInfo => Unit = info => {}) {
  def invokeFlushListener(): Unit = listener(info)
}

object ChunkSet {
  /**
   * Create a ChunkSet out of a set of rows easily.  Mostly for testing.
   * @param rows a RowReader for the data columns only - partition columns at end might be OK
   */
  def apply(dataset: Dataset, part: PartitionKey, rows: Seq[RowReader], factory: MemFactory): ChunkSet = {
    require(rows.nonEmpty)
    val info = ChunkSetInfo(factory, dataset, timeUUID64, rows.length,
                            dataset.timestamp(rows.head),
                            dataset.timestamp(rows.last))
    val filoSchema = Column.toFiloSchema(dataset.dataColumns)
    val chunkMap = RowToVectorBuilder.buildFromRows(rows.toIterator, filoSchema, factory)
    val chunks = dataset.dataColumns.map(c => chunkMap(c.name))
    ChunkSet(info, part, Nil, chunks)
  }
}

/**
  * Records metadata about a chunk set, including its time range.  Is always offheap.
  */
final case class ChunkSetInfo(infoAddr: NativePointer) extends AnyVal {
  // chunk id (usually a timeuuid)
  def id: ChunkID = ChunkSetInfo.getChunkID(infoAddr)
  // number of rows encoded by this chunkset
  def numRows: Int = ChunkSetInfo.getNumRows(infoAddr)
  // the starting timestamp of this chunkset
  def startTime: Long = ChunkSetInfo.getStartTime(infoAddr)
  // The ending timestamp of this chunkset
  def endTime: Long = ChunkSetInfo.getEndTime(infoAddr)

  /**
   * Returns the vector pointer for a particular column
   */
  def vectorPtr(colNo: Int): BinaryVector.BinaryVectorPtr = ChunkSetInfo.getVectorPtr(infoAddr, colNo)

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
   * ChunkSetInfo metadata schema:
   * +0  long   chunkID
   * +8  int    # rows in chunkset
   * +12 long   start timestamp
   * +20 long   end timestamp
   * +28 long[] pointers to each vector
   *
   * Note that block metadata has a 4-byte partition ID appended to the front also.
   */
  val OffsetChunkID = 0
  val OffsetNumRows = 8
  val OffsetStartTime = 12
  val OffsetEndTime = 20
  val OffsetVectors = 28

  def chunkSetInfoSize(numDataColumns: Int): Int = OffsetVectors + 8 * numDataColumns
  def blockMetaInfoSize(numDataColumns: Int): Int = chunkSetInfoSize(numDataColumns) + 4

  def getChunkID(infoPointer: NativePointer): ChunkID = UnsafeUtils.getLong(infoPointer + OffsetChunkID)
  def getChunkID(infoBytes: Array[Byte]): ChunkID =
    UnsafeUtils.getLong(infoBytes, UnsafeUtils.arayOffset + OffsetChunkID)
  def setChunkID(infoPointer: NativePointer, newId: ChunkID): Unit =
    UnsafeUtils.setLong(infoPointer + OffsetChunkID, newId)

  def getNumRows(infoPointer: NativePointer): Int = UnsafeUtils.getInt(infoPointer + OffsetNumRows)
  def resetNumRows(infoPointer: NativePointer): Unit = UnsafeUtils.setInt(infoPointer + OffsetNumRows, 0)
  def incrNumRows(infoPointer: NativePointer): Unit =
    UnsafeUtils.unsafe.getAndAddInt(UnsafeUtils.ZeroPointer, infoPointer + OffsetNumRows, 1)

  def getStartTime(infoPointer: NativePointer): Long = UnsafeUtils.getLong(infoPointer + OffsetStartTime)
  def getEndTime(infoPointer: NativePointer): Long = UnsafeUtils.getLong(infoPointer + OffsetEndTime)
  def setStartTime(infoPointer: NativePointer, time: Long): Unit =
    UnsafeUtils.setLong(infoPointer + OffsetStartTime, time)
  def setEndTime(infoPointer: NativePointer, time: Long): Unit =
    UnsafeUtils.setLong(infoPointer + OffsetEndTime, time)

  def getStartTime(infoBytes: Array[Byte]): Long =
    UnsafeUtils.getLong(infoBytes, UnsafeUtils.arayOffset + OffsetStartTime)
  def getEndTime(infoBytes: Array[Byte]): Long =
    UnsafeUtils.getLong(infoBytes, UnsafeUtils.arayOffset + OffsetEndTime)

  def getVectorPtr(infoPointer: NativePointer, colNo: Int): BinaryVector.BinaryVectorPtr =
    UnsafeUtils.getLong(infoPointer + OffsetVectors + 8 * colNo)
  def setVectorPtr(infoPointer: NativePointer, colNo: Int, vector: BinaryVector.BinaryVectorPtr): Unit =
    UnsafeUtils.setLong(infoPointer + OffsetVectors + 8 * colNo, vector)

  /**
   * Copies the non-vector-pointer portion of ChunkSetInfo
   */
  def copy(orig: ChunkSetInfo, newAddr: NativePointer): Unit =
    UnsafeUtils.copy(orig.infoAddr, newAddr, OffsetVectors)

  def copy(bytes: Array[Byte], newAddr: NativePointer): Unit =
    UnsafeUtils.unsafe.copyMemory(bytes, UnsafeUtils.arayOffset, UnsafeUtils.ZeroPointer, newAddr, bytes.size)

  /**
   * Actually compares the contents of the ChunkSetInfo non-vector metadata to see if they are equal
   */
  def equals(info1: ChunkSetInfo, info2: ChunkSetInfo): Boolean =
    UnsafeUtils.equate(UnsafeUtils.ZeroPointer, info1.infoAddr, UnsafeUtils.ZeroPointer, info2.infoAddr, OffsetVectors)

  /**
   * Initializes a new ChunkSetInfo with some initial fields
   */
  def initialize(factory: MemFactory, dataset: Dataset, id: ChunkID, startTime: Long): ChunkSetInfo = {
    val infoAddr = factory.allocateOffheap(dataset.chunkSetInfoSize)
    setChunkID(infoAddr, id)
    resetNumRows(infoAddr)
    setStartTime(infoAddr, startTime)
    setEndTime(infoAddr, Long.MaxValue)
    ChunkSetInfo(infoAddr)
  }

  def apply(factory: MemFactory, dataset: Dataset,
            id: ChunkID, numRows: Int, startTime: Long, endTime: Long): ChunkSetInfo = {
    val newInfo = initialize(factory, dataset, id, startTime)
    UnsafeUtils.setInt(newInfo.infoAddr + OffsetNumRows, numRows)
    setEndTime(newInfo.infoAddr, endTime)
    newInfo
  }

  /**
   * Copies the offheap info to a byte array for serialization to a persistent store as a byte array
   */
  def toBytes(info: ChunkSetInfo): Array[Byte] = {
    val bytes = new Array[Byte](OffsetVectors)
    UnsafeUtils.unsafe.copyMemory(UnsafeUtils.ZeroPointer, info.infoAddr, bytes, UnsafeUtils.arayOffset, bytes.size)
    bytes
  }
}