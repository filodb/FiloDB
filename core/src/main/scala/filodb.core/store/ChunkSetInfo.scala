package filodb.core.store

import java.nio.ByteBuffer

import com.googlecode.javaewah.EWAHCompressedBitmap
import com.typesafe.scalalogging.StrictLogging
import debox.Buffer

import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset}
import filodb.memory.BinaryRegion.NativePointer
import filodb.memory.MemFactory
import filodb.memory.data.ElementIterator
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
    val startTime = dataset.timestamp(rows.head)
    val info = ChunkSetInfo(factory, dataset, newChunkID(startTime), rows.length,
                            startTime,
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
  val nullInfo = ChunkSetInfo(UnsafeUtils.ZeroPointer.asInstanceOf[NativePointer])

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

/**
 * When constructed, the iterator holds a shared lock over the backing collection, to protect
 * the contents of the native pointers. The close method must be called when the native pointers
 * don't need to be accessed anymore, and then the lock is released.
 */
// Why can't Scala have unboxed iterators??
trait ChunkInfoIterator { base: ChunkInfoIterator =>
  def close(): Unit
  def hasNext: Boolean
  def nextInfo: ChunkSetInfo

  /**
   * Returns a new ChunkInfoIterator which filters items from this iterator
   */
  def filter(func: ChunkSetInfo => Boolean): ChunkInfoIterator =
    new FilteredChunkInfoIterator(this, func)

  /**
   * Returns a regular Scala Iterator, transforming each ChunkSetInfo into an item of type B
   * Note that regular Iterators are boxed, so this works best if B is an object.
   */
  def map[B](func: ChunkSetInfo => B): Iterator[B] = new Iterator[B] {
    def hasNext: Boolean = base.hasNext
    def next: B = {
      try {
        func(base.nextInfo)
      } catch {
        case e: Throwable => close(); throw e;
      }
    }
  }

  /**
   * Creates a standard Scala Buffer, materializing all infos.
   * Note that this will box and create objects around ChunkSetInfo, so
   * please only use this for testing or convenience.  VERY MEMORY EXPENSIVE.
   */
  def toBuffer: Seq[ChunkSetInfo] = {
    try {
      val buf = new collection.mutable.ArrayBuffer[ChunkSetInfo]
      while (hasNext) { buf += nextInfo }
      buf
    } catch {
      case e: Throwable => close(); throw e;
    }
  }
}

object ChunkInfoIterator {
  val empty = new ChunkInfoIterator {
    def close(): Unit = {}
    def hasNext: Boolean = false
    def nextInfo: ChunkSetInfo = ChunkSetInfo(0)
  }
}

class ElementChunkInfoIterator(elIt: ElementIterator) extends ChunkInfoIterator {
  def close(): Unit = elIt.close()
  final def hasNext: Boolean = elIt.hasNext
  final def nextInfo: ChunkSetInfo = ChunkSetInfo(elIt.next)
}

class FilteredChunkInfoIterator(base: ChunkInfoIterator,
                                filter: ChunkSetInfo => Boolean,
                                // Move vars here for better performance -- no init field
                                var nextnext: ChunkSetInfo = ChunkSetInfo(0),
                                var gotNext: Boolean = false) extends ChunkInfoIterator {
  def close(): Unit = {
    base.close()
  }

  final def hasNext: Boolean = {
    try {
      while (base.hasNext && !gotNext) {
        nextnext = base.nextInfo
        if (filter(nextnext)) gotNext = true
      }
      gotNext
    } catch {
      case e: Throwable => close(); throw e;
    }
  }

  final def nextInfo: ChunkSetInfo = {
    gotNext = false   // reset so we can look for the next item where filter == true
    nextnext
  }
}

/**
 * A sliding window based iterator over the chunks needed to be read from for each window.
 * Assumes the ChunkInfos are in increasing time order.
 * The sliding window goes from (start-window, start] -> (end-window, end] in step increments, and for
 * each window, this class may be used as a ChunkInfoIterator. Excludes start, includes end.

 * @param infos the base ChunkInfoIterator to perform windowing over
 * @param start the starting window end timestamp, must have same units as the ChunkSetInfo start/end times
 * @param step the increment the window goes forward by
 * @param end the ending window end timestamp.  If it does not line up on multiple of start + n * step, then
 *            the windows will slide until the window end is beyond end.
 * @param window the # of millis/time units that define the length of each window
 */
class WindowedChunkIterator(infos: ChunkInfoIterator, start: Long, step: Long, end: Long, window: Long,
                            // internal vars, put it here for better performance
                            var curWindowEnd: Long = -1L,
                            var curWindowStart: Long = -1L,
                            private var readIndex: Int = 0,
                            windowInfos: Buffer[NativePointer] = Buffer.empty[NativePointer])
extends ChunkInfoIterator {
  final def close(): Unit = infos.close()

  /**
   * Returns true if there are more windows to process
   */
  final def hasMoreWindows: Boolean = (curWindowEnd < 0) || (curWindowEnd + step <= end)

  /**
   * Advances to the next window.
   */
  final def nextWindow(): Unit = {
    // advance window pointers and reset read index
    if (curWindowEnd == -1L) {
      curWindowEnd = start
      curWindowStart = start - Math.max(window - 1, 0)  // window cannot be below 0, ie start should never be > end
    } else {
      curWindowEnd += step
      curWindowStart += step
    }
    readIndex = 0

    // drop initial chunksets of window that are no longer part of the window
    while (windowInfos.nonEmpty && ChunkSetInfo(windowInfos(0)).endTime < curWindowStart) {
      windowInfos.remove(0)
    }

    var lastEndTime = if (windowInfos.isEmpty) -1L else ChunkSetInfo(windowInfos(windowInfos.length - 1)).endTime

    // if new window end is beyond end of most recent chunkset, add more chunksets (if there are more)
    while (curWindowEnd > lastEndTime && infos.hasNext) {
      val next = infos.nextInfo
      // Add if next chunkset is within window.  Otherwise keep going
      if (curWindowStart <= next.endTime) {
        windowInfos += next.infoAddr
        lastEndTime = Math.max(next.endTime, lastEndTime)
      }
    }
  }

  /**
   * hasNext of ChunkInfoIterator.  Returns true if for current window there is another relevant chunk.
   */
  final def hasNext: Boolean = readIndex < windowInfos.length

  /**
   * Returns the next ChunkSetInfo for the current window
   */
  final def nextInfo: ChunkSetInfo = {
    val next = windowInfos(readIndex)
    readIndex += 1
    ChunkSetInfo(next)
  }
}

/**
 * A RowReader that returns info about each ChunkSetInfo set via setInfo with a schema like this:
 * 0:  ID (Long)
 * 1:  NumRows (Int)
 * 2:  startTime (Long)
 * 3:  endTime (Long)
 * 4:  numBytes(Int) of chunk
 * 5:  readerclass of chunk
 */
class ChunkInfoRowReader(column: Column) extends RowReader {
  import Column.ColumnType._
  var info: ChunkSetInfo = _

  def setInfo(newInfo: ChunkSetInfo): Unit = { info = newInfo }

  def notNull(columnNo: Int): Boolean = columnNo < 6
  def getBoolean(columnNo: Int): Boolean = ???
  def getInt(columnNo: Int): Int = columnNo match {
    case 1 => info.numRows
    case 4 => BinaryVector.totalBytes(info.vectorPtr(column.id))
  }
  def getLong(columnNo: Int): Long = columnNo match {
    case 0 => info.id
    case 2 => info.startTime
    case 3 => info.endTime
  }

  def getDouble(columnNo: Int): Double = ???
  def getFloat(columnNo: Int): Float = ???
  def getString(columnNo: Int): String = column.columnType match {
    case IntColumn    => vectors.IntBinaryVector(info.vectorPtr(column.id)).getClass.getName
    case LongColumn   => vectors.LongBinaryVector(info.vectorPtr(column.id)).getClass.getName
    case TimestampColumn => vectors.LongBinaryVector(info.vectorPtr(column.id)).getClass.getName
    case DoubleColumn => vectors.DoubleVector(info.vectorPtr(column.id)).getClass.getName
    case StringColumn => vectors.UTF8Vector(info.vectorPtr(column.id)).getClass.getName
    case o: Any       => "nananana, heyheyhey, goodbye"
  }

  override def filoUTF8String(columnNo: Int): ZeroCopyUTF8String = ZeroCopyUTF8String(getString(columnNo))

  def getAny(columnNo: Int): Any = ???

  def getBlobBase(columnNo: Int): Any = ???
  def getBlobOffset(columnNo: Int): Long = ???
  def getBlobNumBytes(columnNo: Int): Int = ???
}