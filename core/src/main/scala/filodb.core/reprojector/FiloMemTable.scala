package filodb.core.reprojector

import com.typesafe.config.Config
import java.nio.ByteBuffer
import java.util.TreeMap
import net.ceedubs.ficus.Ficus._
import org.velvia.filo.{VectorInfo, RowToVectorBuilder, RowReader, FiloRowReader, FastFiloRowReader}
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.math.Ordered
import scalaxy.loops._

import filodb.core.{KeyRange, SortKeyHelper}
import filodb.core.Types._
import filodb.core.columnstore.{RowWriterSegment, Segment}
import filodb.core.metadata.{Column, Dataset, RichProjection}

/**
 * A MemTable using Filo vectors to store rows in memory, plus an index to seek into the chunks.
 * The index is just an on-heap TreeMap per partition and keeps rows in sorted order.
 * The idea is to minimize serialization costs by leveraging Filo vectors, as compared to MapDB,
 * which has to do several expensive key serialization steps when inserting into a Map.
 * Reads are still efficient because Filo vectors are designed for fast random access.
 * New rows are kept in tempRows while enough rows or time elapses to be flushed into columnar
 * chunks.  If not enough rows before flush.interval elapses, then the chunk is flushed anyways, but will be
 * appended to next time, such that each chunk always fills up.
 *
 * ==Config==
 * {{{
 *   memtable {
 *     filo.chunksize = 1000   # The number of rows per Filo chunk
 *     flush.interval = 1 s    # If less than chunksize rows ingested before this interval, then
 *                             # rows will be flushed out anyways
 *   }
 * }}}
 */
class FiloMemTable[K](val projection: RichProjection[K], config: Config) extends MemTable[K] {
  import RowReader._
  import collection.JavaConverters._

  val chunkSize = config.as[Option[Int]]("memtable.filo.chunksize").getOrElse(1000)

  // From sort key K to a Long: upper 32-bits = chunk index, lower 32 bits = row index
  type KeyMap = TreeMap[K, Long]

  private val chunks = new ArrayBuffer[Array[ByteBuffer]]
  private val readers = new ArrayBuffer[FiloRowReader]
  private val partKeyMap = new HashMap[PartitionKey, KeyMap]

  private val filoSchema = projection.columns.map {
    case Column(name, _, _, colType, serializer, false, false) =>
      require(serializer == Column.Serializer.FiloSerializer)
      VectorInfo(name, colType.clazz)
  }
  private val clazzes = filoSchema.map(_.dataType).toArray
  private val colIdToNumber =
    projection.columns.zipWithIndex.map { case (col, idx) => (col.name, idx) }.toMap

  // Holds temporary rows before being flushed to chunk columnar storage
  private val tempRows = new ArrayBuffer[RowReader]
  private val callbacks = new ArrayBuffer[(Int, Int, () => Unit)]   // Start and end row index for each callback
  private val builder = new RowToVectorBuilder(filoSchema)

  private def getKeyMap(partition: PartitionKey): KeyMap = {
    partKeyMap.getOrElse(partition, {
      // Only hit this if partition isn't defined, so save expensive synchronization for initial cases only
      partKeyMap.synchronized {
        partKeyMap.getOrElseUpdate(partition, new KeyMap(projection.helper.ordering))
      }
    })
  }

  private def chunkRowIdToLong(chunkIndex: Int, rowNo: Int): Long =
    (chunkIndex.toLong << 32) + rowNo

  // Converts rows to chunks, merging with previous chunks as needed, and invokes callbacks
  // Also updates the partKeyMap which maps every sort key to its chunk ID and row index
  private def flushRowsToChunks(): Unit = synchronized {
    builder.reset()

    // Last chunk written partial?  Get chunks out, populate builder, remove from chunks
    if (readers.nonEmpty && readers.last.parsers(0).length < chunkSize) {
      val reader = readers.last
      for { i <- 0 until reader.parsers(0).length optimized } {
        reader.rowNo = i
        builder.addRow(reader)
      }
      chunks.remove(chunks.length - 1, 1)
      readers.remove(readers.length - 1, 1)
    }

    // Add new rows to builder, adding to partKeyMap in the meantime
    val baseLength = builder.builders.head.length   // Nonzero only if partial chunks added
    val rowsToAdd = Math.min(chunkSize - baseLength, tempRows.length)
    val nextChunkIndex = chunks.length
    val sortKeyFunc = projection.sortKeyFunc

    for { i <- 0 until rowsToAdd optimized } {
      val row = tempRows(i)
      builder.addRow(row)
      val keyMap = getKeyMap(projection.partitionFunc(row))
      keyMap.put(sortKeyFunc(row), chunkRowIdToLong(nextChunkIndex, baseLength + i))
    }

    // Add chunks
    val chunkAray = new Array[ByteBuffer](filoSchema.length)
    builder.convertToBytes().foreach { case (colName, bb) => chunkAray(colIdToNumber(colName)) = bb }
    chunks += chunkAray
    readers += new FastFiloRowReader(chunkAray, clazzes)

    // invoke callbacks
    while (callbacks.nonEmpty && callbacks.head._1 < rowsToAdd && callbacks.head._2 < rowsToAdd) {
      callbacks.head._3()
      callbacks.remove(0, 1)
    }

    // Remove rows added from tempRows
    tempRows.remove(0, rowsToAdd)

    // Adjust remaining callback row indices
    callbacks.zipWithIndex.foreach { case ((firstIdx, lastIdx, fn), i) =>
      callbacks(i) = (Math.min(0, firstIdx - rowsToAdd), Math.min(0, lastIdx - rowsToAdd), fn)
    }
  }

  def close(): Unit = {}

  /**
   * === Row ingest, read, delete operations ===
   */
  def ingestRows(rows: Seq[RowReader])(callback: => Unit): Unit = if (rows.nonEmpty) {
    val oldLen = tempRows.length
    tempRows ++= rows
    callbacks += ((oldLen, tempRows.length - 1, { () => callback }))
    while (tempRows.length >= chunkSize) flushRowsToChunks()
  }

  /**
   * Forces the rows in tempRows to be committed into the chunk store and disk storage / WAL.
   * This might be necessary before flushing the memtable, or for testing.
   */
  def forceCommit(): Unit = {
    while (tempRows.nonEmpty) flushRowsToChunks()
  }

  private def getRowReader(keyLong: Long): RowReader = {
    val reader = readers((keyLong >> 32).toInt)
    reader.rowNo = keyLong.toInt
    reader
  }

  def readRows(keyRange: KeyRange[K]): Iterator[RowReader] = {
    val keyMap = getKeyMap(keyRange.partition)
    keyMap.subMap(keyRange.start, true, keyRange.end, !keyRange.endExclusive)
          .values.iterator.asScala
          .map(getRowReader)
  }

  def readAllRows(): Iterator[(PartitionKey, K, RowReader)] = {
    partKeyMap.keys.toIterator.flatMap { partition =>
      getKeyMap(partition).entrySet.iterator.asScala
        .map { entry =>
          (partition, entry.getKey, getRowReader(entry.getValue))
        }
    }
  }

  def removeRows(keyRange: KeyRange[K]): Unit = ???

  // Only approximate, and it's better to give not the # of unique rows but the # of rows
  // ingested into chunks, because lots of replaces still uses up more memory
  def numRows: Int = chunks.length * chunkSize + tempRows.length

  def clearAllData(): Unit = {
    chunks.clear
    readers.clear
    partKeyMap.clear
    tempRows.clear
    callbacks.clear
    builder.reset()
  }
}