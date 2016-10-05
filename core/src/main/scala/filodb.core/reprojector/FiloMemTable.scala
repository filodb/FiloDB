package filodb.core.reprojector

import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConversions._
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.util.TreeMap

import org.velvia.filo.{FastFiloRowReader, RowReader}

import scala.math.Ordered
import scalaxy.loops._
import filodb.core.KeyRange
import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset, RichProjection}

import scala.collection.mutable.ArrayBuffer

/**
 * A MemTable using Filo vectors to store rows in memory, plus an index to seek into the chunks.
 * The index is just an on-heap TreeMap per partition and keeps rows in sorted order.
 * The idea is to minimize serialization costs by leveraging Filo vectors, as compared to MapDB,
 * which has to do several expensive key serialization steps when inserting into a Map.
 * Reads are still efficient because Filo vectors are designed for fast random access and minimal
 * deserialization.
 * The user is responsible for buffering writes.  The FiloAppendStore is added to every time such that
 * if no buffering is done, then inserts will be very expensive.
 *
 * ==Config==
 * {{{
 *   memtable {
 *     filo.chunksize = 1000   # The minimum number of rows per Filo chunk
 *   }
 * }}}
 */
class FiloMemTable(val projection: RichProjection,
                   config: Config,
                   actorAddr: String,
                   version: Int,
                   reloadFlag: Boolean = false) extends MemTable with StrictLogging {
  import collection.JavaConverters._

  type PK = projection.partitionType.T
  type RK = projection.rowKeyType.T
  type SK = projection.segmentType.T

  // From row key K to a Long: upper 32-bits = chunk index, lower 32 bits = row index
  type KeyMap = TreeMap[RK, Long]

  private implicit val partOrdering = projection.partitionType.ordering
  private implicit val partSegOrdering = projection.segmentType.ordering
  private val partSegKeyMap = new TreeMap[(PK, SK), KeyMap](Ordering[(PK, SK)])

  private val appendStore = new FiloAppendStore(projection, config, version, reloadFlag)

  val walDir = config.getString("memtable.memtable-wal-dir")

  // NOTE: No synchronization required, because MemTables are used within an actor.
  // See InMemoryMetaStore for a thread-safe design
  private def getKeyMap(partition: PK, segment: SK): KeyMap = {
    partSegKeyMap.get((partition, segment)) match {
      //scalastyle:off
      case null =>
        //scalastyle:on
        val newMap = new KeyMap(projection.rowKeyType.ordering)
        partSegKeyMap.put((partition, segment), newMap)
        newMap
      case k: KeyMap => k
    }
  }

  private def chunkRowIdToLong(chunkIndex: Int, rowNo: Int): Long =
    (chunkIndex.toLong << 32) + rowNo

  private val rowKeyFunc = projection.rowKeyFunc
  private val partitionFunc = projection.partitionKeyFunc
  private val segmentKeyFunc = projection.segmentKeyFunc

  def close(): Unit = {}

  /**
   * === Row ingest, read, delete operations ===
   */
  def ingestRows(rows: Seq[RowReader]): Unit = if (rows.nonEmpty) {
    val (chunkIndex, startRowNo) = appendStore.appendRows(rows)
    var rowNo = startRowNo
    // For a Seq[] interface, foreach is much much faster than rows(i)
    rows.foreach { row =>
      val keyMap = getKeyMap(partitionFunc(row), segmentKeyFunc(row))
      keyMap.put(rowKeyFunc(row), chunkRowIdToLong(chunkIndex, rowNo))
      rowNo += 1
    }
  }

  def reloadMemTable(): Unit =  {
    var recentFile: Option[Path] = None
    var setWalFile: Boolean = true
    var position: Int = 0
    for {file <- Files.newDirectoryStream(Paths.get(
      s"${walDir}/${projection.datasetRef.dataset}_${version}"), "*.wal")} {
      logger.debug(s"loading WAL file:${file.getFileName}")
      val walReader = new WriteAheadLogReader(config, projection.columns, file.toString)
      if (walReader.validFile) {
        val chunks = walReader.readChunks().getOrElse(new ArrayBuffer[Array[ByteBuffer]])
        for (chunkArray <- chunks) {
          val (chunkIndex, rowreader) = appendStore.initWithChunks(chunkArray)
          // FiloRowReader - set row no for each row
          for {index <- 0 to rowreader.parsers.head.length - 1} {
            rowreader.setRowNo(index)
            val keyMap = getKeyMap(partitionFunc(rowreader), segmentKeyFunc(rowreader))
            keyMap.put(rowKeyFunc(rowreader), chunkRowIdToLong(chunkIndex, index))
          }
        }
        // TODO: if there is more than one wal file then delete files and keep only the last one
        recentFile = Some(file)
        position = walReader.buffer.position()
      } else{
        setWalFile = false
        logger.error(s"Unable to load WAL file: ${file.getFileName} due to invalid WAL header format")
      }
    }
    if(setWalFile) {
      appendStore.setWalAHeadLogFile(recentFile, position)
    }
  }

  def readRows(partition: projection.PK, segment: projection.SK): Iterator[(projection.RK, RowReader)] =
    getKeyMap(partition, segment).entrySet.iterator.asScala
      .map { entry => (entry.getKey, appendStore.getRowReader(entry.getValue)) }

  def getSegments(): Iterator[(projection.PK, projection.SK)] =
    partSegKeyMap.keySet.iterator.asScala

  def numRows: Int = appendStore.numRows

  def clearAllData(): Unit = {
    partSegKeyMap.clear
    appendStore.reset()
  }

  override def deleteWalFiles(): Unit = appendStore.deleteWalFiles()
}