package filodb.core.reprojector

import java.nio.file.{Files, Path, Paths}

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.util.TreeMap

import org.velvia.filo.RowReader

import scala.math.Ordered
import scalaxy.loops._
import filodb.core.KeyRange
import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import filodb.core.store.SegmentInfo

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

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
                   actorPath: String,
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

  private var appendStore = new FiloAppendStore(projection, config, version, actorPath, reloadFlag)

  val walDir = config.getString("write-ahead-log.memtable-wal-dir")

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
    logger.debug(s"started reload memtable:${walDir}/${actorPath}_${projection.datasetRef}_${version}")
    var loadResult: (Int, Boolean) = (0, true)

    try {
      val walfiles = Files.newDirectoryStream(Paths.get(
        s"${walDir}/${actorPath}_${projection.datasetRef}_${version}"), "*.wal").toList

      if (!walfiles.isEmpty && walfiles.length > 0) {
        logger.debug(s"started loading WAL files")
        for {index <- walfiles.indices} {
          loadResult = loadChunks(walfiles(index))
          logger.debug(s"Loaded WAL file successfully into Memtable-2:${loadResult._2}, ${loadResult._1}")
          if (index < walfiles.length - 1) {
            // TODO @parekuti: if there is more than one wal file then delete files and keep only the last one
          } else {
            initWALLogFile(Some(walfiles(index)), loadResult._2, loadResult._1)
          }
        }
      } else {
        logger.debug(s"No Memtable WAL files exists")
        initWALLogFile(None, false, 0)
      }
    } catch {
      // TODO @parekuti: Handle NoSuchFileException errors
      case e: Exception =>
        logger.error(s"Error while scanning Memtable WAL folders:${e.getMessage}")
        throw e
    }
  }

  private def initWALLogFile(recentFile: Option[Path],
                             setWalFile: Boolean,
                             position: Int): Unit = {
    if (setWalFile) {
      appendStore.setWriteAheadLogFile(recentFile, position)
    } else {
      appendStore = new FiloAppendStore(projection, config, version, actorPath, false)
    }
  }

  private def loadChunks(path: Path): (Int, Boolean) = {
    val walReader = new WriteAheadLogReader(config, projection.columns, path.toString)
    if (walReader.validFile) {
      logger.debug(s"Valid WAL file:${path.toString} and ready to load chunks into Memtable")
      val chunks = walReader.readChunks().getOrElse(new ArrayBuffer[Array[Chunk]])
      for {chunkArray <- chunks} {
        val (chunkIndex, rowreader) = appendStore.initWithChunks(chunkArray)
        // FiloRowReader - set row no for each row
        for {index <- 0 to rowreader.parsers.head.length - 1} {
          rowreader.setRowNo(index)
          val keyMap = getKeyMap(partitionFunc(rowreader), segmentKeyFunc(rowreader))
          keyMap.put(rowKeyFunc(rowreader), chunkRowIdToLong(chunkIndex, index))
        }
      }
      (walReader.buffer.position(), true)
    } else {
      logger.error(s"Unable to load WAL file: ${path.getFileName} due to invalid WAL header format")
      // TODO @parekuti: Move invalid WAL file
      (0, false)
    }
  }

  def readRows(partition: projection.PK, segment: projection.SK): Iterator[RowReader] =
    getKeyMap(partition, segment).entrySet.iterator.asScala
      .map { entry => appendStore.getRowReader(entry.getValue) }

  def safeReadRows(segInfo: SegmentInfo[projection.PK, projection.SK]): Iterator[RowReader] =
    getKeyMap(segInfo.partition, segInfo.segment).entrySet.iterator.asScala
      .map { entry => appendStore.safeRowReader(entry.getValue) }

  def getSegments(): Iterator[(projection.PK, projection.SK)] =
    partSegKeyMap.keySet.iterator.asScala

  def numRows: Int = appendStore.numRows

  def clearAllData(): Unit = {
    partSegKeyMap.clear
    appendStore.reset()
  }

  override def deleteWalFiles(): Unit = appendStore.deleteWalFiles()
}