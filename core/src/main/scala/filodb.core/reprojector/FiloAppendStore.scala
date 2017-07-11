package filodb.core.reprojector

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import java.nio.ByteBuffer
import java.nio.file.{Path, Paths}
import net.ceedubs.ficus.Ficus._
import org.velvia.filo._
import scala.collection.mutable.ArrayBuffer
import scalaxy.loops._

import filodb.core.metadata.{Column, RichProjection}
import filodb.core.store.ChunkSet

/**
 * FiloAppendStore is an append-only store that stores chunks of rows in Filo vector format.
 * No buffering is done; every write results in Filo vectors being appended and an on-disk WAL
 * being updated.  If the last chunk was below chunksize, then it is rewritten with new contents.
 * The user must do buffering ahead of time to ensure all the serialization costs isn't overwhelming.
 *
 * ==Config==
 * {{{
 *   memtable {
 *     filo.chunksize = 1000   # The minimum number of rows per Filo chunk
 *   }
 * }}}
 */
class FiloAppendStore(val projection: RichProjection,
                      config: Config,
                      version: Int,
                      actorPath: String,
                      reloadFlag: Boolean = false) extends StrictLogging {
  import RowReader._

  val chunkSize = config.as[Option[Int]]("memtable.filo.chunksize").getOrElse(1000)
  logger.info(s"FiloAppendStore starting with chunkSize = $chunkSize, reloadFlag=${reloadFlag}")

  private val chunks = new ArrayBuffer[Array[ByteBuffer]]
  private val readers = new ArrayBuffer[FiloRowReader]

  private val filoSchema = Column.toFiloSchema(projection.columns)
  private val clazzes = filoSchema.map(_.dataType).toArray
  private val colIds = filoSchema.map(_.name).toArray

  private val builder = new RowToVectorBuilder(filoSchema, ChunkSet.builderMap)

  private var _numRows = 0

  private var wal: Option[WriteAheadLog] = createWalAheadLog

  def createWalAheadLog : Option[WriteAheadLog] = {
    if (!reloadFlag && config.getBoolean("write-ahead-log.write-ahead-log-enabled")) {
      logger.debug(s"Creating WriteAheadLog for dataset: (${projection.datasetRef}, ${version})")
      Some(new WriteAheadLog(config, projection.datasetRef, actorPath, projection.dataColumns, version))
    }else{
      None
    }
  }

  /**
   * Appends new rows to the row store.  The rows are serialized into Filo vectors and flushed to
   * the WAL as well, so make sure there are a sizeable number of rows, ideally >= chunksize.
   * NOTE: only one chunk is flushed, even if # of rows is much bigger than chunksize.
   * @param rows the rows to append
   * @return the (chunkIndex, starting row #) of the rows just added
   */
  def appendRows(rows: Seq[RowReader]): (Int, Int) = {
    val baseLength = builder.builders.head.length   // Nonzero only if partial chunks added

    // Last chunk written partial?  Builder should have been left with rows intact. Remove last chunks/readers
    if (readers.nonEmpty && readers.last.parsers(0).length < chunkSize) {
      chunks.remove(chunks.length - 1, 1)
      readers.remove(readers.length - 1, 1)
    }

    // Add new rows to builder
    val nextChunkIndex = chunks.length
    rows.foreach(builder.addRow)
    _numRows += rows.length

    // Add chunks
    val finalLength = builder.builders.head.length
    val colIdToBuffers = builder.convertToBytes()
    val chunkArray = colIds.map(colIdToBuffers)
    chunks += chunkArray
    readers += new FastFiloRowReader(chunkArray, clazzes, finalLength)

    // write chunks to WAL
    if (config.getBoolean("write-ahead-log.write-ahead-log-enabled")) {
      wal.foreach(_.writeChunks(chunkArray))
    }

    // Reset builder if it was at least chunkSize rows
    if (finalLength >= chunkSize) builder.reset()

    (nextChunkIndex, baseLength)
  }

  def initWithChunks(walChunks: Array[ByteBuffer]): (Int, FastFiloRowReader) = {

    val nextChunkIndex = chunks.length

    // Add chunks
    chunks += walChunks
    val rowreader = new FastFiloRowReader(walChunks, clazzes)
    readers += rowreader
    _numRows += rowreader.parsers.head.length

    (nextChunkIndex, rowreader)
  }

  def setWriteAheadLogFile(recentFile: Option[Path], position: Int): Unit = {
    val pathObj = recentFile.getOrElse(Paths.get(""))
    logger.debug(s"Creating WriteAheadLog for dataset: ${projection.datasetRef}" +
      s" using path object: ${pathObj.getFileName}")
    wal = Some(new WriteAheadLog(config, projection.datasetRef, actorPath,
      projection.dataColumns, version, pathObj, position))
  }

  /**
   * Retrieves a single row at a given chunkIndex and row number.  This is meant for speed - so
   * no limit checking is done, and the chunkIndex and rowNo are encoded as a long.
   * @param keyLong upper 32 bits = chunk #, lower 32 bits = row #
   */
  final def getRowReader(keyLong: Long): RowReader = {
    val reader = readers((keyLong >> 32).toInt)
    reader.setRowNo(keyLong.toInt)
    reader
  }

  final def safeRowReader(keyLong: Long): RowReader = {
    val reader = readers((keyLong >> 32).toInt)
    SafeFiloRowReader(reader, keyLong.toInt)
  }

  def numRows: Int = _numRows

  def reset(): Unit = {
    chunks.clear
    readers.clear
    builder.reset()
  }

  def deleteWalFiles(): Unit = wal.foreach(_.delete())
}