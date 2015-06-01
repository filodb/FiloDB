package filodb.core.datastore

import akka.actor.{ActorRef, PoisonPill, Props}
import java.nio.ByteBuffer
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.Future
import scala.util.Try

import filodb.core.BaseActor
import filodb.core.messages._
import filodb.core.metadata.{Partition, Column, Shard}

/**
 * The ReadCoordinatorActor asynchronously pulls columnar chunks out from the datastore
 * and assembles them into blocks of rows for iteration over row chunks.
 *
 * This is a first cut so lots of assumptions and missing functionality (TODO):
 *  - assumes all the columns requested actually exist for a particular version
 *     (whereas some versions may in fact only have a subset of requested columns)
 *  - does not handle multiple versions
 *  - may use a lot of memory.  Does not limit range of chunks read.
 */
object ReadCoordinatorActor {
  // ////////// Messages
  case object GetNextChunk
  case object FinishedReadingShard
  case object MoreChunks

  // ////////// Responses
  // NOTE: chunks is ordered the same as columns
  case object InvalidPartitionVersion extends ErrorResponse
  case class RowChunk(startRowId: Long, endRowId: Long, chunks: Array[ByteBuffer]) extends Response
  case object EndOfPartition extends Response

  // scalastyle:off
  val NullBuffer: ByteBuffer = null
  // scalastyle:on

  val DefaultMaxRowChunks = 200

  def props(datastore: Datastore,
            partition: Partition,
            version: Int,
            columns: Seq[String],
            maxRowChunks: Int = DefaultMaxRowChunks): Props =
    Props(classOf[ReadCoordinatorActor], datastore, partition, version, columns, maxRowChunks)
}

/**
 * We use these assumptions to make the reading and joining of data from different columns more efficient:
 * - chunks are read in order of increasing starting rowId
 * - multiple columns are read in parallel but the same rowId range at one time
 * - only one outstanding "GetNextChunk" request is served at any one time
 *
 */
class ReadCoordinatorActor(datastore: Datastore,
                           partition: Partition,
                           version: Int,
                           columns: Seq[String],
                           maxRowChunks: Int = ReadCoordinatorActor.DefaultMaxRowChunks) extends BaseActor {
  import ReadCoordinatorActor._

  // Initialize variables
  val firstRowIds = partition.shardsForVersions(version -> version)
  var rowIdIndex = 0
  var startingRowId: Long = Try(firstRowIds(0)).getOrElse(-1L)

  val chunks: Array[ByteBuffer] = Array.fill(maxRowChunks * columns.length)(NullBuffer)
  val rowIdWritten = new Array[Long](columns.length)
  var curChunkRowId = -1L
  var requestor: Option[ActorRef] = None
  var doneReading: Boolean = false

  // TODO: use a dedicated thread pool for reading Futures
  import context.dispatcher

  private def resetChunkBuffer(): Unit = {
    java.util.Arrays.fill(chunks.asInstanceOf[Array[AnyRef]], NullBuffer: AnyRef)
    java.util.Arrays.fill(rowIdWritten, -1L)
  }

  private def baseIndex(rowId: Long): Int =
    ((rowId - startingRowId) / partition.chunkSize).toInt * columns.length

  private def updateChunk(colIdx: Int, rowId: Long, bytes: ByteBuffer): Unit = {
    chunks(baseIndex(rowId) + colIdx) = bytes
    if (rowId > rowIdWritten(colIdx)) rowIdWritten(colIdx) = rowId
  }

  private def startReadShard(): Unit = {
    val shard = Shard(partition, version, firstRowIds(rowIdIndex))
    doneReading = false
    resetChunkBuffer()
    val endingRowId = startingRowId + maxRowChunks * partition.chunkSize
    val readFutures = columns.zipWithIndex.map { case (col, idx) =>
      logger.debug(s"Starting read of column $col, shard $shard from $startingRowId -> $endingRowId")
      datastore.scanOneColumn(shard, col, Some(startingRowId -> endingRowId))(0) {
      case (acc, (_, rowId, bytes)) =>
        updateChunk(idx, rowId, bytes)
        // A bit inefficient. TODO: not every chunk update leads to push of chunks to consumer
        self ! MoreChunks
        acc
      }
    }
    Future.sequence(readFutures).onSuccess {
      case answers: Any => self ! FinishedReadingShard
        logger.debug(s"Done reading all columns $columns")
    }
    // TODO: recover / error handling
  }

  private def nextChunkAvailable: Boolean =
    (curChunkRowId >= 0L) &&
     rowIdWritten.min >= curChunkRowId

  private def returnChunk(): Unit = {
    requestor.foreach { requestorRef =>
      // TODO: actually find real ending Id from columns?
      val endingRowId = curChunkRowId + partition.chunkSize - 1
      val base = baseIndex(curChunkRowId)
      val rowChunks = java.util.Arrays.copyOfRange(chunks.asInstanceOf[Array[AnyRef]],
                                                   base, base + columns.length)
      requestorRef ! RowChunk(curChunkRowId, endingRowId, rowChunks.asInstanceOf[Array[ByteBuffer]])
    }
    requestor = None
    curChunkRowId += partition.chunkSize
  }

  private def advanceToNextShard(): Unit = {
    if (doneReading && curChunkRowId > rowIdWritten.min) {
      // Are we at end of all shards?
      if (rowIdIndex >= firstRowIds.length - 1) {
        logger.info("Read past last shard, quitting...")
        requestor.foreach(_ ! EndOfPartition)
        self ! PoisonPill
      } else {
        // should we be reading next shard?
        if (curChunkRowId >= firstRowIds(rowIdIndex + 1)) {
          rowIdIndex += 1
          startingRowId = firstRowIds(rowIdIndex)
          logger.info(s"Advancing to next shard starting at $startingRowId...")
        } else {
          startingRowId = curChunkRowId
        }
        curChunkRowId = -1
      }
    }
  }

  def receive: Receive = {
    case GetNextChunk =>
      if (firstRowIds.isEmpty) { sender ! InvalidPartitionVersion }
      else {
        advanceToNextShard()
        // Are we at the beginning of a shard?  Initiate reads; put request on stack
        if (curChunkRowId < 0L) {
          startReadShard()
          requestor = Some(sender)
          curChunkRowId = startingRowId
        }
        // Is there a next chunk to read?  If so, return with the chunk; update state
        // Otherwise, set pending flag; send the chunk when it comes in.
        else {
          requestor = Some(sender)
          if (nextChunkAvailable) { returnChunk() }
        }
      }

    // More chunks available to push back.
    case MoreChunks =>
      if (requestor.isDefined && nextChunkAvailable) returnChunk()

    case FinishedReadingShard =>
      doneReading = true
  }

  // at GetNextChunk: figure out next shard to read from, read it async
  // assemble chunks, detect when a full chunk for set of rows has been read, send chunks back
}