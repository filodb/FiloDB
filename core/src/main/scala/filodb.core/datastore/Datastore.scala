package filodb.core.datastore

import java.nio.ByteBuffer
import scala.concurrent.{Future, ExecutionContext}

import filodb.core.metadata.Shard
import filodb.core.messages._

object Datastore {
  case class Ack(lastSequenceNo: Long) extends Response
  case object ChunkMisaligned extends ErrorResponse
}

/**
 * The Datastore trait is intended to be the higher level API abstraction
 * over the lower level storage specific APIs.  Most folks will want
 * to use this instead of the *Api traits.  It combines the separate *Api traits as well.
 *
 * It includes some verification and metadata logic on top of the *Api interfaces
 * TODO: as well as throttling/backpressure.
 */
trait Datastore {
  import Datastore._

  def dataApi: DataApi

  import DataApi.ColRowBytes

  /**
   * Inserts one chunk of data from different columns.
   * Checks that the rowIdRange is aligned with the chunkSize.
   * @param shard the Shard to write to
   * @param rowId the starting rowId for the chunk. Must be aligned to chunkSize.
   * @param lastSeqNo the last Long sequence number of the chunk of columns
   * @param columnsBytes the column name and bytes to be written for each column
   * @returns Ack(lastSeqNo), or other ErrorResponse
   */
  def insertOneChunk(shard: Shard,
                     rowId: Long,
                     lastSeqNo: Long,
                     columnsBytes: Map[String, ByteBuffer])
                    (implicit context: ExecutionContext): Future[Response]
    = dataApi.insertOneChunk(shard, rowId, columnsBytes)
        .collect { case Success => Ack(lastSeqNo) }

  /**
   * Streams chunks from one column in, applying the folding function to chunks.
   * @param shard the Shard to read from
   * @param column the name of the column to read from
   * @param rowIdRange an optional range to restrict portion of shard to read from.
   * @param initValue the initial value to be fed to the folding function
   * @param foldFunc a function taking the currently accumulated T, new chunk, and returns a T
   * @returns either a T or ErrorResponse
   */
  def scanOneColumn[T](shard: Shard,
                    column: String,
                    rowIdRange: Option[(Long, Long)] = None)
                   (initValue: T)
                   (foldFunc: (T, ColRowBytes) => T)
                   (implicit context: ExecutionContext): Future[Either[T, ErrorResponse]] =
    dataApi.scanOneColumn(shard, column, rowIdRange)(initValue)(foldFunc)
}