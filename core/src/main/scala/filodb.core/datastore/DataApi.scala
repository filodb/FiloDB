package filodb.core.datastore

import java.nio.ByteBuffer
import scala.concurrent.Future

import filodb.core.messages._
import filodb.core.metadata.Shard

object DataApi {
  type ColRowBytes = (String, Long, ByteBuffer)
}

/**
 * A low-level interface for all datastores implementing core table data I/O
 */
trait DataApi {
  import DataApi._

  /**
   * Inserts one chunk of data from different columns.
   * Checks that the rowIdRange is aligned with the chunkSize.
   * @param shard the Shard to write to
   * @param rowId the starting rowId for the chunk. Must be aligned to chunkSize.
   * @param columnsBytes the column name and bytes to be written for each column
   * @returns Success, or other ErrorResponse
   */
  def insertOneChunk(shard: Shard,
                     rowId: Long,
                     columnsBytes: Map[String, ByteBuffer]): Future[Response]

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
                      (foldFunc: (T, ColRowBytes) => T): Future[Either[T, ErrorResponse]]
}