package filodb.core.ingest

import akka.actor.{Actor, ActorRef, Props}
import java.nio.ByteBuffer
import org.velvia.filo.RowIngestSupport

import filodb.core.BaseActor
import filodb.core.messages._

/**
 * The RowIngesterActor provides a high-level row-based API on top of the chunked columnar low level API.
 *
 * - ingest individual rows with a sequence # and Row ID.
 * - groups the rows into chunks aligned with the chunksize and translates into columnar format
 * - Also takes care of replaces by reading older chunks and doing operations on it
 *
 * See doc/ingestion.md for more detailed information.
 */
object RowIngesterActor {
  /**
   * Appends a new row or replaces an existing row, depending on the rowId.
   * @param sequenceNo input sequence number, used for at least once acking / replays
   * @param rowId the row ID within the partition to append to or replace
   * @param the row of data R
   */
  case class Row[R](sequenceNo: Long, rowId: Long, row: R)

  def props[R](ingesterActor: ActorRef, rowIngestSupport: RowIngestSupport[R]): Props =
    Props(classOf[RowIngesterActor[R]], ingesterActor, rowIngestSupport)
}

class RowIngesterActor[R](ingesterActor: ActorRef,
                          rowIngestSupport: RowIngestSupport[R]) extends BaseActor {
  import RowIngesterActor._

  def receive: Receive = {
    case Row(seqNo, rowId, row) =>
  }
}