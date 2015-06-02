package filodb.core.datastore

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import java.nio.ByteBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scalaxy.loops._

import filodb.core.metadata.{Partition, Column}
import filodb.core.messages.Response

object ReadRowExtractor {
  val DefaultTimeout = 1 minute
}

/**
 * This class iterates over all the columnar chunks in a partition
 * for a given version.
 * @type {[type]}
 */
class ReadRowExtractor[R](datastore: Datastore,
                          partition: Partition,
                          version: Int,
                          columns: Seq[Column],
                          rowSetter: RowSetter[R],
                          readTimeout: FiniteDuration = ReadRowExtractor.DefaultTimeout)
                         (implicit system: ActorSystem) {
  val columnNames = columns.map(_.name)
  val coordinator = system.actorOf(ReadCoordinatorActor.props(datastore, partition, version, columnNames))
  val numColumns = columns.length
  var numRows = 0
  var rowNo = 0

  implicit val timeout = Timeout(readTimeout)

  import ReadCoordinatorActor._
  import Serde._

  var extractors: Array[ColumnRowExtractor[_, R]] = Array()

  def hasNext: Boolean = {
    // At the end of current chunk?  Then fetch next one and wait
    if (rowNo >= numRows) {
      Await.result((coordinator ? GetNextChunk).mapTo[Response], readTimeout) match {
        case EndOfPartition =>  return false
        case RowChunk(startRowId, endRowId, chunks) =>
          extractors = getRowExtractors(chunks, columns, rowSetter)
          rowNo = 0
          numRows = (endRowId - startRowId).toInt + 1
      }
    }
    true
  }

  def next(row: R): Unit = {
    for { i <- 0 until numColumns optimized } {
      extractors(i).extractToRow(rowNo, row)
    }
    rowNo += 1
  }
}

/**
 * Sets the value in row of type R at position index (0=first column)
 * TODO: move RowSetter and company to Filo project.
 * NOTE: This is definitely designed for performance/mutation rather than
 * functional purity.  In fact it's designed with Spark's Row trait in mind.
 */
trait RowSetter[R] {
  def setInt(row: R, index: Int, data: Int): Unit
  def setLong(row: R, index: Int, data: Long): Unit
  def setDouble(row: R, index: Int, data: Double): Unit
  def setString(row: R, index: Int, data: String): Unit
}