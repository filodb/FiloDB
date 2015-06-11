package filodb.core.datastore

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import org.velvia.filo.RowExtractors.ColumnRowExtractor
import org.velvia.filo.RowSetter
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
                         (implicit system: ActorSystem) extends StrictLogging {
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
        case RowChunk(startRowId, chunks) =>
          logger.trace(s"Got RowChunk($startRowId, ${chunks.length})")
          extractors = getRowExtractors(chunks, columns, rowSetter)
          numRows = extractors.foldLeft(Int.MaxValue) { (acc, e) => Math.min(acc, e.wrapper.length) }
          rowNo = 0
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
