package filodb.core.ingest

import akka.actor.{Actor, ActorRef, Props}
import java.nio.ByteBuffer

import filodb.core.BaseActor
import filodb.core.messages._

/**
 * One IngesterActor instance is created for each dataset/partition ingestion pipeline.
 * It is responsible for managing the state of a single dataset/partition:
 * - Verifying the schema and metadata at the start of ingestion
 * - managing and adding shards as needed
 * - Sending data to DataWriterActor and managing backpressure
 *
 * It receives chunks of columnar data from RowIngesterActor.
 */
object IngesterActor {
  // Sent to IngesterActor either directly or from RowIngesterActor
  // Will get an Ack sent back to the CoordinatorActor, or other error.
  case class ChunkedColumns(version: Int,
                            rowIdRange: (Long, Long),
                            lastSequenceNo: Long,
                            columnsBytes: Map[String, ByteBuffer])

  def props(dataset: String,
            partition: String,
            columns: Seq[String],
            metadataActor: ActorRef,
            dataWriterActor: ActorRef): Props =
    Props(classOf[IngesterActor], dataset, partition, columns, metadataActor, dataWriterActor)
}

/**
 * This is a state machine.  Upon init, not only do the actors get set but it starts to
 * verify the dataset state, schema, etc.
 * @type {[type]}
 */
class IngesterActor(dataset: String,
                    partition: String,
                    columns: Seq[String],
                    metadataActor: ActorRef,
                    dataWriterActor: ActorRef) extends BaseActor {
  import IngesterActor._

  val ready: Receive = {
    case ChunkedColumns(version, (firstRowId, lastRowId), lastSequenceNo, columnsBytes) =>
  }

  def receive: Receive = ready
}