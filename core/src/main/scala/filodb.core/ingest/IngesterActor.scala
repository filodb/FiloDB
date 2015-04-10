package filodb.core.ingest

import akka.actor.{Actor, ActorRef, PoisonPill, Props, FSM}
import java.nio.ByteBuffer

import filodb.core.BaseActor
import filodb.core.datastore.Datastore
import filodb.core.messages._
import filodb.core.metadata.{Column, Partition, Shard}

/**
 * One IngesterActor instance is created for each dataset/partition ingestion pipeline.
 * It is responsible for writing columnar chunks of a single dataset/partition:
 * - managing and adding shards as needed
 * - Sending data to DataWriterActor and managing backpressure
 *
 * It receives chunks of columnar data from RowIngesterActor.
 */
object IngesterActor {
  // /////////// Commands

  // Sent to IngesterActor either directly or from RowIngesterActor
  // Will get an Ack sent back to the CoordinatorActor, or other error.
  case class ChunkedColumns(version: Int,
                            rowIdRange: (Long, Long),
                            lastSequenceNo: Long,
                            columnsBytes: Map[String, ByteBuffer])

  // /////////// Responses

  case class Ack(dataset: String, partition: String, seqId: Long) extends Response
  case class ShardingError(dataset: String, partition: String, seqId: Long) extends ErrorResponse
  case class WriteError(dataset: String, partition: String, seqId: Long, resp: ErrorResponse)
    extends ErrorResponse

  def props(partition: Partition,
            schema: Seq[Column],
            metadataActor: ActorRef,
            datastore: Datastore,
            sourceActor: ActorRef): Props =
    Props(classOf[IngesterActor], partition, schema, metadataActor, datastore, sourceActor)
}

/**
 * Constructor:
 * @param sourceActor the Actor to whom Acks will flow back to. usually the one pushing new rows
 *        to RowIngesterActor.
 *
 * Acks and failure handling:
 *
 * Every columnar chunk coming into IngesterActor has a sequence ID.  Both when writing to
 * the dataWriterActor as well as the metadataActor if necessary, IngesterActor will get acks back
 * with the sequenceID.  IngesterActor keeps track of the un-acked sequenceIDs and only sends back
 * Acks to the User if all of the writers acknowledge for a given sequenceID.
 *
 * Right now error responses for a given sequenceID will cause the ingesterActor to shutdown.
 * In the future we can keep some number of columns in memory and retry operations from the last successfully
 * acked point in time.
 * TODO: Retries
 * TODO: handling acking
 */
class IngesterActor(partition: Partition,
                    schema: Seq[Column],
                    metadataActor: ActorRef,
                    datastore: Datastore,
                    sourceActor: ActorRef) extends BaseActor {
  import IngesterActor._

  import context.dispatcher

  def receive: Receive = {
    case ChunkedColumns(version, (firstRowId, lastRowId), lastSequenceNo, columnsBytes) =>
      // TODO: 0. Check the columns against the schema

      // 1. Figure out the shard
      partition.shardingStrategy.getShard(partition, firstRowId, version) match {
        case None =>
          sourceActor ! ShardingError(partition.dataset, partition.partition, lastSequenceNo)
        case Some(shardRowId) =>
          // 2. Update partition shard info if needed
          if (!partition.contains(shardRowId, version)) {
            logger.debug(s"Adding shardRowId $shardRowId to partition $partition...")
            metadataActor ! Partition.AddShardVersion(partition, shardRowId, version)
            // track Ack from metadataActor
          }

          // 3. Forward to dataWriterActor
          val shard = Shard(partition, version, shardRowId)
          datastore.insertOneChunk(shard, firstRowId, lastSequenceNo, columnsBytes)
            .onSuccess { case response: Response => self ! response }
      }

    case Datastore.Ack(lastSeqNo: Long) =>
      // For now, just pass the ack straight back.  In the future, we'll want to use this for throttling
      sourceActor ! Ack(partition.dataset, partition.partition, lastSeqNo)
  }
}