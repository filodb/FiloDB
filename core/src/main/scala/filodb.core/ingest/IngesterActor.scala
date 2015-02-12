package filodb.core.ingest

import akka.actor.{Actor, ActorRef, PoisonPill, Props, FSM}
import java.nio.ByteBuffer

import filodb.core.BaseActor
import filodb.core.messages._
import filodb.core.metadata.{Column, Partition}

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
  // /////////// Commands

  // Sent to IngesterActor either directly or from RowIngesterActor
  // Will get an Ack sent back to the CoordinatorActor, or other error.
  case class ChunkedColumns(version: Int,
                            rowIdRange: (Long, Long),
                            lastSequenceNo: Long,
                            columnsBytes: Map[String, ByteBuffer])

  // /////////// Responses

  case object CannotLockPartition extends ErrorResponse
  case class UndefinedColumns(undefined: Seq[String]) extends ErrorResponse

  // Sent from IngesterActor when start of ingestion can begin
  case class GoodToGo(partition: Partition, schema: Column.Schema) extends Response

  // Sent from IngesterActor when its ready for more chunks
  case class Ready(dataset: String, partition: String)

  // /////////// States

  sealed trait IngestState
  case object GetLock extends IngestState
  case object GetSchema extends IngestState
  case object GetPartition extends IngestState
  case object Ready extends IngestState
  case object Waiting extends IngestState

  // /////////// Data
  // /
  sealed trait Data
  case object Uninitialized extends Data
  case class GotSchema(schema: Column.Schema) extends Data
  case class GotData(partition: Partition, schema: Column.Schema) extends Data

  // /////////// Core functions
  //
  def invalidColumns(columns: Seq[String], schema: Column.Schema): Seq[String] =
    (columns.toSet -- schema.keys).toSeq

  def props(dataset: String,
            partition: String,
            columns: Seq[String],
            metadataActor: ActorRef,
            dataWriterActor: ActorRef): Props =
    Props(classOf[IngesterActor], dataset, partition, columns, metadataActor, dataWriterActor)
}

import IngesterActor._

/**
 * This is a state machine.  Upon init, not only do the actors get set but it starts to
 * verify the dataset state, schema, etc.
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
 */
class IngesterActor(dataset: String,
                    partitionName: String,
                    columns: Seq[String],
                    metadataActor: ActorRef,
                    dataWriterActor: ActorRef) extends BaseActor with FSM[IngestState, Data] {
  // If partition locking was implemented, we would do something like this:
  // metadataActor ! GetPartitonLock(dataset, partitionName)
  // startWith(GetLock, Uninitialized)

  startWith(GetSchema, Uninitialized)

  // TODO: what to do about versions?
  metadataActor ! Column.GetSchema(dataset, Integer.MAX_VALUE)

  when(GetSchema) {
    case Event(Column.TheSchema(schema), Uninitialized) =>
      val undefinedCols = invalidColumns(columns, schema)
      if (undefinedCols.nonEmpty) {
        logger.info(s"Undefined columns $undefinedCols for dataset $dataset...")
        context.parent ! UndefinedColumns(undefinedCols.toSeq)
        self ! PoisonPill
        stay using Uninitialized
      } else {
        metadataActor ! Partition.GetPartition(dataset, partitionName)
        goto(GetPartition) using GotSchema(schema)
      }
  }

  when(GetPartition) {
    case Event(Partition.ThePartition(partObj), GotSchema(schema)) =>
      context.parent ! GoodToGo(partObj, schema)
      // TODO: Register with the dataWriterActor
      goto(Waiting) using GotData(partObj, schema)
  }

  when(Ready) {
    case Event(ChunkedColumns(version, (firstRowId, lastRowId), lastSequenceNo, columnsBytes),
               data: GotData) =>
      // 1. Figure out the shard
      // 2. Update partition shard info if needed
      // 3. Forward to dataWriterActor
      // stay at Ready or go to Waiting depending on how many chunks we have left to go before able to
      // send again
      goto(Ready) using data
  }
}