package filodb.core.ingest

import akka.actor.{Actor, ActorRef, PoisonPill, Props, FSM}
import java.nio.ByteBuffer

import filodb.core.BaseActor
import filodb.core.messages._
import filodb.core.metadata.{Column, Partition, Shard}

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
  case class NoDatasetColumns(dataset: String) extends ErrorResponse
  case class UndefinedColumns(dataset: String, undefined: Seq[String]) extends ErrorResponse

  // Sent from IngesterActor when start of ingestion can begin
  case class GoodToGo(partition: Partition, schema: Column.Schema) extends Response

  case class Ack(dataset: String, partition: String, seqId: Long) extends Response
  case class ShardingError(dataset: String, partition: String, seqId: Long) extends ErrorResponse
  case class WriteError(dataset: String, partition: String, seqId: Long, resp: ErrorResponse)
    extends ErrorResponse

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
 * Normally the two states are Ready and Waiting.  If more than N unacked chunks are written out,
 * then the actor goes into Waiting state.
 *
 * Acks and failure handling:
 *
 * Errors during the initial partition/schema verification phase will cause the actor to shut itself down.
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

  def killMyself(msg: Any): State = {
    context.parent ! msg
    self ! PoisonPill
    stay using Uninitialized
  }

  when(GetSchema) {
    case Event(Column.TheSchema(schema), Uninitialized) =>
      val undefinedCols = invalidColumns(columns, schema)
      if (schema.isEmpty) {
        logger.info(s"Either no columns defined or no dataset $dataset")
        killMyself(NoDatasetColumns(dataset))
      } else if (undefinedCols.nonEmpty) {
        logger.info(s"Undefined columns $undefinedCols for dataset $dataset with schema $schema")
        killMyself(UndefinedColumns(dataset, undefinedCols.toSeq))
      } else {
        metadataActor ! Partition.GetPartition(dataset, partitionName)
        goto(GetPartition) using GotSchema(schema)
      }
  }

  when(GetPartition) {
    case Event(Partition.ThePartition(partObj), GotSchema(schema)) =>
      context.parent ! GoodToGo(partObj, schema)
      goto(Ready) using GotData(partObj, schema)
    case Event(NotFound, GotSchema(schema)) =>
      killMyself(NotFound)
  }

  when(Ready) {
    case Event(ChunkedColumns(version, (firstRowId, lastRowId), lastSequenceNo, columnsBytes),
               data: GotData) =>
      val partObj = data.partition

      // TODO: 0. Check the columns against the schema

      // 1. Figure out the shard
      partObj.shardingStrategy.getShard(partObj, firstRowId, version) match {
        case None =>
          context.parent ! ShardingError(partObj.dataset, partObj.partition, lastSequenceNo)
          goto(Ready) using data
        case Some(shardRowId) =>
          // 2. Update partition shard info if needed
          if (!partObj.contains(shardRowId, version)) {
            metadataActor ! Partition.AddShardVersion(partObj, shardRowId, version)
            // track Ack from metadataActor
          }

          // 3. Forward to dataWriterActor
          val shard = Shard(partObj, version, shardRowId)
          val writeCmd = Shard.WriteColumnData(shard, firstRowId, lastSequenceNo, columnsBytes)
          dataWriterActor ! writeCmd

          goto(Ready) using data
      }
      // TODO: stay at Ready or go to Waiting depending on how many chunks we have left to go before able to
      // send again

    case Event(ack: Shard.Ack, data: GotData) =>
      // For now, just pass the ack straight back.  In the future, we'll want to use this for throttling
      context.parent ! Ack(data.partition.dataset, data.partition.partition, ack.lastSequenceNo)
      goto(Ready) using data
  }
}