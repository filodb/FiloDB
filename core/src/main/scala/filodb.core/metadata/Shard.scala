package filodb.core.metadata

import filodb.core.messages.{Command, Response, ErrorResponse}
import java.nio.ByteBuffer

// Classes that deal with each shard of data within a partition
//
case class Shard(partition: Partition, version: Int, firstRowId: Long)

case class ShardState(shard: Shard, columnsWritten: Set[String])

object Shard {
  /**
   * Updates the columns written metadata in the shard.
   * @return ColumnsWrittenUpdated(newState), Nop if no change is required
   * TODO: maybe get rid of this.  Have storage impl take care of updating columnswritten state.
   *       Querying the currently written columns sounds like fair game though.
   */
  case class UpdateColumnsWritten(state: ShardState, writtenColumns: Set[String]) extends Command

  sealed trait WriterResponse extends Response
  case object Ready extends WriterResponse
  case class Ack(lastSequenceNo: Long) extends WriterResponse
  case object ChunkTooBig extends ErrorResponse with WriterResponse
  case object ChunkMisaligned extends ErrorResponse with WriterResponse
  case class ColumnsWrittenUpdated(newState: ShardState) extends WriterResponse
  case object Nop extends WriterResponse
}