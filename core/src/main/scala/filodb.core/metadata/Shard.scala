package filodb.core.metadata

import filodb.core.messages.{Command, Response, ErrorResponse}
import java.nio.ByteBuffer

// Classes that deal with each shard of data within a partition
//
case class Shard(partition: Partition, version: Int, firstRowId: Long)

case class ShardState(shard: Shard, columnsWritten: Set[String])

object Shard {
  /**
   * Commands for the DataWriterActor.  Note: these are distinct from the commands for the MetadataActor.
   *
   * Why are these not in some DataWriterActor file?  Because there could be multiple implementations.
   */
  sealed trait WriterCommand extends Command

  /**
   * Writes a chunk of columnar data.  Only write it when the sender has received a Ready signal.
   * @param shard the Shard to write to
   * @param rowIdRange the starting and ending RowId for the chunk. Must be aligned to chunkSize and be
   *                   no more than chunkSize rows.
   * @param columnsBytes the column name and bytes to be written for each column
   * @return Ack() for a successful write, or any number of errors
   */
  case class WriteColumnData(shard: Shard,
                             rowIdRange: (Long, Long),
                             columnsBytes: Map[String, ByteBuffer]) extends WriterCommand

  /**
   * Updates the columns written metadata in the shard.
   * @return ColumnsWrittenUpdated(newState), Nop if no change is required
   */
  case class UpdateColumnsWritten(state: ShardState, writtenColumns: Set[String]) extends WriterCommand

  sealed trait WriterResponse extends Response
  case object Ready extends WriterResponse
  case class Ack(firstRowId: Long, lastRowId: Long) extends WriterResponse
  case object ChunkTooBig extends ErrorResponse with WriterResponse
  case object ChunkMisaligned extends ErrorResponse with WriterResponse
  case class ColumnsWrittenUpdated(newState: ShardState) extends WriterResponse
  case object Nop extends WriterResponse
}