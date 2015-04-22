package filodb.core.metadata

import filodb.core.messages.{Command, Response, ErrorResponse}
import java.nio.ByteBuffer

// Classes that deal with each shard of data within a partition
//
case class Shard(partition: Partition, version: Int, firstRowId: Long)

case class ShardState(shard: Shard, columnsWritten: Set[String])
