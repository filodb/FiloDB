package filodb.core.metadata

/**
 * A [[ShardingStrategy]] determines how a Partition will be sharded.
 */
sealed trait ShardingStrategy {
  def needNewShard(partition: Partition, rowId: Int): Boolean
  def serialize(): String
}

sealed trait ShardingStrategyDeserializer {
  val tag: String
  def deserialize(strs: Array[String]): ShardingStrategy
}

object ShardingStrategy {
  val DefaultNumRowsPerShard = 100000
  val DefaultStrategy = ShardByNumRows(DefaultNumRowsPerShard)

  val deserializers: Map[String, ShardingStrategyDeserializer] =
    Seq(ShardByNumRows)
      .map { deser => deser.tag -> deser }.toMap

  def deserialize(str: String): ShardingStrategy = {
    val parts = str.split(":")
    try {
      deserializers(parts(0)).deserialize(parts.tail)
    } catch {
      case e: NoSuchElementException =>
        throw new IllegalArgumentException("No such sharding strategy: " + str)
    }
  }
}

// Fixed number of rows per shard.  But how does one determine the # of rows?
// Also not very effective in terms of keeping the size the same.
case class ShardByNumRows(shardSize: Int) extends ShardingStrategy {
  def needNewShard(partition: Partition, rowId: Int): Boolean = {
    if (partition.isEmpty) { true }
    else {    // if not empty, there should be a last shard
      (rowId - partition.firstRowId.last) >= shardSize
    }
  }

  def serialize(): String = s"${ShardByNumRows.tag}:$shardSize"
}

object ShardByNumRows extends ShardingStrategyDeserializer {
  val tag = "byNumRows"
  def deserialize(strs: Array[String]): ShardingStrategy = {
    require(strs.size == 1, "ShardByNumRows expects exactly 1 argument <shardSize: Int>")
    ShardByNumRows(strs(0).toInt)
  }
}