package filodb.core.metadata

/**
 * A [[ShardingStrategy]] determines how a Partition will be sharded.
 * Shards are necessary to limit physical row length and distribute the data.
 * Shards are an internal implementation detail.
 *
 * It maps a rowId to a shard, and determines if a new shard is needed.
 */
sealed trait ShardingStrategy {
  def needNewShard(partition: Partition, rowId: Long): Boolean
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

/**
 * [[ShardByNumRows]] is super simple fixed hashing by number of rows.
 * Each shard starts at a multiple of the shardSize.
 * It works for both append and random writes, but determining the shard size
 * is tricky.   Using the default will lead to widely varying shard sizes
 * between different datasets, possibly leading to memory/latency issues.
 *
 * TODO: Estimating the shardSize based on a schema.
 */
case class ShardByNumRows(shardSize: Int) extends ShardingStrategy {
  def needNewShard(partition: Partition, rowId: Long): Boolean = {
    if (partition.isEmpty) { true }
    else {    // if not empty, there should be a last shard
      (rowId - partition.firstRowId.last) >= shardSize.toLong
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