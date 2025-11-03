package filodb.core.query

case class ShardInfo(active: Boolean, address: String)

case class ActiveShardMapper(shards: Array[ShardInfo]) {
  def allShardsActive: Boolean = (shards.size > 0) && shards.forall(_.active)
  def activeShards: Seq[Int] = shards.zipWithIndex.filter(_._1.active).map(_._2).toIndexedSeq
}

sealed trait FailoverMode
case object LegacyFailoverMode extends FailoverMode
case object ShardLevelFailoverMode extends FailoverMode

