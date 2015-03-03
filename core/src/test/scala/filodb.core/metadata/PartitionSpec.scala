package filodb.core.metadata

import org.scalatest.FunSpec
import org.scalatest.Matchers

class PartitionSpec extends FunSpec with Matchers {
  describe("Partition") {
    it("should return empty when no shards") {
      val p = Partition("foo", "first")
      p should be ('empty)

      val p2 = Partition("foo", "second", shardVersions = Map(0L -> (0 -> 1)))
      p2 should not be ('empty)
    }

    it("should return invalid when bad state") {
      val pp = Partition("foo", "first", shardVersions = Map(0L -> (0 -> 1)))
      pp should be ('valid)

      val p2 = Partition("foo", "negChunkSize", chunkSize = -5)
      p2 should not be ('valid)
    }

    it("should contains() when shard and version are part of Partition") {
      val p = Partition("foo", "second", shardVersions = Map(10L -> (3 -> 8)))
      p.contains(0L, 0) should equal (false)
      p.contains(10L, 3) should equal (true)
      p.contains(10L, 8) should equal (true)
      p.contains(10L, 9) should equal (false)
    }

    it("should addShardVersion() successfully") {
      val p = Partition("foo", "second", shardVersions = Map(10L -> (0 -> 1)))

      // Adding a shard and version already there = identical Partition
      p.addShardVersion(10L, 0) should equal (p)
      //
      val pp = p.copy(shardVersions = p.shardVersions + (20L -> (1 -> 1)))
      p.addShardVersion(20L, 1) should equal (pp)

      Partition("foo", "second").addShardVersion(10L, 0).addShardVersion(10L, 1) should equal (p)
    }
  }

  describe("ShardByNumRows.getShard") {
    it("should return None if rowId or version is invalid") {
      val empty = Partition("foo", "first")
      val strategy = ShardByNumRows(100)
      strategy.getShard(empty, -1L, 0) should equal (None)
      strategy.getShard(empty, 100L, -1) should equal (None)
    }

    it("should return shard firstRowId per shard size rounding") {
      val p = Partition("foo", "0", shardVersions = Map(0L -> (0 -> 1)))
      val strategy = ShardByNumRows(100)
      strategy.getShard(p, 40L, 0) should equal (Some(0L))
      strategy.getShard(p, 103L, 0) should equal (Some(100L))
      strategy.getShard(p, 10020L, 0) should equal (Some(10000L))
    }

    it("can serialize and deserialize") {
      val strategy = ShardByNumRows(100)
      ShardingStrategy.deserialize(strategy.serialize()) should equal (strategy)
    }
  }
}