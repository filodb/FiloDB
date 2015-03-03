package filodb.core.metadata

import org.scalatest.FunSpec
import org.scalatest.Matchers

class PartitionSpec extends FunSpec with Matchers {
  describe("Partition") {
    it("should return empty when no shards") {
      val p = Partition("foo", "first")
      p should be ('empty)

      val p2 = Partition("foo", "second", firstRowId = Seq(0L), versionRange = Seq((0, 1)))
      p2 should not be ('empty)
    }

    it("should return invalid when internal state inconsistent") {
      val p = Partition("foo", "first")
      p should be ('valid)

      val pp = Partition("foo", "second", firstRowId = Seq(0L), versionRange = Seq((0, 1)))
      p should be ('valid)

      val p2 = Partition("foo", "negChunkSize", chunkSize = -5)
      p2 should not be ('valid)

      val p3 = Partition("foo", "notIncreasingRowIds",
                         firstRowId = Seq(0L, 100L, 50L),
                         versionRange = Seq((0, 1), (0, 2), (1, 2)))
      p3 should not be ('valid)

      val p4 = Partition("foo", "shardListsUnEqual",
                         firstRowId = Seq(0L),
                         versionRange = Seq((0, 1), (1, 2)))
      p4 should not be ('valid)
    }

    it("should addShard() and return None if new shard is invalid") {
      val p = Partition("foo", "second", firstRowId = Seq(10L), versionRange = Seq((0, 1)))
      val pp = p.copy(firstRowId = p.firstRowId :+ 20L,
                      versionRange = p.versionRange :+ (1 -> 2))
      p.addShard(20, 1 -> 2) should equal (Some(pp))

      p.addShard(0, 1 -> 2) should equal (None)

      Partition("foo", "second").addShard(10, 0 -> 1) should equal (Some(p))
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
      val p = Partition("foo", "0", firstRowId = Seq(0), versionRange = Seq((0, 1)))
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