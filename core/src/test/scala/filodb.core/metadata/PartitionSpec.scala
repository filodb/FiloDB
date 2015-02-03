package filodb.core.metadata

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers

class PartitionSpec extends FunSpec with ShouldMatchers {
  describe("Partition") {
    it("should return empty when last row is negative") {
      val p = Partition("foo", "first")
      p should be ('empty)

      val p2 = Partition("foo", "second", 100)
      p2 should not be ('empty)
    }

    it("should return invalid when internal state inconsistent") {
      val p = Partition("foo", "first")
      p should be ('valid)

      val p2 = Partition("foo", "second", 100)
      p2 should not be ('valid)

      val p3 = Partition("foo", "nonEmptyShards", firstRowId = Seq(0, 100))
      p3 should not be ('valid)

      val p4 = Partition("foo", "shardListsUnEqual", 100,
                         firstRowId = Seq(0),
                         firstVersion = Seq(0, 1))
      p4 should not be ('valid)
    }
  }

  describe("ShardByNumRows.needNewShard") {
    it("should return true if Partition is empty") {
      val empty = Partition("foo", "first")
      val strategy = ShardByNumRows(100)
      strategy.needNewShard(empty, 0) should equal (true)
    }

    it("should return true if last row ID greater than shard start row ID by X") {
      val p = Partition("foo", "0", 19, firstRowId = Seq(0), firstVersion = Seq(0))
      val strategy = ShardByNumRows(100)
      strategy.needNewShard(p, 40) should equal (false)
      strategy.needNewShard(p, 100) should equal (true)
    }

    it("can serialize and deserialize") {
      val strategy = ShardByNumRows(100)
      ShardingStrategy.deserialize(strategy.serialize()) should equal (strategy)
    }
  }
}