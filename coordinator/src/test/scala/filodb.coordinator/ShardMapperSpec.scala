package filodb.coordinator

import akka.testkit._

import filodb.core._

object ShardMapperSpec extends ActorSpecConfig

class ShardMapperSpec extends ActorTest(ShardMapperSpec.getNewSystem) {
  import ShardMapper.ShardAndNode

  val ref1 = TestProbe().ref
  val ref2 = TestProbe().ref
  val dataset = DatasetRef("foo")

  it("fails if numShards is not a power of 2") {
    intercept[IllegalArgumentException] {
      new ShardMapper(34)
    }
  }

  it("can getIngestionShard using different spread values") {
    val mapper1 = new ShardMapper(64)
    mapper1.numAssignedShards should equal (0)
    val shardKeyHash = ("someAppName" + "$" + "someMetricName").hashCode
    val partitionHash = "a=1;b=2;c=3".hashCode

    // Turns out that:
    // shardKeyHash: 10001000111111100110111011011
    // partitionHash: 10011101001111010111011100101111

    mapper1.ingestionShard(shardKeyHash, partitionHash, 0) shouldEqual 27
    mapper1.ingestionShard(shardKeyHash, partitionHash, 1) shouldEqual 27
    mapper1.ingestionShard(shardKeyHash, partitionHash, 2) shouldEqual 27
    mapper1.ingestionShard(shardKeyHash, partitionHash, 3) shouldEqual 31
    mapper1.ingestionShard(shardKeyHash, partitionHash, 4) shouldEqual 31
    mapper1.ingestionShard(shardKeyHash, partitionHash, 5) shouldEqual 15
    mapper1.ingestionShard(shardKeyHash, partitionHash, 6) shouldEqual 47
    intercept[IllegalArgumentException] {
      mapper1.ingestionShard(shardKeyHash, partitionHash, 7)
    }
  }

  it("can assign new nodes to shards and fail if shards already assigned") {
    val mapper1 = new ShardMapper(64)
    mapper1.allNodes should equal (Set.empty)
    mapper1.unassignedShards should equal (0 to 63)

    mapper1.registerNode(Seq(0, 10, 20), ref1).isSuccess should be (true)
    mapper1.allNodes should equal (Set(ref1))
    mapper1.numAssignedShards should equal (3)
    mapper1.assignedShards should equal (Seq(0, 10, 20))

    mapper1.registerNode(Seq(15, 20), ref2).isSuccess should be (false)
  }

  it("can getQueryShards given a shard key and spread") {


    val mapper1 = new ShardMapper(64)
    mapper1.numAssignedShards should equal(0)
    val shardKeyHash = ("someAppName" + "$" + "someMetricName").hashCode

    // Turns out that:
    // shardKeyHash: 10001000111111100110111011011

    mapper1.registerNode(Seq(0, 10, 40, 42), ref1).isSuccess shouldBe true
    mapper1.registerNode(Seq(41, 43, 45), ref2).isSuccess shouldBe true
    mapper1.numAssignedShards shouldEqual 7

    mapper1.queryShards(shardKeyHash, 0) shouldEqual Seq(27)
    mapper1.queryShards(shardKeyHash, 1) shouldEqual Seq(26, 27)
    mapper1.queryShards(shardKeyHash, 2) shouldEqual Seq(24, 25, 26, 27)
    mapper1.queryShards(shardKeyHash, 3) shouldEqual Seq(24, 25, 26, 27, 28, 29, 30, 31)
    mapper1.queryShards(shardKeyHash, 4) shouldEqual
      Seq(16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31)
    mapper1.queryShards(shardKeyHash, 5).size shouldEqual 32
    mapper1.queryShards(shardKeyHash, 5).contains(15) shouldBe true
    mapper1.queryShards(shardKeyHash, 6).size shouldEqual 64
    mapper1.queryShards(shardKeyHash, 6).contains(47) shouldBe true
    intercept[IllegalArgumentException] {
      mapper1.queryShards(shardKeyHash, 7)
    }

  }

  it("can return shards and nodes given a partition key") {
    val mapper1 = new ShardMapper(64)
    mapper1.registerNode(Seq(0, 10, 20), ref1).isSuccess should be (true)
    mapper1.registerNode(Seq(21, 22), ref2).isSuccess should be (true)

    mapper1.partitionToShardNode("123456".hashCode) should equal (ShardAndNode(21, ref2))
  }

  it("can remove new nodes added and return which shards removed") {
    val mapper1 = new ShardMapper(64)
    mapper1.registerNode(Seq(0, 10, 20), ref1).isSuccess should be (true)
    mapper1.unassignedShards.length should equal (61)

    mapper1.removeNode(ref2) should equal (Nil)
    mapper1.removeNode(ref1) should equal (Seq(0, 10, 20))
    mapper1.unassignedShards.length should equal (64)
    mapper1.removeNode(ref1) should equal (Nil)
  }

  it("can update status from events and filter shards by status") {
    val mapper1 = new ShardMapper(32)
    mapper1.numAssignedShards shouldEqual 0
    mapper1.activeShards(Seq(1, 5, 10)) shouldEqual Nil

    mapper1.updateFromEvent(IngestionStarted(dataset, 2, ref1)).isSuccess shouldEqual true
    mapper1.updateFromEvent(RecoveryStarted(dataset, 4, ref1, 0)).isSuccess shouldEqual true
    mapper1.numAssignedShards shouldEqual 2
    mapper1.activeShards(Seq(1, 2, 3, 4)) shouldEqual Seq(2, 4)

    mapper1.updateFromEvent(IngestionStarted(dataset, 3, ref2)).isSuccess shouldEqual true
    mapper1.updateFromEvent(ShardDown(dataset, 4)).isSuccess shouldEqual true
    mapper1.numAssignedShards shouldEqual 3
    mapper1.activeShards(Seq(1, 2, 3, 4)) shouldEqual Seq(2, 3)

    // Even when down, should be able to still access the node ref
    mapper1.coordForShard(4) shouldEqual ref1
  }

  it("can produce a minimal set of events to reproduce ShardMapper state") {
    val mapper1 = new ShardMapper(32)
    mapper1.updateFromEvent(IngestionStarted(dataset, 2, ref1)).isSuccess shouldEqual true
    mapper1.updateFromEvent(RecoveryStarted(dataset, 4, ref1, 50)).isSuccess shouldEqual true
    mapper1.updateFromEvent(IngestionStarted(dataset, 3, ref2)).isSuccess shouldEqual true
    mapper1.updateFromEvent(ShardDown(dataset, 4)).isSuccess shouldEqual true

    val events = mapper1.minimalEvents(dataset)
    events should have length (4)
    events(0) shouldBe an[IngestionStarted]
    events(1) shouldBe an[IngestionStarted]

    val mapper2 = new ShardMapper(32)
    events.foreach { e => mapper2.updateFromEvent(e).get }
    mapper2 shouldEqual mapper1
  }
}