package filodb.coordinator

import akka.testkit._

import filodb.core._

object ShardMapperSpec extends ActorSpecConfig

class ShardMapperSpec extends ActorTest(ShardMapperSpec.getNewSystem) {
  import ShardMapper.ShardAndNode

  val ref1 = TestProbe().ref
  val ref2 = TestProbe().ref
  val dataset = DatasetRef("foo")

  it("can hashToShard using different number of bits") {
    val mapper1 = new ShardMapper(64)
    mapper1.numAssignedShards should equal (0)
    mapper1.hashToShard("banana".hashCode, 123456, 4) should equal (40)
    mapper1.hashToShard("banana".hashCode, 123456, 0) should equal (0)
    mapper1.hashToShard("banana".hashCode, 123456, 5) should equal (42)

    mapper1.hashToShard("banana".hashCode, "123456".hashCode, 4) should equal (41)
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

  it("can find shards given a shard key and number of bits") {
    val mapper1 = new ShardMapper(64)
    mapper1.registerNode(Seq(0, 10, 40, 42), ref1).isSuccess should be (true)
    mapper1.registerNode(Seq(41, 43, 45), ref2).isSuccess should be (true)
    mapper1.numAssignedShards should equal (7)

    mapper1.shardKeyToShards("banana".hashCode, 4) should equal (Seq(40, 41, 42, 43))
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
    mapper1.updateFromEvent(RecoveryStarted(dataset, 4, ref1)).isSuccess shouldEqual true
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
    mapper1.updateFromEvent(RecoveryStarted(dataset, 4, ref1)).isSuccess shouldEqual true
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