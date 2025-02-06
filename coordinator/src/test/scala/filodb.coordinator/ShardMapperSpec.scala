package filodb.coordinator

import akka.actor.ActorRef
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
    // shardKeyHash:     10001000111111100110111011011 (0x111fcddb)
    // partitionHash: 10011101001111010111011100101111 (0x9d3d772f)

    mapper1.ingestionShard(shardKeyHash, partitionHash, 0) shouldEqual 0x1b
    mapper1.ingestionShard(shardKeyHash, partitionHash, 1) shouldEqual 0x3b
    mapper1.ingestionShard(shardKeyHash, partitionHash, 2) shouldEqual 0x2b
    mapper1.ingestionShard(shardKeyHash, partitionHash, 3) shouldEqual 0x2b
    mapper1.ingestionShard(shardKeyHash, partitionHash, 4) shouldEqual 0x2f
    mapper1.ingestionShard(shardKeyHash, partitionHash, 5) shouldEqual 0x2f
    mapper1.ingestionShard(shardKeyHash, partitionHash, 6) shouldEqual 0x2f
    intercept[IllegalArgumentException] {
      mapper1.ingestionShard(shardKeyHash, partitionHash, 7)
    }
  }

  it("can assign new nodes to shards and succeed in reassignment if shards already assigned") {
    val mapper1 = new ShardMapper(64)
    mapper1.allNodes should equal (Set.empty)
    mapper1.unassignedShards should equal (0 to 63)

    mapper1.registerNode(Seq(0, 10, 20), ref1).isSuccess should be (true)
    mapper1.allNodes should equal (Set(ref1))
    mapper1.numAssignedShards should equal (3)
    mapper1.assignedShards should equal (Seq(0, 10, 20))

    mapper1.registerNode(Seq(15, 20), ref2).isSuccess should be (true)
  }

  it("can getQueryShards given a shard key and spread") {
    val mapper1 = new ShardMapper(64)
    mapper1.numAssignedShards should equal(0)
    val shardKeyHash = ("someAppName" + "$" + "someMetricName").hashCode

    // Turns out that:
    // shardKeyHash: 10001000111111100110111011011
    // & 0x03f => lower bits = 0x1b

    mapper1.registerNode(Seq(0, 10, 40, 42), ref1).isSuccess shouldBe true
    mapper1.registerNode(Seq(41, 43, 45), ref2).isSuccess shouldBe true
    mapper1.numAssignedShards shouldEqual 7

    mapper1.queryShards(shardKeyHash, 0) shouldEqual Seq(0x1b)
    mapper1.queryShards(shardKeyHash, 1) shouldEqual Seq(0x1b, 0x3b)
    mapper1.queryShards(shardKeyHash, 2) shouldEqual Seq(0x0b, 0x1b, 0x2b, 0x3b)
    mapper1.queryShards(shardKeyHash, 3) shouldEqual Seq(0x03, 0x0b, 0x13, 0x1b, 0x23, 0x2b, 0x33, 0x3b)
    mapper1.queryShards(shardKeyHash, 4) shouldEqual (0 to 15).map(_*4 + 3)
    mapper1.queryShards(shardKeyHash, 5).size shouldEqual 32
    mapper1.queryShards(shardKeyHash, 5) shouldEqual (0 to 31).map(_*2 + 1)
    mapper1.queryShards(shardKeyHash, 6).size shouldEqual 64
    mapper1.queryShards(shardKeyHash, 6) shouldEqual (0 to 63)
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
    mapper1.activeOrRecoveringShards(Seq(1, 5, 10)) shouldEqual Nil
    mapper1.notActiveShards.size shouldEqual 32

    mapper1.updateFromEvent(ShardAssignmentStarted(dataset, 2, ref1)).isSuccess shouldEqual true
    mapper1.updateFromEvent(IngestionStarted(dataset, 2, ref1)).isSuccess shouldEqual true
    mapper1.numAssignedShards shouldEqual 1
    mapper1.updateFromEvent(RecoveryInProgress(dataset, 4, ref1, 0)).isSuccess shouldEqual true
    mapper1.numAssignedShards shouldEqual 2
    mapper1.activeOrRecoveringShards(Seq(1, 2, 3, 4)) shouldEqual Seq(2, 4)
    mapper1.notActiveShards shouldEqual (Set(0,1) ++ (3 to 31).toSet  )

    mapper1.updateFromEvent(ShardAssignmentStarted(dataset, 3, ref2)).isSuccess shouldEqual true
    mapper1.updateFromEvent(IngestionStarted(dataset, 3, ref2)).isSuccess shouldEqual true
    mapper1.activeOrRecoveringShards(Seq(1, 2, 3, 4)) shouldEqual Seq(2, 3, 4)
    mapper1.numAssignedShards shouldEqual 3
    mapper1.notActiveShards.size shouldEqual 30
    println(mapper1.prettyPrint)

    mapper1.updateFromEvent(ShardDown(dataset, 4, ref1)).isSuccess shouldEqual true
    mapper1.activeOrRecoveringShards(Seq(1, 2, 3, 4)) shouldEqual Seq(2, 3)
    mapper1.numAssignedShards shouldEqual 2
    mapper1.coordForShard(4) shouldEqual ActorRef.noSender
    mapper1.notActiveShards.size shouldEqual 30
  }

  it("can produce a minimal set of events to reproduce ShardMapper state") {
    val mapper1 = new ShardMapper(32)
    mapper1.updateFromEvent(ShardAssignmentStarted(dataset, 2, ref1)).isSuccess shouldEqual true
    mapper1.updateFromEvent(IngestionStarted(dataset, 2, ref1)).isSuccess shouldEqual true
    mapper1.updateFromEvent(RecoveryInProgress(dataset, 4, ref1, 50)).isSuccess shouldEqual true
    mapper1.updateFromEvent(IngestionStarted(dataset, 3, ref2)).isSuccess shouldEqual true
    mapper1.updateFromEvent(ShardDown(dataset, 4, ref1)).isSuccess shouldEqual true

    val events = mapper1.minimalEvents(dataset)
    events should have length (4)
    events(0) shouldBe an[IngestionStarted]
    events(1) shouldBe an[IngestionStarted]

    val mapper2 = new ShardMapper(32)
    events.foreach { e => mapper2.updateFromEvent(e).get }
    mapper2 shouldEqual mapper1
  }

  it("can update status from events during shard assignment changes") {
    val numShards = 32
    val map = new ShardMapper(numShards)

    val initialShards = Seq(2, 4)
    initialShards forall { shard =>
      map.statusForShard(shard) == ShardStatusUnassigned &&
        map.updateFromEvent(ShardAssignmentStarted(dataset, shard, ref1)).isSuccess &&
        map.statusForShard(shard) == ShardStatusAssigned &&
        map.coordForShard(shard).compareTo(ref1) == 0 } shouldEqual true

    map.assignedShards shouldEqual initialShards
    map.unassignedShards.size shouldEqual numShards - initialShards.size
    map.activeOrRecoveringShards(Seq(1, 2, 3, 4)) shouldEqual Seq.empty

    map.updateFromEvent(IngestionStarted(dataset, 2, ref1)).isSuccess shouldEqual true
    map.activeOrRecoveringShards(Seq(1, 2, 3, 4)) shouldEqual Seq(2)

    val nextShards = Seq(1, 3)
    nextShards forall { shard =>
      map.statusForShard(shard) == ShardStatusUnassigned &&
        map.updateFromEvent(ShardAssignmentStarted(dataset, shard, ref2)).isSuccess &&
        map.statusForShard(shard) == ShardStatusAssigned &&
        map.coordForShard(shard).compareTo(ref2) == 0 &&
        map.updateFromEvent(IngestionStarted(dataset, shard, ref2)).isSuccess } shouldEqual true

    map.assignedShards shouldEqual (initialShards ++ nextShards).sorted
    map.unassignedShards.size shouldEqual numShards - (initialShards.size + nextShards.size)

    map.activeOrRecoveringShards(Seq(1, 2, 3, 4)) shouldEqual Seq(1, 2, 3)
    map.numAssignedShards shouldEqual initialShards.size + nextShards.size

    map.updateFromEvent(IngestionError(dataset, 3, new java.io.IOException("ingestion fu"))).isSuccess shouldEqual true
    map.updateFromEvent(RecoveryStarted(dataset, 3, ref2, 0)).isSuccess shouldEqual true
    map.numAssignedShards shouldEqual initialShards.size + nextShards.size

    map.updateFromEvent(ShardDown(dataset, 3, ref2)).isSuccess shouldEqual true
    // Even when down, should be able to still access the node ref
    // so when would be free the shard for re-assignment other than a planned stop or end of a stream
    map.coordForShard(3) shouldEqual ActorRef.noSender
    map.statusForShard(3) == ShardStatusUnassigned
  }

  it("should be idempotent for shard already assigned to the given coordinator for transitional status") {
    val numShards = 32
    val map = new ShardMapper(numShards)
    val shards = Seq(1, 2)
    shards forall { shard =>
      map.statusForShard(shard) == ShardStatusUnassigned &&
        map.updateFromEvent(ShardAssignmentStarted(dataset, shard, ref2)).isSuccess &&
        map.statusForShard(shard) == ShardStatusAssigned &&
        map.coordForShard(shard).compareTo(ref2) == 0 &&
        map.updateFromEvent(IngestionStarted(dataset, shard, ref2)).isSuccess } shouldEqual true

    map.updateFromEvent(ShardAssignmentStarted(dataset, 2, ref2))
    map.updateFromEvent(IngestionStarted(dataset, 2, ref2))
    map.updateFromEvent(RecoveryStarted(dataset, 2, ref2, 0))
  }

  it("should register node if a second assignment is attempted") {
    val numShards = 32
    val map = new ShardMapper(numShards)
    map.statusForShard(1) == ShardStatusUnassigned
    map.updateFromEvent(ShardAssignmentStarted(dataset, 1, ref2)).isSuccess shouldEqual true
    map.statusForShard(1) shouldEqual ShardStatusAssigned
    map.coordForShard(1) shouldEqual ref2
    //this means a re-assignment
    map.updateFromEvent(IngestionStarted(dataset, 1, ref2)).isSuccess shouldEqual true
    map.updateFromEvent(ShardAssignmentStarted(dataset, 1, ref1)).isSuccess shouldEqual true
  }

  it("should register node if a second assignment is attempted2") {
    val numShards = 32
    val map = new ShardMapper(numShards)
    map.statusForShard(1) shouldEqual ShardStatusUnassigned
    map.coordForShard(1)

    map.updateFromEvent(ShardAssignmentStarted(dataset, 1, ref1)).isSuccess shouldEqual true
    map.statusForShard(1) shouldEqual ShardStatusAssigned
    map.coordForShard(1) shouldEqual ref1
    map.updateFromEvent(ShardAssignmentStarted(dataset, 1, ref2)).isSuccess shouldEqual true
  }

  it("should set shard status to ShardStatusError if updated with IngestionError") {
    val numShards = 32
    val map = new ShardMapper(numShards)
    map.updateFromEvent(ShardAssignmentStarted(dataset, 1, ref1))
    map.updateFromEvent(IngestionStarted(dataset, 1, ref1))
    map.updateFromEvent(IngestionError(dataset, 1, new java.io.IOException("e")))
    map.statusForShard(1) shouldEqual ShardStatusError
  }

  it("should be idempotent for registerNode and assign/unassign/register/unregister as expected") {
    val coord1 = TestProbe().ref
    val coord2 = TestProbe().ref
    val coord3 = TestProbe().ref
    val numShards = 32
    val map = new ShardMapper(numShards)

    def assert(coord: ActorRef, shards: Seq[Int], numAssignedShards: Int, unassignedShards: Int): Boolean =
      shards.forall(map.coordForShard(_) == coord1) &&
        map.shardsForAddress(coord.path.address) == shards &&
        map.numAssignedShards == numAssignedShards &&
        map.unassignedShards.size == unassignedShards

    // ShardAssignmentStrategy.{datasetAdded, nodeAdded} => addShards
    map.registerNode(Seq(0, 1), coord1).isSuccess
    // expected on first register
    assert(coord = coord1, shards = Seq(0, 1), numAssignedShards = 2, unassignedShards = 30)

    // idempotent, same coord, same shards
    map.registerNode(Seq(0, 1), coord1).isSuccess
    assert(coord = coord1, shards = Seq(0, 1), numAssignedShards = 2, unassignedShards = 30)

    // idempotent - update status, calls registerNode()
    Seq(0, 1) forall { shard =>
      map.updateFromEvent(ShardAssignmentStarted(dataset, shard, coord1)).isSuccess } shouldEqual true
    Seq(0, 1) forall { shard =>
      map.updateFromEvent(IngestionStarted(dataset, shard, coord1)).isSuccess } shouldEqual true
    Seq(0, 1).forall(map.activeOrRecoveringShard) shouldEqual true
    Seq(0, 1) forall { shard =>
      map.updateFromEvent(RecoveryStarted(dataset, shard, coord1, 0)).isSuccess } shouldEqual true
    assert(coord = coord1, shards = Seq(0, 1), numAssignedShards = 2, unassignedShards = 30)

    // shards should be reassigned
    map.registerNode(Seq(1), coord2).isSuccess shouldEqual true
    Seq(1) forall { shard =>
      map.updateFromEvent(ShardAssignmentStarted(dataset, shard, coord2)).isSuccess } shouldEqual true
    assert(coord = coord1, shards = Seq(1), numAssignedShards = 2, unassignedShards = 30)

    // second coord
    map.registerNode(Seq(2, 3), coord2).isSuccess  shouldEqual true
    assert(coord = coord2, shards = Seq(2, 3), numAssignedShards = 4, unassignedShards = 28)

    // second coord shards not reassigned
    map.registerNode(Seq(3), coord1).isSuccess  shouldEqual true
    assert(coord = coord2, shards = Seq(3), numAssignedShards = 2, unassignedShards = 28)

    Seq(0, 3).forall(s => map.coordForShard(s) == coord1) shouldEqual true
    Seq(1, 2).forall(s => map.coordForShard(s) == coord2) shouldEqual true

    // remove first, unassign shards
    map.unassignedShards.size shouldEqual 28
    map.numAssignedShards shouldEqual 4
    map.removeNode(coord1) shouldEqual Seq(0, 3)
    map.unassignedShards.size shouldEqual 30
    map.numAssignedShards shouldEqual 2
    Seq(0, 3).forall(s => Option(map.coordForShard(s)).isEmpty) shouldEqual true

    // now re-assign free shards to new coord
    val newCoord = TestProbe().ref
    map.registerNode(Seq(0, 3), newCoord).isSuccess
    Seq(0, 3).forall(s => map.coordForShard(s) == newCoord) shouldEqual true
    assert(coord = newCoord, shards = Seq(0, 3), numAssignedShards = 4, unassignedShards = 28)
  }

  it ("test bitmap conversion of shard mapper") {
    val numShards = 32
    val shardMapper = new ShardMapper(numShards) // default init to ShardStatusUnassigned
    var bitRep = ShardMapperV2.shardMapperBitMapRepresentation(shardMapper)
    bitRep.length shouldEqual 4
    bitRep.forall (x => x == 0x00.toByte) shouldEqual true // no bit should be set at this point

    // move everyone to assigned
    shardMapper.registerNode(shardMapper.statuses.indices, TestProbe().ref)
    shardMapper.assignedShards.length shouldEqual 32

    // status updated to assigned but bitmap representation should NOT yet be set to 1
    bitRep = ShardMapperV2.shardMapperBitMapRepresentation(shardMapper)
    bitRep.length shouldEqual 4
    bitRep.forall (x => x == 0x00.toByte) shouldEqual true // no bit should be set at this point

    // move first 8 and last 8 shards to active
    for (i <- 0 to 7) {
      shardMapper.updateFromEvent(IngestionStarted(dataset, i, TestProbe().ref))
    }
    for (i <- 24 to 31) {
      shardMapper.updateFromEvent(IngestionStarted(dataset, i, TestProbe().ref))
    }
    shardMapper.activeShards().size shouldEqual 16
    bitRep = ShardMapperV2.shardMapperBitMapRepresentation(shardMapper)
    // 1111 1111    0000 0000    0000 0000    1111 1111
    bitRep(0) shouldEqual 0xFF.toByte
    bitRep(1) shouldEqual 0x00.toByte
    bitRep(2) shouldEqual 0x00.toByte
    bitRep(3) shouldEqual 0xFF.toByte
  }

  it ("test bitmap conversion of shard mapper with 256 shards") {
    val shardMapper = new ShardMapper(256) // default init to ShardStatusUnassigned
    var bitRep = ShardMapperV2.shardMapperBitMapRepresentation(shardMapper)
    bitRep.length shouldEqual 32
    bitRep.forall (x => x == 0x00.toByte) shouldEqual true // no bit should be set at this point
    shardMapper.registerNode(shardMapper.statuses.indices, TestProbe().ref)
    shardMapper.assignedShards.length shouldEqual 256

    // make all shards active
    for (i <- 0 to 255) {
      shardMapper.updateFromEvent(IngestionStarted(dataset, i, TestProbe().ref))
    }
    // check if all the bits are set correctly
    bitRep = ShardMapperV2.shardMapperBitMapRepresentation(shardMapper)
    bitRep.forall (x => x == 0xFF.toByte) shouldEqual true

    // make some shards in recovery mode
    for (i <- 60 to 63) {
      shardMapper.updateFromEvent(RecoveryInProgress(dataset, i, TestProbe().ref, 50))
    }
    // make some shards in down mode
    for (i <- 64 to 67) {
      shardMapper.updateFromEvent(ShardDown(dataset, i, TestProbe().ref))
    }

    shardMapper.activeShards().size shouldEqual 248
    shardMapper.notActiveShards().size shouldEqual 8
    bitRep = ShardMapperV2.shardMapperBitMapRepresentation(shardMapper)

    // first 60 shards are active
    for (i <- 0 to 6) {
      bitRep(i) shouldEqual 0xFF.toByte
    }

    // shards 56-63 should be 1111 0000
    bitRep(7) shouldEqual 0xF0.toByte

    // shards 64-71 should be 0000 1111
    bitRep(8) shouldEqual 0x0F.toByte

    // last 188 shards are active
    for (i <- 9 to 31) {
      bitRep(i) shouldEqual 0xFF.toByte
    }
  }

  it ("test padding is set correctly in non 8 byte aligned number of shards") {
    val shardMapper = new ShardMapper(2) // default init to ShardStatusUnassigned
    var bitRep = ShardMapperV2.shardMapperBitMapRepresentation(shardMapper)
    bitRep.length shouldEqual 1
    bitRep.forall (x => x == 0x00.toByte) shouldEqual true // no bit should be set at this point
    shardMapper.registerNode(shardMapper.statuses.indices, TestProbe().ref)
    shardMapper.assignedShards.length shouldEqual 2
    for (i <- 0 to 1) {
      shardMapper.updateFromEvent(IngestionStarted(dataset, i, TestProbe().ref))
    }
    bitRep = ShardMapperV2.shardMapperBitMapRepresentation(shardMapper)
    bitRep(0) shouldEqual 0xC0.toByte // 1100 0000 - padding for last 6 shards
    shardMapper.updateFromEvent(ShardDown(dataset, 1, TestProbe().ref))

    bitRep = ShardMapperV2.shardMapperBitMapRepresentation(shardMapper)
    bitRep(0) shouldEqual 0x80.toByte // 1000 0000
  }
}
