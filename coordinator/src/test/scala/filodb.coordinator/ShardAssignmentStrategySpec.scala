package filodb.coordinator

import akka.testkit.TestProbe

import filodb.coordinator.NodeClusterActor.DatasetResourceSpec
import filodb.core.DatasetRef

class ShardAssignmentStrategySpec extends AkkaSpec {

  import DefaultShardAssignmentStrategy._

  val dataset = DatasetRef("dataset")

  val coord1 = TestProbe()
  val coord2 = TestProbe()
  val coord3 = TestProbe()
  val coord4 = TestProbe()
  val coord5 = TestProbe()

  "DefaultShardAssignmentStrategy" must {

    "distribute shards evenly among coords - test case 1" in {
      val numShards = 8
      val numCoords = 3
      val resources = DatasetResourceSpec(numShards, numCoords)
      val mapper = new ShardMapper(numShards)

      val assignment1 = shardAssignments(coord1.ref, dataset, resources, mapper)
      assignment1 shouldEqual Seq(0, 1, 2)
      mapper.registerNode(assignment1, coord1.ref)

      val assignment2 = shardAssignments(coord2.ref, dataset, resources, mapper)
      assignment2 shouldEqual Seq(3, 4, 5)
      mapper.registerNode(assignment2, coord2.ref)

      val assignment3 = shardAssignments(coord3.ref, dataset, resources, mapper)
      assignment3 shouldEqual Seq(6, 7)
      mapper.registerNode(assignment3, coord3.ref)

    }

    "distribute shards evenly among coords - test case 2" in {
      val numShards = 16
      val numCoords = 5
      val resources = DatasetResourceSpec(numShards, numCoords)
      val mapper = new ShardMapper(numShards)

      val assignment1 = shardAssignments(coord1.ref, dataset, resources, mapper)
      assignment1 shouldEqual Seq(0, 1, 2, 3)
      mapper.registerNode(assignment1, coord1.ref)

      val assignment2 = shardAssignments(coord2.ref, dataset, resources, mapper)
      assignment2 shouldEqual Seq(4, 5, 6)
      mapper.registerNode(assignment2, coord2.ref)

      val assignment3 = shardAssignments(coord3.ref, dataset, resources, mapper)
      assignment3 shouldEqual Seq(7, 8, 9)
      mapper.registerNode(assignment3, coord3.ref)

      val assignment4 = shardAssignments(coord4.ref, dataset, resources, mapper)
      assignment4 shouldEqual Seq(10, 11, 12)
      mapper.registerNode(assignment4, coord4.ref)

      val assignment5 = shardAssignments(coord5.ref, dataset, resources, mapper)
      assignment5 shouldEqual Seq(13, 14, 15)
      mapper.registerNode(assignment5, coord5.ref)
    }

    "distribute shards evenly among coords - test case 3" in {
      val numShards = 2
      val numCoords = 2
      val resources = DatasetResourceSpec(numShards, numCoords)
      val mapper = new ShardMapper(numShards)

      val assignment1 = shardAssignments(coord1.ref, dataset, resources, mapper)
      assignment1 shouldEqual Seq(0)
      mapper.registerNode(assignment1, coord1.ref)

      val assignment2 = shardAssignments(coord2.ref, dataset, resources, mapper)
      assignment2 shouldEqual Seq(1)
      mapper.registerNode(assignment2, coord2.ref)

    }

    "not assign shards after all shard are assigned" in {
      val numShards = 2
      val numCoords = 3
      val resources = DatasetResourceSpec(numShards, numCoords)
      val mapper = new ShardMapper(numShards)

      val assignment1 = shardAssignments(coord1.ref, dataset, resources, mapper)
      assignment1 shouldEqual Seq(0)
      mapper.registerNode(assignment1, coord1.ref)

      val assignment2 = shardAssignments(coord2.ref, dataset, resources, mapper)
      assignment2 shouldEqual Seq(1)
      mapper.registerNode(assignment2, coord2.ref)

      // since all shards are assigned, we should get empty when we try to assign more
      val assignment3 = shardAssignments(coord3.ref, dataset, resources, mapper)
      assignment3 shouldEqual Seq.empty
    }

    "not assign more shards to a coord with shards already assigned" in {
      val numShards = 8
      val numCoords = 3
      val resources = DatasetResourceSpec(numShards, numCoords)
      val mapper = new ShardMapper(numShards)

      val assignment1 = shardAssignments(coord1.ref, dataset, resources, mapper)
      assignment1 shouldEqual Seq(0, 1, 2)
      mapper.registerNode(assignment1, coord1.ref)

      shardAssignments(coord1.ref, dataset, resources, mapper) shouldEqual Seq.empty
    }

    "but assign more shards to a coord with shards already assigned if there is capacity" in {
      val numShards = 4
      val numCoords = 3
      val resources = DatasetResourceSpec(numShards, numCoords)
      val mapper = new ShardMapper(numShards)

      val assignment1 = shardAssignments(coord1.ref, dataset, resources, mapper)
      assignment1 shouldEqual Seq(0, 1)
      mapper.registerNode(assignment1, coord1.ref)

      val assignment2 = shardAssignments(coord2.ref, dataset, resources, mapper)
      assignment2 shouldEqual Seq(2) // this node has capacity for one more
      mapper.registerNode(assignment2, coord2.ref)

      val assignment3 = shardAssignments(coord3.ref, dataset, resources, mapper)
      mapper.registerNode(assignment3, coord3.ref)
      assignment3 shouldEqual Seq(3) // this node has capacity for one more

      mapper.removeNode(coord1.ref)

      // coord2 can take one more node
      val assignment4 = shardAssignments(coord2.ref, dataset, resources, mapper)
      assignment4 shouldEqual Seq(0)
      mapper.registerNode(assignment4, coord2.ref)

      // however now that coord2 has 2 shards, we cannot have 2 shards on second node
      shardAssignments(coord3.ref, dataset, resources, mapper) shouldEqual Seq.empty
    }

    "not assign shards to spare nodes unless shards become available subsequently" in {
      val numShards = 4
      val numCoords = 2
      val resources = DatasetResourceSpec(numShards, numCoords)
      val mapper = new ShardMapper(numShards)

      val assignment1 = shardAssignments(coord1.ref, dataset, resources, mapper)
      assignment1 shouldEqual Seq(0, 1)
      mapper.registerNode(assignment1, coord1.ref)

      val assignment2 = shardAssignments(coord2.ref, dataset, resources, mapper)
      assignment2 shouldEqual Seq(2, 3)
      mapper.registerNode(assignment2, coord2.ref)

      val assignment3 = shardAssignments(coord3.ref, dataset, resources, mapper)
      assignment3 shouldEqual Seq.empty

      // say coord1 went down
      val removedShards = mapper.removeNode(coord1.ref)

      val assignment4 = shardAssignments(coord3.ref, dataset, resources, mapper)
      assignment4 shouldEqual removedShards
    }
  }
}
