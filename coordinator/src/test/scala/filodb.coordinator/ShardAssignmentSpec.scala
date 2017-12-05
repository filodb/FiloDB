package filodb.coordinator

class ShardAssignmentSpec extends ShardCoordinatorSpec {
  import ShardAssignmentStrategy._

  // 4 shards to a node, 8 shards total per dataset
  val emptyMaps = Map(dataset1 -> new ShardMapper(8),
                      dataset2 -> new ShardMapper(8))
  val resourceMap = Map(dataset1 -> DatasetResources(4.0),
                        dataset2 -> DatasetResources(4.0))

  "ShardAssignmentStrategy" must {
    "assign new node shards when no shards assigned" in {
      val strategy = new DefaultShardAssignmentStrategy
      // Since there are two datasets, shards for both datasets will get assigned.
      val added = strategy.nodeAdded(localCoordinator, emptyMaps, resourceMap)
      added.node shouldEqual localCoordinator
      added.shards should have length 2
      val info = added.shards.head
      info.shards shouldEqual Seq(0, 1, 2, 3)

      added.shards.last.shards shouldEqual Seq(0, 1, 2, 3)

      // Test that calling this again does not change anything, produces same results (idempotency)
      // This makes sure that nothing incld shard maps is mutated
      val added2 = strategy.nodeAdded(localCoordinator, emptyMaps, resourceMap)
      added2 shouldEqual added
    }

    "assign new node shards when shards already assigned and still room" in {
      // We need shardmappers that already have one coordinator assigned
      val partialMap1 = ShardMapper.copy(emptyMaps(dataset1), dataset1)
      (0 to 3).foreach { s => partialMap1.updateFromEvent(IngestionStarted(dataset1, s, localCoordinator)) }
      val partialMap2 = ShardMapper.copy(emptyMaps(dataset2), dataset2)
      (4 to 7).foreach { s => partialMap2.updateFromEvent(IngestionStarted(dataset1, s, localCoordinator)) }
      val partialMaps = Map(dataset1 -> partialMap1, dataset2 -> partialMap2)
      val strategy = new DefaultShardAssignmentStrategy

      val added = strategy.nodeAdded(downingCoordinator, partialMaps, resourceMap)
      added.node shouldEqual downingCoordinator
      added.shards should have length 2
      val info = added.shards.head
      info.ref shouldEqual dataset1
      info.shards shouldEqual (4 to 7)

      added.shards.last.shards shouldEqual Seq(0, 1, 2, 3)

      val added2 = strategy.nodeAdded(downingCoordinator, partialMaps, resourceMap)
      added2 shouldEqual added
    }

    "not assign shards when adding coordinator that already has shards" in {
      // We need shardmappers that already have one coordinator assigned
      val partialMap1 = ShardMapper.copy(emptyMaps(dataset1), dataset1)
      (0 to 3).foreach { s => partialMap1.updateFromEvent(IngestionStarted(dataset1, s, localCoordinator)) }
      val partialMap2 = ShardMapper.copy(emptyMaps(dataset2), dataset2)
      (4 to 7).foreach { s => partialMap2.updateFromEvent(IngestionStarted(dataset1, s, localCoordinator)) }
      val partialMaps = Map(dataset1 -> partialMap1, dataset2 -> partialMap2)
      val strategy = new DefaultShardAssignmentStrategy

      val added = strategy.nodeAdded(localCoordinator, partialMaps, resourceMap)
      added.node shouldEqual localCoordinator
      added.shards should have length 0

      val added2 = strategy.nodeAdded(localCoordinator, partialMaps, resourceMap)
      added2 shouldEqual added
    }

    "not assign new shards when all shards already assigned" in {
      // We need shardmappers that already have one coordinator assigned
      val fullMap1 = ShardMapper.copy(emptyMaps(dataset1), dataset1)
      (0 to 3).foreach { s => fullMap1.updateFromEvent(IngestionStarted(dataset1, s, localCoordinator)) }
      (4 to 7).foreach { s => fullMap1.updateFromEvent(IngestionStarted(dataset1, s, downingCoordinator)) }
      val fullMap2 = ShardMapper.copy(emptyMaps(dataset2), dataset2)
      (0 to 3).foreach { s => fullMap2.updateFromEvent(IngestionStarted(dataset1, s, localCoordinator)) }
      (4 to 7).foreach { s => fullMap2.updateFromEvent(IngestionStarted(dataset1, s, downingCoordinator)) }
      val fullMaps = Map(dataset1 -> fullMap1, dataset2 -> fullMap2)
      val strategy = new DefaultShardAssignmentStrategy

      val added = strategy.nodeAdded(thirdCoordinator, fullMaps, resourceMap)
      added.node shouldEqual thirdCoordinator
      added.shards should have length 0

      val added2 = strategy.nodeAdded(thirdCoordinator, fullMaps, resourceMap)
      added2 shouldEqual added
    }

    import NodeClusterActor.DatasetResourceSpec

    "assign shards when new dataset added and still room" in {
      val strategy = new DefaultShardAssignmentStrategy
      // Since there are two datasets, shards for both datasets will get assigned.
      val coords = Set(localCoordinator, thirdCoordinator)
      val added = strategy.datasetAdded(dataset1, coords, DatasetResourceSpec(4, 1), emptyMaps)

      added.resources.shardToNodeRatio shouldEqual 4.0
      added.shards.keySet shouldEqual coords
      added.shards.values.reduce(_ ++ _) shouldEqual (0 to 7)
    }

    "not assign shards if calling datasetAdded but shards for dataset already assigned" in {
      val fullMap1 = ShardMapper.copy(emptyMaps(dataset1), dataset1)
      (0 to 3).foreach { s => fullMap1.updateFromEvent(IngestionStarted(dataset1, s, localCoordinator)) }
      (4 to 7).foreach { s => fullMap1.updateFromEvent(IngestionStarted(dataset1, s, downingCoordinator)) }
      val fullMaps = Map(dataset1 -> fullMap1)

      val strategy = new DefaultShardAssignmentStrategy
      val coords = Set(localCoordinator, downingCoordinator)
      val added = strategy.datasetAdded(dataset1, coords, DatasetResourceSpec(4, 1), fullMaps)

      added.resources.shardToNodeRatio shouldEqual 4.0
      added.shards.keySet shouldEqual coords
      added.shards.values.map(_.length).sum shouldEqual 0
    }

    // For upgrade scenario
    "assign new shards preferentially to spare nodes from failed shards" in (pending)
  }
}