package filodb.cassandra.metastore

import java.util.UUID

import filodb.core.store.MetaStoreSpec
import filodb.cassandra.AllTablesTest
import filodb.core.Success

class CassandraMetaStoreSpec extends MetaStoreSpec with AllTablesTest {

  describe("checkpoint api") {
    it("should allow reading and writing checkpoints for shard") {
      val ds = dataset.copy(name = "gdelt-" + UUID.randomUUID()) // Add uuid so tests can be rerun

      // when there is no data for a shard, then return Long.MinValue as the checkpoint
      metaStore.readEarliestCheckpoint(ds.ref, 2).futureValue shouldEqual Long.MinValue
      metaStore.readCheckpoints(ds.ref, 2).futureValue.isEmpty shouldBe true

      // should be able to insert checkpoints for new groups
      metaStore.writeCheckpoint(ds.ref, 2, 1, 10).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ds.ref, 2, 2, 9).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ds.ref, 2, 3, 13).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ds.ref, 2, 4, 5).futureValue shouldEqual Success
      metaStore.readEarliestCheckpoint(ds.ref, 2).futureValue shouldEqual 5

      // should be able to update checkpoints for existing groups
      metaStore.writeCheckpoint(ds.ref, 2, 1, 12).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ds.ref, 2, 2, 15).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ds.ref, 2, 3, 17).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ds.ref, 2, 4, 7).futureValue shouldEqual Success
      metaStore.readEarliestCheckpoint(ds.ref, 2).futureValue shouldEqual 7

      // should be able to retrieve raw offsets as well
      val offsets = metaStore.readCheckpoints(ds.ref, 2).futureValue
      offsets.size shouldEqual 4
      offsets(1) shouldEqual 12
      offsets(2) shouldEqual 15
      offsets(3) shouldEqual 17
      offsets(4) shouldEqual 7

      // should be able to clear the table
      metaStore.clearAllData().futureValue
      metaStore.readCheckpoints(ds.ref, 2).futureValue.isEmpty shouldBe true
    }
  }
}