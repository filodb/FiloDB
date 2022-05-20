package filodb.core.memstore

import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.BeforeAndAfter

import filodb.core.GdeltTestData.datasetOptions
import filodb.core.GdeltTestData.schema
import filodb.core.metadata.Dataset

class EphemeralIndexMetadataStoreTest  extends AnyFunSpec with Matchers with BeforeAndAfter {

  val dataset = Dataset("gdelt", schema.slice(4, 6), schema.patch(4, Nil, 2), datasetOptions)

  it("should return default state for empty EphemeralIndexMetadataStore") {
    val store = new EphemeralIndexMetadataStore()
    store.currentState(dataset.ref, 0) shouldEqual ((IndexState.TriggerRebuild, None))
  }

  it("should return the latest  state set for a given dataset and shard but default values for another shard") {
    val store = new EphemeralIndexMetadataStore()
    store.updateState(dataset.ref, 0, IndexState.Building, 10)
    store.currentState(dataset.ref, 0) shouldEqual ((IndexState.Building, Some(10)))
    store.currentState(dataset.ref, 1) shouldEqual ((IndexState.TriggerRebuild, None))
  }
}
