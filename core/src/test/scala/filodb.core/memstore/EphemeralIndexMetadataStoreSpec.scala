package filodb.core.memstore

import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.BeforeAndAfter

import filodb.core.GdeltTestData.datasetOptions
import filodb.core.GdeltTestData.schema
import filodb.core.metadata.Dataset

class EphemeralIndexMetadataStoreSpec  extends AnyFunSpec with Matchers with BeforeAndAfter {

  val dataset = Dataset("gdelt", schema.slice(4, 6), schema.patch(4, Nil, 2), datasetOptions)

  it("must return TriggerRebuild for EphemeralIndexMetadataStore by default") {
    val store = new EphemeralIndexMetadataStore()
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.TriggerRebuild, None))
  }

  it("must return Empty as current statue for EphemeralIndexMetadataStore by default") {
    val store = new EphemeralIndexMetadataStore()
    store.currentState(dataset.ref, 0) shouldEqual ((IndexState.Empty, None))
  }

  it("must return the latest  state set for a given dataset and shard but default values for another shard") {
    val store = new EphemeralIndexMetadataStore()
    // Updating the current state also will change the Initial state
    store.updateState(dataset.ref, 0, IndexState.Refreshing, 10)
    store.currentState(dataset.ref, 0) shouldEqual ((IndexState.Refreshing, Some(10)))
    store.initState(dataset.ref, 0) shouldEqual ((IndexState.Refreshing, Some(10)))

    // Default values for other dataset/shard combination
    store.initState(dataset.ref, 1) shouldEqual ((IndexState.TriggerRebuild, None))
    store.currentState(dataset.ref, 1) shouldEqual ((IndexState.Empty, None))
  }
}
