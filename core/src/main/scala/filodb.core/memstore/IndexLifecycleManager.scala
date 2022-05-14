package filodb.core.memstore

import filodb.core.DatasetRef


object IndexState extends Enumeration {
  val Empty, Rebuilding, Built, Synced, Unknown = Value
}


trait IndexLifecycleManager {

  /**
   *
   * @param datasetRef The dataset ref
   * @param shard the shard id of index
   * @return a tuple of state and the option of time at which the state was recorded
   */
  def currentState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long])

  /**
   *  Updates the state of the index
   * @param datasetRef The dataset ref
   * @param shard the shard id of index
   * @param state One of the IndexState.Values for the index
   * @param time a time in millis since epoch for when the state was updated
   */
  def updateState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long): Unit
}
