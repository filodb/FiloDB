package filodb.core.memstore

import scala.collection.mutable.Map

import filodb.core.DatasetRef

/**                IndexState transition
 *
 *   Following are the states set in PartKeyLuceneIndex
 *   1. Process starts with TriggerRebuild/Rebuilding as init state:
 *        Index Directory is recursively deleted, following are the two possible outcomes
 *          a. Error deleting index directory, TriggerRebuild is set as the state and exception is propagated
 *          b. Successfully deleted index directory, index state becomes Empty
 *   2. Error instantiating IndexWriter
 *        a. TriggerRebuild is set and goto step 1
 *        b. If deletion is successful, IndexWriter is re-instantiated, any exception instantiating are not handled
 *   3. Process restarts with IndexState as Empty
 *        Index is already empty, no further action needed
 *   4. Process starts with Index state set to "Refreshing".
 *        The timestamp associated with Refreshing state is the time used to read the data from datastore, whenever
 *        a process restarts with index state set to Refreshing, no data is assumed to be present in index after the
 *        time set with the state. Data starting from the provided timestamp is added to index again as index addition
 *        is idempotent
 *   5. Process starts with IndexState set to Synced
 *        The timestamp associated with Synced indicates all data till the time associated with the IndexState is
 *        guaranteed to be durable and persistent
 *
 */
object IndexState extends Enumeration {
  val Empty, Rebuilding, Refreshing, Synced, TriggerRebuild = Value
}


trait IndexMetadataStore {

  /**
   * Init state is only queried when index is instantiated
   * @param datasetRef The dataset ref
   * @param shard the shard id of index
   * @return a tuple of state and the option of time at which the state was recorded
   */
  def initState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long])

  /**
   * Convenience method to test if the initState is trigger rebuild
   * @param datasetRef
   * @param shard
   * @return
   */
  def shouldTriggerRebuild(datasetRef: DatasetRef, shard: Int): Boolean =
    initState(datasetRef: DatasetRef, shard: Int) match {
      case (IndexState.TriggerRebuild, _) => true
      case _                              => false
    }

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


  /**
   *  Updates the state of the index
   * @param datasetRef The dataset ref
   * @param shard the shard id of index
   * @param state One of the IndexState.Values for the index
   * @param time a time in millis since epoch for when the state was updated
   */
  def updateInitState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long): Unit

}

/**
 * Non thread safe in memory index state metadata store
 */
class EphemeralIndexMetadataStore extends IndexMetadataStore {


  private val currentState = Map.empty[(DatasetRef, Int), (IndexState.Value, Option[Long])]
  private val initStateMap  = Map.empty[(DatasetRef, Int), (IndexState.Value, Option[Long])]

  override def currentState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long]) =
    currentState.get((datasetRef, shard)).getOrElse((IndexState.Empty, None))

  override def updateState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long): Unit = {
    currentState((datasetRef, shard)) = (state, Some(time))
    updateInitState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long)
  }

  override def initState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long]) =
    initStateMap.get((datasetRef, shard)).getOrElse((IndexState.TriggerRebuild, None))

  override def updateInitState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long): Unit =
    initStateMap((datasetRef, shard)) = (state, Some(time))
}


