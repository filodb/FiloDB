package filodb.core.memstore

import scala.collection.mutable.Map

import filodb.core.DatasetRef

/**                IndexState transition
 *
 *   Following are the states set in PartKeyLuceneIndex
 *   1. TriggerRebuild is set:
 *        Index Directory is recursively deleted
 *          a. Error deleting index directory -> TriggerRebuild and exception is propagated
 *          b. Successfully deleted index directory -> Empty
 *   2. Error instantiating IndexWriter
 *        a. TriggerRebuild is set and goto step 1
 *        b. If deletion is successful, IndexWriter is re-instantiated, any exception instantiating are not handled
 *   3. Process restarts with IndexState as Empty
 *        Possible when the process crashed right after cleaning the directory but before the Building state is set.
 *        In this case, the recursive directory deletion is done even if the directory might be still empty
 *   4. Process starts with Index state set to  "Building".
 *        The timestamp associated with Building state is the time used to read the data from datastore, whenever
 *        a process restarts with index state set to Building, no data is assumed to be present in index after the time
 *        set set with the state. Data starting from the provided tiemstamp is added to index again as index addition
 *        is idempotent
 *   5. Process starts with IndexState set to Synced
 *        The timestamp associated with Synced indicates all data till the time associated with the IndexState is
 *        guaranteed to be durable and persistent
 *
 *
 *                    TriggerRebuild (Intent to rebuild the index on next restart)
 *                         |
 *                         | (Recursively delete index directory)
 *                         v
 *                     Empty (Directory of the index is empty)
 *                         |
 *                         |
 *                         v
 *                    Building (Index rebuild started, time associated is the start time for pulling data)
 *                         |
 *                         |
 *                         v
 *                    Synced (Index rebuilt and synced to persistent store)
 */
object IndexState extends Enumeration {
  val Empty, Building, Synced, TriggerRebuild = Value
}


trait IndexMetadataStore {

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

/**
 * Non thread safe in memory index state metadata store
 */
class EphemeralIndexMetadataStore extends IndexMetadataStore {


  private val currentState = Map.empty[(DatasetRef, Int), (IndexState.Value, Option[Long])]

  override def currentState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long]) =
    currentState.get((datasetRef, shard)).getOrElse((IndexState.TriggerRebuild, None))

  override def updateState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long): Unit = {
    currentState((datasetRef, shard)) = (state, Some(time))
  }
}


