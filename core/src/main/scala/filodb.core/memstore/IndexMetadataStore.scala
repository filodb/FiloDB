package filodb.core.memstore

import com.typesafe.scalalogging.StrictLogging
import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.ByteBuffer
import scala.collection.mutable.Map
import scala.util.{Failure, Success, Try, Using}

import filodb.core.DatasetRef
import filodb.core.memstore.FileSystemBasedIndexMetadataStore.{genFileV1Magic, snapFileV1Magic}





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
  val Empty, Refreshing, Synced, TriggerRebuild = Value
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

object FileSystemBasedIndexMetadataStore {
  val genFileV1Magic = 0x5b
  val snapFileV1Magic = 0x5c
  val expectedGenerationEnv = "INDEX_GENERATION"
}
/**
 * File system based metadata store that uses filesystem for storing the metadata. For rebuilds, current implementation
 * supports a rebuild of entire DS cluster and not just targeted shards for simplicity.
 *
 * @param rootDirectory
 * @param expectedGeneration: The expected generation of the metadata. If None is found, no rebuild will be triggered
 *                            The checks for generation will run only when a valid integer generation is passed
 * @param maxRefreshHours:    Refreshing for a large duration is not efficient and it will be faster to rebuild the
 *                            index from scratch, if a snapFile and the elapsed time between now and and lastSync
 *                            time is greater than maxRefreshHours, initState will respond with TriggerRebuild
 */
class FileSystemBasedIndexMetadataStore(rootDirectory: String, expectedGeneration: Option[Int],
                                        maxRefreshHours: Long = Long.MaxValue)
  extends IndexMetadataStore with StrictLogging {

  val genFilePathFormat = rootDirectory + File.separator + "_%s_%d_rebuild.gen"
  val snapFilePathFormat = rootDirectory + File.separator + "_%s_%d_snap"

  private def hour(millis: Long = System.currentTimeMillis()): Long = millis / 1000 / 60 / 60

  /**
   * Init state is only queried when index is instantiated, the filesystem based implementation uses the file
   * /<rootDirectory>/_<dataset>_<shard>_rebuild.gen file for triggering the rebuild, following cases will decide
   * if a rebuild is triggered
   *       a. If no expectedVersion is provided, then simply call currentState
   *       b. If the file is absent, the state is assumed to be rebuild and the index will be rebuilt.
   *       c. If the file is present and the contents are garbled, its assumed to be a rebuild of index.
   *       d. If the file is present and the content returns the current generation (a numeric version),
   *          the value is compared to an expected environment value and if the environment variable has a generation,
   *          greater than the one in the file, a rebuild is triggered.
   *       e. If sync file is present and the time in sync file is is more than maxRefreshHours in the past than now,
   *          a refresh will be triggered
   *       f. If none of the above three cases trigger a rebuild, the currentState method is used to determine the state
   * @param datasetRef The dataset ref
   * @param shard the shard id of index
   * @return a tuple of state and the option of time at which the state was recorded
   */
  override def initState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long]) = {
    val snapFile = new File(snapFilePathFormat.format(datasetRef.dataset, shard))
    expectedGeneration.map(expectedGen => {
      val genFile = new File(genFilePathFormat.format(datasetRef.dataset, shard))
      if (!genFile.exists()) {
        logger.info("Gen file {} does not exist and expectedGen is {}, triggering rebuild",
          genFile.toString, expectedGen)
        (IndexState.TriggerRebuild, None)
      } else {
        Using(new FileInputStream(genFile)) { fin =>
          val magicByte = fin.read()
          if (magicByte != FileSystemBasedIndexMetadataStore.genFileV1Magic) {
            // if first byte is not the expected magicByte then the file is not as expected and possibly garbled
            logger.warn("Gen file {} exist and possibly corrupt, triggering rebuild", genFile.toString)
            (IndexState.TriggerRebuild, None)
          } else {
            // If the first byte is magicByte read just the next 4 bytes. This ensures we just read what we need
            val bytes = Array[Byte](0, 0, 0, 0)
            if (fin.read(bytes) != 4) {
              // Not able to read 4 bytes, the file is truncated and we will trigger a rebuild
              logger.warn("Gen file {} with right magic bytes but less than 4 bytes for generation," +
                " triggering rebuild", genFile.toString)
              (IndexState.TriggerRebuild, None)
            } else {
              val generation = ByteBuffer.wrap(bytes).getInt()
              if (expectedGen > generation) {
                logger.info("Gen file {} has generation {} and expected generation is {}, triggering rebuild",
                  genFile.toString, generation, expectedGen)
                (IndexState.TriggerRebuild, None)
              } else if (shouldRebuildIndexForOldSnap(datasetRef, shard, snapFile)) {
                logger.info("lastSnapTime is more than {} hours from now, " +
                  "for dataset={} and shard={}. Triggering index rebuild instead of syncing the delta",
                  maxRefreshHours, datasetRef.toString, shard)
                (IndexState.TriggerRebuild, None)
              } else
                currentState(datasetRef, shard)
            }
          }
        }.get
      }
    }).getOrElse(
      if (shouldRebuildIndexForOldSnap(datasetRef, shard, snapFile))
        (IndexState.TriggerRebuild, None)
      else
        currentState(datasetRef, shard)
    )
  }

  /**
   * Determines if the rebuild of index should be triggered based on how old the snap is in snap file
   * @param datasetRef
   * @param shard
   * @param snapFile
   * @return
   */
  private def shouldRebuildIndexForOldSnap(datasetRef: DatasetRef, shard: Int, snapFile: File): Boolean=
    if (snapFile.exists()) {
      Using(new FileInputStream(snapFile)) { fin =>
        getSnapTime(snapFile.toString, fin) match {
          case Success(snapTime)    =>  hour() - hour(snapTime) > maxRefreshHours
          case Failure(_)           =>  true
        }
      }.get
    } else
      false

/**
   * The /<rootDirectory>/_<dataset>_<shard>_snap file contains the last time the index for the given dataset and shard
   * was synched. Current implementation only supports storing Synced state and no state related information is stored
   * in the snap file. The file contains the magic byte followed by 8 bytes for the long value for the timestamp
   * starting with most to least significant bytes
   *
   * In case the snap file does not exists, magic is not as expected or the file is corrupt, RebuildIndex will be
   * triggered
   *
   * @param datasetRef The dataset ref
   * @param shard the shard id of index
   * @return a tuple of state and the option of time at which the state was recorded
   */

  override def currentState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long]) = {
    val snapFile = new File(snapFilePathFormat.format(datasetRef.dataset, shard))
    if (!snapFile.exists()) {
      logger.info("Snap file {} does not exist , triggering rebuild", snapFile.toString)
      (IndexState.Empty, None)
    } else {
      Using(new FileInputStream(snapFile)) { fin =>
        getSnapTime(snapFile.toString, fin) match {
          case Success(snapTime)    =>
                                        logger.info("Found lastSyncTime={} for dataset={} and shard={}",
                                          snapTime, datasetRef.toString, shard)
                                        (IndexState.Synced, Some(snapTime))
          case Failure(e)           =>  throw e           // Failure to read the snap file in currentState is fatal
        }
      }.get
    }
  }

  /**
   *  Gets the snapTime from the snap file
   *
   * @param snapFile   the snapFile
   * @param fin        The input stream object for the file
   * @return           Long snap time if file is valid
   */
  private def getSnapTime(snapFile: String, fin: FileInputStream): Try[Long] = {
        val magicByte = fin.read()
        if (magicByte != FileSystemBasedIndexMetadataStore.snapFileV1Magic) {
          // if first byte is not the expected magicByte then the file is not as expected and possibly garbled
          logger.warn(" Snap {} exist and possibly corrupt, this will trigger a rebuild", snapFile.toString)
          Failure(new IllegalStateException("Invalid magic bytes in snap file"))
        } else {
          // If the first byte is magicByte read just the next 8 bytes
          val bytes = Array[Byte](0, 0, 0, 0, 0, 0, 0, 0)
          if (fin.read(bytes) != 8) {
            // Not able to read 8 bytes, the file is truncated and we will trigger a rebuild
            logger.warn("Snap file {} with right magic bytes but less than 8 for time," +
              " this will trigger a rebuild of index", snapFile.toString)
            Failure(new IllegalStateException("Snap file truncated"))
          } else {
            val lastSyncTime = ByteBuffer.wrap(bytes).getLong()
            Success(lastSyncTime)
          }
        }
   }
  /**
   *  Updates the state of the index, the snap file will be updated with the correct timestamp in case the state
   *  is Synced, current implementation of updateState ignores other index states. Also when the index is updated
   *  and synced, it is important to ensure the gen file is updated with the current generation. This guarantees that
   *  next time the on bootstrap, the initState should return Synced and not TriggerRebuild
   *
   * @param datasetRef The dataset ref
   * @param shard the shard id of index
   * @param state One of the IndexState.Values for the index
   * @param time a time in millis since epoch for when the state was updated
   */
  // scalastyle:off method.length
  override def updateState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long): Unit = {
    state match {
      case IndexState.Synced           =>  // Only handle cases where state is Synced
                                    val snapFile = new File(snapFilePathFormat.format(datasetRef.dataset, shard))
                                    val genFile = new File(genFilePathFormat.format(datasetRef.dataset, shard))
                                    Using(new FileOutputStream(snapFile)){
                                      // First write the last synced time to the snap file
                                      snapFos =>
                                        logger.info("Updating state of dataset={}, shard={} with syncTime={}",
                                          datasetRef.toString, shard, time)
                                        snapFos.write(snapFileV1Magic)
                                        val buff = ByteBuffer.allocate(8)
                                        buff.putLong(time)
                                        snapFos.write(buff.array())
                                    }.flatMap { _ =>
                                      // If writing the snap file is successful, update the generation file so
                                      // next call to initState does not trigger the rebuild. Note that these
                                      // two operations are not atomic, we may still have the snap file updated but
                                      // generation file not, in which case next call to initState will trigger
                                      // the rebuild.
                                      expectedGeneration.map{
                                        generation =>
                                          Using(new FileOutputStream(genFile)) { genFos =>
                                            logger.info("Updating genFile " +
                                              "of dataset={}, shard={} with generation={}",
                                              datasetRef.toString, shard, generation)
                                            genFos.write(genFileV1Magic)
                                            val buff = ByteBuffer.allocate(4)
                                            buff.putInt(generation)
                                            genFos.write(buff.array())
                                          }
                                      }.getOrElse(Success(()))
                                    }.get
      case IndexState.TriggerRebuild   => // Delete gen file so next restart triggers the rebuild
                                    val genFile = new File(genFilePathFormat.format(datasetRef.toString, shard))
                                    val deleted = genFile.delete()
                                    logger.info("genFile={} delete returned {} on TriggerRebuild, " +
                                      "index will be rebuild on next restart of dataset={} and shard={}",
                                      genFile, deleted, datasetRef.toString, shard)
      case IndexState.Empty            => // Empty is invoked denoting the index directory is empty. This also signals
                                          // metastore to delete the corresponding gen and snap file
                                          val genFile = new File(genFilePathFormat.format(datasetRef.toString, shard))
                                          val snapFile = new File(snapFilePathFormat.format(datasetRef.toString, shard))
                                          val genDeleted = genFile.delete()
                                          val snapDeleted = snapFile.delete()
                                          logger.info("genFile={} delete returned {} and " +
                                            "snapFile={} delete returned {} when empty state was triggered " +
                                            "for dataset={} and shard={}",
                                            genFile, genDeleted, snapFile, snapDeleted, datasetRef.toString, shard)
      case _                           =>  //NOP
    }
  }
  // scalastyle:on method.length

  /**
   * Updates the state of the index
   * @param datasetRef The dataset ref
   * @param shard the shard id of index
   * @param state One of the IndexState.Values for the index
   * @param time a time in millis since epoch for when the state was updated
   */
  override def updateInitState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long): Unit = ???
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

/**
 * Non thread safe in memory index state metadata store
 */
class FileCheckpointedIndexMetadataStore(val indexLocation: String) extends IndexMetadataStore {

  private val currentState = Map.empty[(DatasetRef, Int), (IndexState.Value, Option[Long])]

  override def currentState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long]) = {
    currentState.get((datasetRef, shard)).getOrElse((IndexState.Empty, None))
  }

  override def updateState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long): Unit = {
    val propertyFile = new File(indexLocation, datasetRef.dataset + "-" + shard + ".properties")
    DownsampleIndexCheckpointer.writeCheckpoint(propertyFile, time)
    currentState((datasetRef, shard)) = (state, Some(time))
  }

  override def initState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long]) = {
    val propertyFile = new File(indexLocation, datasetRef.dataset + "-" + shard + ".properties")
    val checkpointTime = DownsampleIndexCheckpointer.getDownsampleLastCheckpointTime(propertyFile)
    val state = if (checkpointTime == 0) {
      (IndexState.TriggerRebuild, None)
    } else {
      (IndexState.Synced, Some(checkpointTime))
    }
    currentState((datasetRef, shard)) = state
    state
  }

  // NOT sure I understand why we need this call?
  override def updateInitState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long): Unit = {
    ???
  }
}

