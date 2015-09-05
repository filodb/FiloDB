package filodb.core.ingest

import akka.actor.{Actor, ActorRef, Props}

import filodb.core.BaseActor
import filodb.core.metadata.{Dataset, MetaStore}
import filodb.core.columnstore.ColumnStore

/**
 * The Reprojector flushes rows out of the MemTable and writes out Segments to the ColumnStore.
 *
 * It can be triggered by a timer to regularly flush or by events (such as MemTable getting full).
 * It works on one partition at a time, going through rows, creating segments, flushing them to
 * the ColumnStore, then deleting the rows from the memtable.
 *
 * It is an actor, working asynchronously, and multiple reprojectors can be in action at the same time.
 */
object ReprojectorActor {
  /**
   * Sent to periodically check if flushing is required.
   */
  case object Check

  /**
   * Flushes data for a particular dataset.  Usually called explicitly when ingestion has finished.
   * Usually starts with the most stale partitions.
   * NOTE: This may take a long time!  After a segment has been flushed this actor may call flush again.
   */
  case class Flush(dataset: Dataset)

  /**
   * Creates a new ReprojectorActor (to be used in system.actorOf(....))
   * Note: partition does not need to include shard info, just chunkSize, dataset and partition name.
   */
  def props[R](metaStore: MetaStore,
               columnStore: ColumnStore): Props =
    Props(classOf[ReprojectorActor[R]], metaStore)
}

class ReprojectorActor[R](metaStore: MetaStore,
                          columnStore: ColumnStore) extends BaseActor {
  import ReprojectorActor._

  def receive: Receive = {
    case Check =>
      logger.debug("Checking for anything that needs to be flushed...")
  }
}