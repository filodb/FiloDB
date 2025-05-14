package filodb.core.memstore.synchronization

import debox.Buffer
import scala.concurrent.Future

import filodb.core.Response
import filodb.core.store.PartKeyRecord

trait PartKeyUpdatesPublisher {

  val shard: Int

  var queue: debox.Buffer[Int] = debox.Buffer.empty[Int]

  /**
   * Stores the updated or dirty partKey in an in-memory concurrent queue.
   * Note: This can be called concurrently by ingestion-thread of different shards.
   * @param pk PartKeyRecord (shard, startTimeMs, endTimeMs, partKey)
   * @return true if adding to queue was successful
   *         false, otherwise. This is helpful for callee to track the skipped updates
   */
  def store(partId: Int): Unit = {
    queue += partId
  }

  def fetchAll(): Buffer[Int] = {
    val old = queue
    queue = debox.Buffer.empty[Int]
    old
  }

  /**
   * We need to ensure that the partKey updates are published, before the changes are made in the long-durable store for
   * used for partKeyIndex backup/fault-tolerance.
   *
   * WHY? This is because, we want to avoid any miss in partkey updates in case the updates is written to the long-term
   *      partkey table and the pod/node is down immediately before the partkey updates are published.
   *
   * This is invoked during the flush task of any num-group of the shard.
   * @return Number of partKeys flushed.
   *         None, if there was any exception or if there is nothing to flush
   */
  def publish(offset: Long, partKeyRecords: Iterator[PartKeyRecord]): Future[Response]
}
