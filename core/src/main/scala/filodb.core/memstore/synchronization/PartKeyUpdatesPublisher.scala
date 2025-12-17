package filodb.core.memstore.synchronization

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

import filodb.core.Response
import filodb.core.memstore.FiloSchedulers.{assertThreadName, IngestSchedName}
import filodb.core.store.PartKeyRecord

/**
 * Publishes *ALL* the partKey updates ( new, expired, re-ingest ) for a given *SHARD*.
 * Single-threaded use only ( guarantees sequencing of read/write/publish operations )
 * */
trait PartKeyUpdatesPublisher {

  val shard: Int

  var buffer: scala.collection.mutable.ArrayBuffer[Int] = scala.collection.mutable.ArrayBuffer.empty[Int]

  /**
   * Stores the updated or dirty part ids in a buffer.
   */
  def store(partId: Int): Unit = {
    buffer += partId
  }

  /**
   * Retrieves all the updated partIds since the last flush.
   * Reset's the buffer to store the newly updated partIds for the next flush.
   * Make sure that this is called only in IngestionThread
   * @return buffer[updated-partIds]
   */
  def fetchAll(): ArrayBuffer[Int] = {
    assertThreadName(IngestSchedName)
    val old = buffer
    buffer = scala.collection.mutable.ArrayBuffer.empty[Int]
    old
  }

  /**
   * We need to ensure that the partKey updates are published, before the changes are made in the long-durable store for
   * used for partKeyIndex backup/fault-tolerance. This is why we are leveraging the same flush-path to publish this
   * updates.
   *
   * Why are we not using a separate scheduler for publishing updates ?
   *  Consider the following scenario:
   *  1. lets say for num-group 1, the partKey updates are written to the long-term storage.
   *  2. the updates are waiting to be published because the publishing thread is not yet scheduled.
   *  3. At this time, the pod/node hosting the shard goes down (for deployment, crash etc.)
   *  4. On node startup, the shard will bootstrap from data-store and will skip the kafka offsets with the required
   *     updates.
   *  5. This can lead to missed updates and can result in out-of-sync issues for downstream applications which are
   *     consuming this updates.
   *
   * This is invoked during the flush task of any num-group of the shard.
   * @return Success if all the partKeys are written successfully.
   *         ErrorResponse, otherwise
   */
  def publish(offset: Long, partKeyRecords: Iterator[PartKeyRecord]): Future[Response]
}
