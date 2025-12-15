package filodb.core.memstore.synchronization

import scala.concurrent.Future

import monix.reactive.Observable

import filodb.core.{DatasetRef, Response}
import filodb.core.memstore.FiloSchedulers.{assertThreadName, IOSchedName}
import filodb.core.store.{ColumnStore, PartKeyRecord}

/**
 * Cassandra based partkey updates publisher. In this all the partKey updates are stored in a cassandra table.
 * @param shard FiloDB shard
 * @param ref Dataset ref
 * @param colStore Cassandra Column Store where updates are published.
 */
class CassandraPartKeyUpdatesPublisher(override val shard: Int,
                                       ref: DatasetRef,
                                       colStore: ColumnStore,
                                       tags: Map[String, String]) extends PartKeyUpdatesPublisher {
  /**
   * NOTE: DO-NOT change the time bucket without considering the consuming pattern of the downstream applications.
   * */
  final val timeBucket5mInMillis = 300000L

  override def publish(offset: Long, partKeyRecords: Iterator[PartKeyRecord]): Future[Response] = {
    assertThreadName(IOSchedName)
    val currentTime = System.currentTimeMillis()
    val epoch5mBucket = currentTime / timeBucket5mInMillis
    colStore.writePartKeyUpdates(
      ref, epoch5mBucket, currentTime, offset, tags, Observable.fromIteratorUnsafe(partKeyRecords))
  }
}
