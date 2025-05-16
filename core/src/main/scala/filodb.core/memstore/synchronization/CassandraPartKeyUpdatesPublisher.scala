package filodb.core.memstore.synchronization

import scala.concurrent.Future

import kamon.tag.TagSet
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
                                       tagSet: TagSet) extends PartKeyUpdatesPublisher {
  final val hourInMillis = 3600000L

  override def publish(offset: Long, partKeyRecords: Iterator[PartKeyRecord]): Future[Response] = {
    assertThreadName(IOSchedName)
    val currentTime = System.currentTimeMillis()
    val epochHour = currentTime / hourInMillis
    colStore.writePartKeyUpdates(
      ref, epochHour, currentTime, offset, tagSet, Observable.fromIteratorUnsafe(partKeyRecords))
  }
}
