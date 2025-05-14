package filodb.core.memstore.synchronization

import scala.concurrent.Future

import monix.reactive.Observable

import filodb.core.{DatasetRef, Response}
import filodb.core.memstore.FiloSchedulers.{assertThreadName, IOSchedName}
import filodb.core.store.{ColumnStore, PartKeyRecord}

class CassandraPartKeyUpdatesPublisher(override val shard: Int,
                                       ref: DatasetRef,
                                       colStore: ColumnStore) extends PartKeyUpdatesPublisher {
  final val hourInMillis = 3600000L

  override def publish(offset: Long, partKeyRecords: Iterator[PartKeyRecord]): Future[Response] = {
    assertThreadName(IOSchedName)
    val currentTime = System.currentTimeMillis()
    val epochHour = currentTime / hourInMillis
    colStore.writePartKeyUpdates(ref, epochHour, currentTime, offset, Observable.fromIteratorUnsafe(partKeyRecords))
  }
}
