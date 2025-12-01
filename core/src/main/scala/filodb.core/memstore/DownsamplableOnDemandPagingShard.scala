package filodb.core.memstore

import scala.concurrent.{Await, ExecutionContext, Future}

import com.typesafe.config.Config
import monix.reactive.Observable

import filodb.core.{DataDropped, DatasetRef, Response, Success}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.downsample.DownsampleConfig
import filodb.core.memstore.ratelimit.QuotaSource
import filodb.core.metadata.Schemas
import filodb.core.store.{ColumnStore, MetaStore, PartKeyRecord, StoreConfig}
import filodb.memory.NativeMemoryManager

class DownsamplableOnDemandPagingShard (
  ref: DatasetRef,
  schemas: Schemas,
  storeConfig: StoreConfig,
  numShards: Int,
  quotaSource: QuotaSource,
  shardNum: Int,
  bufferMemoryManager: NativeMemoryManager,
  rawStore: ColumnStore,
  downsampleStore: ColumnStore,
  metastore: MetaStore,
  evictionPolicy: PartitionEvictionPolicy,
  downsampleConfig : DownsampleConfig,
  filodbConfig: Config
) (implicit ec: ExecutionContext) extends OnDemandPagingShard(
    ref: DatasetRef,
    schemas: Schemas,
    storeConfig: StoreConfig,
    numShards: Int,
    quotaSource: QuotaSource,
    shardNum: Int,
    bufferMemoryManager: NativeMemoryManager,
    rawStore: ColumnStore,
    metastore: MetaStore,
    evictionPolicy: PartitionEvictionPolicy,
    filodbConfig: Config
) {

  val downsampleTTLSeconds = downsampleConfig.ttls.last.toSeconds //ttlByResolution(highestDSResolution),
  val dsDatasetRef = downsampleConfig.downsampleDatasetRefs(ref.dataset).last

  import FiloSchedulers._

  override def writeDirtyPartKeys(flushGroup: FlushGroup): Future[Response] = {
    val rawResponse : Future[Response] = super.writeDirtyPartKeys(flushGroup)
    val partKeyRecords: Iterator[filodb.core.store.PartKeyRecord] =
      InMemPartitionIterator2(flushGroup.dirtyPartsToFlush)
        .map(toPartKeyRecord)
        .flatMap(
          rawPartKeyRecord => {
            val dsPartKey : Option[Array[Byte]] = RecordBuilder.buildDownsamplePartKey(
              rawPartKeyRecord.partKey, schemas
            )
            val partKeyRecord = dsPartKey.map(k =>
              PartKeyRecord(k, rawPartKeyRecord.startTime, rawPartKeyRecord.endTime, rawPartKeyRecord.shard)
            )
            partKeyRecord
          }
        )
    assertThreadName(IOSchedName)
    val updateHour = System.currentTimeMillis() / 1000 / 60 / 60
    val downsampleResponse : Future[Response] = downsampleStore.writePartKeys(
      dsDatasetRef, shardNum, Observable.fromIteratorUnsafe(partKeyRecords), downsampleTTLSeconds, updateHour, false
    ).map { resp =>
      if (flushGroup.dirtyPartsToFlush.length > 0) {
        logger.info(s"Finished flush of partKeys to downsample numPartKeys=${flushGroup.dirtyPartsToFlush.length}" +
          s" resp=$resp for dataset=$ref shard=$shardNum")
        shardStats.numDirtyDownsamplePartKeysFlushed.increment(flushGroup.dirtyPartsToFlush.length)
      }
      resp
    }.recover { case e =>
      logger.error(s"Internal Error when persisting part keys in dataset=$ref shard=$shardNum - " +
        "should have not reached this state", e)
      DataDropped
    }
    val both = Seq(rawResponse, downsampleResponse) //    val both = Seq(downsampleResponse)
    val f : Future[Seq[Response]] = Future.sequence(both)
    val resultingFuture : Future[Response] = f.map{
      results : Seq[Response] => results.foldLeft[Response](Success) { (acc, next) => Success
        if (acc != Success) acc
        else if (next != Success) next
        else Success
      }
    }
    import scala.concurrent.duration._
    val result = Await.result(resultingFuture, 60.seconds)
    resultingFuture
  }
}
