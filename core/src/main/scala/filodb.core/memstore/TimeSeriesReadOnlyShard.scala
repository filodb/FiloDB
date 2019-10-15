package filodb.core.memstore

import scala.concurrent.{ExecutionContext, Future}

import monix.reactive.Observable

import filodb.core.DatasetRef
import filodb.core.Types.ColumnId
import filodb.core.memstore.TimeSeriesShard.PartKey
import filodb.core.metadata.Schemas
import filodb.core.query.ColumnFilter
import filodb.core.store.{ChunkScanMethod, PartitionScanMethod, ReadablePartition}
import filodb.memory.format.ZeroCopyUTF8String

class TimeSeriesReadOnlyShard(ref: DatasetRef, schemas: Schemas, shardNum: Int)
                             (implicit val ioPool: ExecutionContext) {

  val shardStats = new TimeSeriesShardStats(ref, shardNum)

  def indexNames(limit: Int): Seq[String] = Seq.empty

  def labelValues(labelName: String, topK: Int): Seq[TermInfo] = Seq.empty

  def labelValuesWithFilters(filter: Seq[ColumnFilter],
                             labelNames: Seq[String],
                             endTime: Long,
                             startTime: Long,
                             limit: Int): Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] = Iterator.empty

  def partKeysWithFilters(filter: Seq[ColumnFilter],
                          endTime: Long,
                          startTime: Long,
                          limit: Int): Iterator[PartKey] = Iterator.empty

  def recoverIndex(): Future[Unit] = Future.successful(Unit)

  def refreshPartKeyIndexBlocking(): Unit = {}

  def scanPartitions(columnIDs: Seq[ColumnId],
                     partMethod: PartitionScanMethod,
                     chunkMethod: ChunkScanMethod): Observable[ReadablePartition] = {
    Observable.empty
  }

}
