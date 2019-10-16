package filodb.core.downsample

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.Config
import monix.reactive.Observable
import net.ceedubs.ficus.Ficus._

import filodb.core.DatasetRef
import filodb.core.Types.ColumnId
import filodb.core.memstore.{PartKey, TermInfo, TimeSeriesShardStats}
import filodb.core.metadata.Schemas
import filodb.core.query.ColumnFilter
import filodb.core.store.{ChunkScanMethod, PartitionScanMethod, ReadablePartition}
import filodb.memory.format.ZeroCopyUTF8String

class DownsampledTimeSeriesShard(ref: DatasetRef, schemas: Schemas,
                                 shardNum: Int,
                                 filodbConfig: Config)
                                (implicit val ioPool: ExecutionContext) {

  val shardStats = new TimeSeriesShardStats(ref, shardNum)

  val downsamplerConfig = filodbConfig.getConfig("downsampler")
  val downsampleResolutions = downsamplerConfig.as[Seq[FiniteDuration]]("resolutions")
  val downsampleTtls = downsamplerConfig.as[Seq[FiniteDuration]]("ttls").map(_.toSeconds.toInt)
  require(downsampleResolutions.length == downsampleTtls.length,
    "Invalid configuration. Downsample resolutions and ttl have different length")

  val downsampledDatasetRefs = DownsampledTimeSeriesStore.downsampleDatasetRefs(ref, downsampleResolutions)

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
    // Step 1: Choose the downsample level depending on the range requested

    // Step 2: Query Cassandra table for that downsample level

    // Step 3: Create a ReadablePartition objects that contain the time series data. This can be either a
    // PagedReadablePartitionOnHeap or PagedReadablePartitionOffHeap. This will be garbage collected/freed
    // when query is complete.

    Observable.empty
  }

}
