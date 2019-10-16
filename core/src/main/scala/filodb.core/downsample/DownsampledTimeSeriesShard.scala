package filodb.core.downsample

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

import com.googlecode.javaewah.IntIterator
import com.typesafe.config.Config
import monix.reactive.Observable
import net.ceedubs.ficus.Ficus._

import filodb.core.DatasetRef
import filodb.core.Types.ColumnId
import filodb.core.memstore.{PartKey, PartKeyLuceneIndex, TermInfo, TimeSeriesShardStats}
import filodb.core.metadata.Schemas
import filodb.core.query.ColumnFilter
import filodb.core.store._
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}

class DownsampledTimeSeriesShard(ref: DatasetRef,
                                 schemas: Schemas,
                                 colStore: ColumnStore,
                                 shardNum: Int,
                                 filodbConfig: Config)
                                (implicit val ioPool: ExecutionContext) {

  val shardStats = new TimeSeriesShardStats(ref, shardNum)

  val downsamplerConfig = filodbConfig.getConfig("downsampler")
  val downsampleResolutions = downsamplerConfig.as[Seq[FiniteDuration]]("resolutions")
  val downsampleTtls = downsamplerConfig.as[Seq[FiniteDuration]]("ttls")
  require(downsampleResolutions.sorted == downsampleResolutions, "Resolutions not sorted")
  require(downsampleResolutions.length == downsampleTtls.length,
    "Invalid configuration. Downsample resolutions and ttl have different length")
  val indexResolution = downsampleResolutions(downsampleTtls.indexOf(downsampleTtls.max))
  val downsampledDatasetRefs = DownsampledTimeSeriesStore.downsampleDatasetRefs(ref, downsampleResolutions)
  val indexDataset = downsampledDatasetRefs(indexResolution)


  private final val partKeyIndex = new PartKeyLuceneIndex(ref, schemas.part, shardNum,
    downsampleTtls.max)

  def indexNames(limit: Int): Seq[String] = Seq.empty

  def labelValues(labelName: String, topK: Int): Seq[TermInfo] = partKeyIndex.indexValues(labelName, topK)

  def labelValuesWithFilters(filter: Seq[ColumnFilter],
                             labelNames: Seq[String],
                             endTime: Long,
                             startTime: Long,
                             limit: Int): Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] = {
    LabelValueResultIterator(partKeyIndex.partIdsFromFilters(filter, startTime, endTime), labelNames, limit)
  }

  def partKeysWithFilters(filter: Seq[ColumnFilter],
                          endTime: Long,
                          startTime: Long,
                          limit: Int): Iterator[PartKey] = {
    import filodb.core.Iterators._
    partKeyIndex.partIdsFromFilters(filter, startTime, endTime)
      .map( pId => PartKey(partKeyFromPartId(pId), UnsafeUtils.arayOffset), limit)
  }

  def recoverIndex(): Future[Unit] = {
    // TODO:
    // Recover lucene index by loading data from the cass table representing
    // the datasetref with highest downsample retention

    // Recover index for the dataset `indexDataset` member of this class
    Future.successful(Unit)
  }

  def refreshPartKeyIndexBlocking(): Unit = {}


  private def chooseDownsampleResolution(chunkScanMethod: ChunkScanMethod): DatasetRef = ???

  def scanPartitions(columnIDs: Seq[ColumnId],
                     partMethod: PartitionScanMethod,
                     chunkMethod: ChunkScanMethod): Observable[ReadablePartition] = {

    // Step 1: Look up index and find matching part keys
    import filodb.core.Iterators._
    val partKeys = partMethod match {
      case SinglePartitionScan(partition, _) => ???
      case MultiPartitionScan(partKeys, _) => ???
      case FilteredPartitionScan(split, filters) =>
        // TODO: There are other filters that need to be added and translated to Lucene queries
        if (filters.nonEmpty) {
          partKeyIndex.partIdsFromFilters(filters,
                                                                          chunkMethod.startTime,
                                                                          chunkMethod.endTime)
            .map(p => partKeyFromPartId(p), 10000)
        } else {
          ???
        }
    }

    // Step 2: Choose the downsample level depending on the range requested
    val downsampledDataset = chooseDownsampleResolution(chunkMethod)

    // Step 3: Query Cassandra table for that downsample level using colStore
    // Create a ReadablePartition objects that contain the time series data. This can be either a
    // PagedReadablePartitionOnHeap or PagedReadablePartitionOffHeap. This will be garbage collected/freed
    // when query is complete.
    Observable.fromIterator(partKeys)
      .mapAsync(10) { case partBytes =>
        colStore.readRawPartitions(downsampledDataset, SinglePartitionScan(partBytes, shardNum), chunkMethod)
          .map(makePagedPartition)
          .toListL
          .map(Observable.fromIterable)
      }.flatten
  }

  private def makePagedPartition(part: RawPartData): ReadablePartition = ???

  /**
    * Iterator for lazy traversal of partIdIterator, value for the given label will be extracted from the ParitionKey.
    */
  case class LabelValueResultIterator(partIterator: IntIterator, labelNames: Seq[String], limit: Int)
    extends Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] {
    var currVal: Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = _
    var index = 0

    override def hasNext: Boolean = {
      var foundValue = false
      while(partIterator.hasNext && index < limit && !foundValue) {
        val partId = partIterator.next()

        import ZeroCopyUTF8String._
        //retrieve PartKey either from In-memory map or from PartKeyIndex
        val nextPart = partKeyFromPartId(partId)

        // FIXME This is non-performant and temporary fix for fetching label values based on filter criteria.
        // Other strategies needs to be evaluated for making this performant - create facets for predefined fields or
        // have a centralized service/store for serving metadata
        currVal = schemas.part.binSchema.toStringPairs(nextPart, UnsafeUtils.arayOffset)
          .filter(labelNames contains _._1).map(pair => {
          (pair._1.utf8 -> pair._2.utf8)
        }).toMap
        foundValue = currVal.size > 0
      }
      foundValue
    }

    override def next(): Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = {
      index += 1
      currVal
    }
  }

  /**
    * retrieve partKey for a given PartId
    */
  private def partKeyFromPartId(partId: Int): Array[Byte] = {
    val partKeyByteBuf = partKeyIndex.partKeyFromPartId(partId)
    if (partKeyByteBuf.isDefined) partKeyByteBuf.get.bytes
    else throw new IllegalStateException("This is not an expected behavior." +
      " PartId should always have a corresponding PartKey!")
  }

}
