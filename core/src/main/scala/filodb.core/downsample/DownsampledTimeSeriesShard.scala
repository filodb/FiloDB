package filodb.core.downsample

import scala.concurrent.{ExecutionContext, Future}

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.reactive.Observable

import filodb.core.{DatasetRef, GlobalScheduler}
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore._
import filodb.core.metadata.Schemas
import filodb.core.query.ColumnFilter
import filodb.core.store._
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}

class DownsampledTimeSeriesShard(ref: DatasetRef,
                                 val storeConfig: StoreConfig,
                                 val schemas: Schemas,
                                 colStore: ColumnStore,
                                 shardNum: Int,
                                 filodbConfig: Config,
                                 downsampleConfig: DownsampleConfig)
                                (implicit val ioPool: ExecutionContext) extends StrictLogging {

  val shardStats = new TimeSeriesShardStats(ref, shardNum)

  val downsampleResolutions = downsampleConfig.resolutions
  val downsampleTtls = downsampleConfig.ttls
  val downsampledDatasetRefs = downsampleConfig.downsampleDatasetRefs(ref.dataset)

  val indexResolution = downsampleResolutions.last
  val indexDataset = downsampledDatasetRefs.last
  val indexTtl = downsampleTtls.last

  // since all partitions are paged from store, this would be much lower than what is configured for raw data
  val maxQueryMatches = storeConfig.maxQueryMatches * 0.5 // TODO configure if really necessary

  private var nextPartitionID = 0

  private final val partKeyIndex = new PartKeyLuceneIndex(indexDataset, schemas.part, shardNum, indexTtl)

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
    partKeyIndex.partIdsFromFilters(filter, startTime, endTime).iterator().take(limit).map { pId =>
      PartKey(partKeyFromPartId(pId), UnsafeUtils.arayOffset)
    }
  }

  def recoverIndex(): Future[Unit] = {
    val indexBootstrapper = new ColStoreIndexBootstrapper(colStore)
    indexBootstrapper.bootstrapIndex(partKeyIndex, shardNum, indexDataset,
                                     GlobalScheduler.globalImplicitScheduler){ _ => createPartitionID() }
      .map { count =>
        logger.info(s"Bootstrapped index for dataset=$indexDataset shard=$shardNum with $count records")
      }
  }

  /**
    * Returns a new non-negative partition ID which isn't used by any existing parition. A negative
    * partition ID wouldn't work with bitmaps.
    */
  private def createPartitionID(): Int = {
    this.synchronized {
      val id = nextPartitionID
      nextPartitionID += 1
      if (nextPartitionID < 0) {
        throw new IllegalStateException("Too many partitions. Reached int capacity")
      }
      id
    }
  }

  def refreshPartKeyIndexBlocking(): Unit = {}

  def lookupPartitions(partMethod: PartitionScanMethod,
                       chunkMethod: ChunkScanMethod): PartLookupResult = {
    partMethod match {
      case SinglePartitionScan(partition, _) => throw new UnsupportedOperationException
      case MultiPartitionScan(partKeys, _) => throw new UnsupportedOperationException
      case FilteredPartitionScan(split, filters) =>

        if (filters.nonEmpty) {
          val res = partKeyIndex.partIdsFromFilters(filters,
            chunkMethod.startTime,
            chunkMethod.endTime)
          val firstPartId = if (res.isEmpty) None else Some(res(0))

          val _schema = firstPartId.map(schemaIDFromPartID)
          // send index result in the partsInMemory field of lookup
          PartLookupResult(shardNum, chunkMethod, res,
            _schema, debox.Map.empty[Int, Long], debox.Buffer.empty)
        } else {
          throw new UnsupportedOperationException("Cannot have empty filters")
        }
    }
  }

  def scanPartitions(lookup: PartLookupResult): Observable[ReadablePartition] = {

    // Step 1: Choose the downsample level depending on the range requested
    val downsampledDataset = chooseDownsampleResolution(lookup.chunkMethod)
    logger.debug(s"Chose resolution $downsampledDataset for chunk method ${lookup.chunkMethod}")
    // Step 2: Query Cassandra table for that downsample level using colStore
    // Create a ReadablePartition objects that contain the time series data. This can be either a
    // PagedReadablePartitionOnHeap or PagedReadablePartitionOffHeap. This will be garbage collected/freed
    // when query is complete.

    if (lookup.partsInMemory.length > maxQueryMatches)
      throw new IllegalArgumentException(s"Seeing ${lookup.partsInMemory.length} matching time series per shard. Try " +
        s"to narrow your query by adding more filters so there is less than $maxQueryMatches matches " +
        s"or request for increasing number of shards this metric lives in")

    val partKeys = lookup.partsInMemory.iterator().map(partKeyFromPartId)
    Observable.fromIterator(partKeys)
      .mapAsync(10) { case partBytes =>
        colStore.readRawPartitions(downsampledDataset,
                                   storeConfig.maxChunkTime.toMillis,
                                   SinglePartitionScan(partBytes, shardNum),
                                   lookup.chunkMethod)
          .map(pd => makePagedPartition(pd, lookup.firstSchemaId.get))
          .toListL
          .map(Observable.fromIterable)
      }.flatten
  }

  protected def schemaIDFromPartID(partID: Int): Int = {
    partKeyIndex.partKeyFromPartId(partID).map { pkBytesRef =>
      val unsafeKeyOffset = PartKeyLuceneIndex.bytesRefToUnsafeOffset(pkBytesRef.offset)
      RecordSchema.schemaID(pkBytesRef.bytes, unsafeKeyOffset)
    }.getOrElse(throw new IllegalStateException("PartId returned by lucene, but partKey not found"))
  }

  private def chooseDownsampleResolution(chunkScanMethod: ChunkScanMethod): DatasetRef = {
    chunkScanMethod match {
      case AllChunkScan => downsampledDatasetRefs.last // since it is the highest resolution/ttl
      case TimeRangeChunkScan(startTime, _) =>
        val ttlIndex = downsampleTtls.indexWhere(t => startTime > System.currentTimeMillis() - t.toMillis)
        downsampledDatasetRefs(ttlIndex)
      case _ => ???
    }
  }

  private def makePagedPartition(part: RawPartData, firstSchemaId: Int): ReadablePartition = {
    val schemaId = RecordSchema.schemaID(part.partitionKey, UnsafeUtils.arayOffset)
    if (schemaId != firstSchemaId)
      throw new IllegalArgumentException("Query involves results with multiple schema. " +
        "Use type tag to provide narrower query")
    // FIXME It'd be nice to pass in the correct partId here instead of -1
    new PagedReadablePartition(schemas(schemaId), shardNum, -1, part)
  }

  /**
    * Iterator for lazy traversal of partIdIterator, value for the given label will be extracted from the ParitionKey.
    */
  case class LabelValueResultIterator(partIds: debox.Buffer[Int], labelNames: Seq[String], limit: Int)
    extends Iterator[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]] {
    var currVal: Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = _
    var numResultsReturned = 0
    var partIndex = 0

    override def hasNext: Boolean = {
      var foundValue = false
      while(partIndex < partIds.length && numResultsReturned < limit && !foundValue) {
        val partId = partIds(partIndex)

        import ZeroCopyUTF8String._
        //retrieve PartKey either from In-memory map or from PartKeyIndex
        val nextPart = partKeyFromPartId(partId)

        // FIXME This is non-performant and temporary fix for fetching label values based on filter criteria.
        // Other strategies needs to be evaluated for making this performant - create facets for predefined fields or
        // have a centralized service/store for serving metadata
        currVal = schemas.part.binSchema.toStringPairs(nextPart, UnsafeUtils.arayOffset)
          .filter(labelNames contains _._1).map(pair => {
          pair._1.utf8 -> pair._2.utf8
        }).toMap
        foundValue = currVal.nonEmpty
        partIndex += 1
      }
      foundValue
    }

    override def next(): Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = {
      numResultsReturned += 1
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
