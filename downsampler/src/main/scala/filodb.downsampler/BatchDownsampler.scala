package filodb.downsampler

import scala.collection.mutable.{ArrayBuffer, Map => MMap}
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import monix.reactive.Observable
import scalaxy.loops._

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.{DatasetRef, ErrorResponse}
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.downsample.{ChunkDownsampler, DoubleChunkDownsampler, HistChunkDownsampler, TimeChunkDownsampler}
import filodb.core.memstore.{PagedReadablePartition, TimeSeriesPartition, TimeSeriesShardStats, WriteBufferPool}
import filodb.core.metadata.Schemas
import filodb.core.store.{AllChunkScan, ChunkSet, RawPartData, ReadablePartition}
import filodb.memory._
import filodb.memory.format.{SeqRowReader, UnsafeUtils}

/**
  * This class maintains state during the processing of a batch of TSPartitions to downsample. Namely
  * a. The memory manager used for the paged partitions
  * b. The buffer pool used to ingest and chunk the downsampled data
  * c. Block store for overflow chunks that go beyond write buffers
  * d. Statistics
  * e. The Cassandra Store API from which to read raw data as well as write downsampled data
  *
  * It performs the operation of downsampling all partitions in the batch and writes downsampled data
  * into cassandra.
  *
  * This is an object instead of class so that it need not be serialized to a spark executor
  * from the driver. All of the necessary params for the behavior are loaded from DownsampleSettings.
  */
object BatchDownsampler extends StrictLogging {

  import DownsamplerSettings._

  val readSched = Scheduler.io("cass-read-sched")
  val writeSched = Scheduler.io("cass-write-sched")
  val cassandraColStore = new CassandraColumnStore(filodbConfig, readSched, None)(writeSched)

  val kamonTags = Map( "rawDataset" -> rawDatasetName,
                       "run" -> "Downsampler",
                       "userTimeStart" -> userTimeStart.toString,
                       "userTimeEnd" -> userTimeEnd.toString)

  val schemas = Schemas.fromConfig(filodbConfig).get

  val rawSchemas = rawSchemaNames.map { s => schemas.schemas(s)}
  /**
    * Downsample Schemas
    */
  val dsSchemas = rawSchemaNames.map { s => schemas.schemas(s).downsample.get}

  /**
    * Chunk Downsamplers by Raw Schema Id
    */
  val chunkDownsamplersByRawSchemaId = debox.Map.empty[Int, scala.Seq[ChunkDownsampler]]
  rawSchemas.foreach { s => chunkDownsamplersByRawSchemaId += s.schemaHash -> s.data.downsamplers }

  /**
    * Raw dataset from which we downsample data
    */
  val rawDatasetRef = DatasetRef(rawDatasetName)

  val maxMetaSize = dsSchemas.map(_.data.blockMetaSize).max

  /**
    * Datasets to which we write downsampled data. Keyed by Downsample resolution.
    */
  val downsampleDatasetRefs = downsampleResolutions.map { res =>
    res -> DatasetRef(s"${rawDatasetRef}_ds_${res.toMinutes}")
  }.toMap

  val blockStore = new PageAlignedBlockManager(blockMemorySize,
    stats = new MemoryStats(kamonTags),
    reclaimer = new ReclaimListener {
      override def onReclaim(metadata: Long, numBytes: Int): Unit = {}
    },
    numPagesPerBlock = 50)
  val blockFactory = new BlockMemFactory(blockStore, None, maxMetaSize,
    kamonTags, false)

  val memoryManager = new NativeMemoryManager(nativeMemManagerSize, kamonTags)

  /**
    * Buffer Pool keyed by Raw schema Id
    */
  val bufferPoolByRawSchemaId = debox.Map.empty[Int, WriteBufferPool]
  rawSchemas.foreach { s =>
    val pool = new WriteBufferPool(memoryManager, s.downsample.get.data, downsampleStoreConfig)
    bufferPoolByRawSchemaId += s.schemaHash -> pool
  }

  val shardStats = new TimeSeriesShardStats(rawDatasetRef, -1) // TODO fix

  /**
    * Downsample batch of raw partitions, and store downsampled chunks to cassandra
    */
  private[downsampler] def downsampleBatch(rawPartsBatch: Seq[RawPartData]) = {

    logger.debug(s"Starting downsampling batch of ${rawPartsBatch.size} partitions rawDataset=$rawDatasetName for " +
      s"ingestionTimeStart=$ingestionTimeStart ingestionTimeEnd=$ingestionTimeEnd " +
      s"userTimeStart=$userTimeStart userTimeEnd=$userTimeEnd")

    val downsampledChunksToPersist = MMap[FiniteDuration, Iterator[ChunkSet]]()
    downsampleResolutions.foreach { res =>
      downsampledChunksToPersist(res) = Iterator.empty
    }
    val rawPartsToFree = ArrayBuffer[PagedReadablePartition]()
    val downsampledPartsPartsToFree = ArrayBuffer[TimeSeriesPartition]()
    rawPartsBatch.foreach { rawPart =>
      downsamplePart(rawPart, rawPartsToFree, downsampledPartsPartsToFree, downsampledChunksToPersist)
    }
    persistDownsampledChunks(downsampledChunksToPersist)

    // reclaim all blocks
    blockFactory.markUsedBlocksReclaimable()
    // free partitions
    rawPartsToFree.foreach(_.free())
    rawPartsToFree.clear()
    downsampledPartsPartsToFree.foreach(_.shutdown())
    downsampledPartsPartsToFree.clear()

    logger.info(s"Finished iterating through and downsampling batch of ${rawPartsBatch.size} " +
      s"partitions in current executor")
  }

  /**
    * Creates new downsample partitions per per the resolutions
    * * specified by `bufferPools`.
    * Downsamples all chunks in `partToDownsample` per the resolutions and stores
    * downsampled data into the newly created partition.
    *
    * NOTE THAT THE DOWNSAMPLE PARTITIONS NEED TO BE FREED/SHUT DOWN BY THE CALLER ONCE CHUNKS ARE PERSISTED
    *
    * @param rawPartsToFree raw partitions that need to be freed are added to this mutable list
    * @param downsampledPartsPartsToFree downsample partitions to be freed are added to this mutable list
    * @param downsampledChunksToPersist downsample chunks to persist are added to this mutable map
    */
  private[downsampler] def downsamplePart(rawPart: RawPartData,
                                          rawPartsToFree: ArrayBuffer[PagedReadablePartition],
                                          downsampledPartsPartsToFree: ArrayBuffer[TimeSeriesPartition],
                                          downsampledChunksToPersist: MMap[FiniteDuration, Iterator[ChunkSet]]) = {
    val rawSchemaId = RecordSchema.schemaID(rawPart.partitionKey, UnsafeUtils.arayOffset)
    val rawPartSchema = schemas(rawSchemaId)
    rawPartSchema.downsample match {
      case Some(downsampleSchema) =>
        logger.debug(s"Downsampling partition ${rawPartSchema.partKeySchema.stringify(rawPart.partitionKey)} ")

        val rawReadablePart = new PagedReadablePartition(rawPartSchema, 0, 0,
          rawPart, memoryManager)
        val bufferPool = bufferPoolByRawSchemaId(rawSchemaId)
        val downsamplers = chunkDownsamplersByRawSchemaId(rawSchemaId)

        val downsampledParts = downsampleResolutions.map { res =>
          val part = new TimeSeriesPartition(0, downsampleSchema, rawReadablePart.partitionKey,
                                            0, bufferPool, shardStats, memoryManager, 1)
          res -> part
        }.toMap

        downsampleChunks(rawReadablePart, downsamplers, downsampledParts, userTimeStart)

        rawPartsToFree += rawReadablePart
        downsampledPartsPartsToFree ++= downsampledParts.values

        downsampledParts.foreach { case (res, dsPartition) =>
          dsPartition.switchBuffers(blockFactory, true)
          downsampledChunksToPersist(res) ++= dsPartition.makeFlushChunks(blockFactory)
        }
      case None =>
        logger.warn(s"Encountered partition ${rawPartSchema.partKeySchema.stringify(rawPart.partitionKey)}" +
          s" which does not have a downsample schema")
    }
  }

  /**
    * Downsample chunks in a partition, ingest the downsampled data into downsampled partitions
    *
    * @param rawPartToDownsample raw partition to downsample
    * @param downsamplers chunk downsamplers to use to downsample
    * @param downsampledParts the downsample parts to ingest downsampled data
    * @param downsampleIngestionTime ingestionTime to use for downsampled data
    */
  // scalastyle:off method.length
  private def downsampleChunks(rawPartToDownsample: ReadablePartition,
                               downsamplers: Seq[ChunkDownsampler],
                               downsampledParts: Map[FiniteDuration, TimeSeriesPartition],
                               downsampleIngestionTime: Long) = {
    val timestampCol = 0
    val rawChunksets = rawPartToDownsample.infos(AllChunkScan)

    // TODO create a rowReader that will not box the vals below
    val downsampleRow = new Array[Any](downsamplers.size)
    val downsampleRowReader = SeqRowReader(downsampleRow)

    while (rawChunksets.hasNext) {
      val chunkset = rawChunksets.nextInfo
      val startTime = chunkset.startTime
      val endTime = chunkset.endTime
      val vecPtr = chunkset.vectorPtr(timestampCol)
      val tsReader = rawPartToDownsample.chunkReader(timestampCol, vecPtr).asLongReader

      // for each downsample resolution
      downsampledParts.foreach { case (resolution, part) =>
        val resMillis = resolution.toMillis
        // A sample exactly for 5pm downsampled 5-minutely should fall in the period 4:55:00:001pm to 5:00:00:000pm.
        // Hence subtract - 1 below from chunk startTime to find the first downsample period.
        // + 1 is needed since the startTime is inclusive. We don't want pStart to be 4:55:00:000;
        // instead we want 4:55:00:001
        var pStart = ((startTime - 1) / resMillis) * resMillis + 1
        var pEnd = pStart + resMillis // end is inclusive
        // for each downsample period
        while (pStart <= endTime) {
          if (pEnd >= userTimeStart && pEnd <= userTimeEnd) {
            // fix the boundary row numbers for the downsample period by looking up the timestamp column
            val startRowNum = tsReader.binarySearch(vecPtr, pStart) & 0x7fffffff
            val endRowNum = Math.min(tsReader.ceilingIndex(vecPtr, pEnd), chunkset.numRows - 1)

            // for each downsampler, add downsample column value
            for {col <- downsamplers.indices optimized} {
              val downsampler = downsamplers(col)
              downsampler match {
                case d: TimeChunkDownsampler =>
                  downsampleRow(col) = d.downsampleChunk(rawPartToDownsample, chunkset, startRowNum, endRowNum)
                case d: DoubleChunkDownsampler =>
                  downsampleRow(col) = d.downsampleChunk(rawPartToDownsample, chunkset, startRowNum, endRowNum)
                case h: HistChunkDownsampler =>
                  downsampleRow(col) = h.downsampleChunk(rawPartToDownsample, chunkset, startRowNum, endRowNum)
                    .serialize()
              }
            }
            logger.trace(s"Ingesting into part=${part.hashCode}: $downsampleRow")
            part.ingest(downsampleIngestionTime, downsampleRowReader, blockFactory)
          }
          pStart += resMillis
          pEnd += resMillis
        }
      }
    }
  }

  /**
    * Persist chunks in `downsampledChunksToPersist` to Cassandra.
    */
  private[downsampler] def persistDownsampledChunks(
                                    downsampledChunksToPersist: MMap[FiniteDuration, Iterator[ChunkSet]]): Unit = {
    // write all chunks to cassandra
    val writeFut = downsampledChunksToPersist.map { case (res, chunks) =>
      cassandraColStore.write(downsampleDatasetRefs(res),
        Observable.fromIterator(chunks), ttlByResolution(res))
    }

    writeFut.foreach { fut =>
      val response = Await.result(fut, cassWriteTimeout)
      logger.debug(s"Got message $response for cassandra write call")
      if (response.isInstanceOf[ErrorResponse])
        throw new IllegalStateException(s"Got response $response when writing to Cassandra")
    }
  }

}
