package filodb.downsampler

import scala.collection.mutable.{ArrayBuffer, Map => MMap}
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

import com.typesafe.scalalogging.StrictLogging
import monix.reactive.Observable
import org.apache.spark.sql.SparkSession

import filodb.core.ErrorResponse
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore.{PagedReadablePartition, TimeSeriesPartition}
import filodb.core.store.{ChunkSet, RawPartData}
import filodb.memory.format.UnsafeUtils

/**
  *
  * Goal: Downsample all real-time data.
  * Goal: Align chunks when this job is run in multiple DCs so that cross-dc repairs can be done.
  * Non-Goal: Downsampling of non-real time data or data with different epoch.
  *
  * Strategy is to run this spark job every 6 hours at 8am, 2pm, 8pm, 2am each day.
  *
  * Run at 8am: We query data with ingestionTime from 10pm to 8am.
  *             Then query and downsample data with userTime between 12am to 6am.
  *             Downsampled chunk would have an ingestionTime of 12am.
  * Run at 2pm: We query data with ingestionTime from 8am to 2pm.
  *             Then query and downsample data with userTime between 6am to 12pm.
  *             Downsampled chunk would have an ingestionTime of 6am.
  * Run at 8pm: We query data with ingestionTime from 10am to 8pm.
  *             Then query and downsample data with userTime between 12pm to 6pm.
  *             Downsampled chunk would have an ingestionTime of 12pm.
  * Run at 2am: We query data with ingestionTime from 8pm to 2am.
  *             Then query and downsample data with userTime between 6pm to 12am.
  *             Downsampled chunk would have an ingestionTime of 6pm.
  *
  * This will cover all data with userTime 12am to 12am.
  * Since we query for a broader ingestionTime, it will include data arriving early/late by 2 hours.
  *
  */
object DownsamplerMain extends App with StrictLogging {

  import DownsamplerSettings._
  import PerSparkExecutorState._
  import filodb.core.Iterators._

  private val spark = SparkSession.builder()
    .appName("FiloDBDownsampler")
    .getOrCreate()

  val splits = PerSparkExecutorState.cassandraColStore.getScanSplits(rawDatasetRef)

  val timeSeriesRdd = spark.sparkContext
    .makeRDD(splits)
    .mapPartitions { splitIter =>
      val rawDataSource = PerSparkExecutorState.cassandraColStore
      rawDataSource.getChunksByIngestionTimeRange(rawDatasetRef, splitIter,
        ingestionTimeStart, ingestionTimeEnd,
        userTimeStart, userTimeEnd, batchSize)
        .toIterator()
    }

  timeSeriesRdd.foreach { rawPartsBatch =>
    downsampleBatch(rawPartsBatch)
  }
  cassandraColStore.shutdown()
  spark.sparkContext.stop()

  // TODO migrate index entries

  /**
    * Downsample batch of raw partitions, and store downsampled chunks to cassandra
    */
  private[downsampler] def downsampleBatch(rawPartsBatch: Seq[RawPartData]) = {

    logger.debug(s"Starting downsampling job for rawDataset=$rawDatasetName for " +
      s"ingestionTimeStart=$ingestionTimeStart ingestionTimeEnd=$ingestionTimeEnd " +
      s"userTimeStart=$userTimeStart userTimeEnd=$userTimeEnd for ${rawPartsBatch.size} partitions")

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

    logger.info(s"Finished iterating through and downsampling ${rawPartsBatch.size} partitions in current executor")
  }

  private def downsamplePart(rawPart: RawPartData,
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
        val downsampledParts = PartitionDownsampler.downsample(shardStats,
          downsampleResolutions, downsampleSchema, chunkDownsamplersByRawSchemaId(rawSchemaId),
          rawReadablePart, blockFactory, bufferPoolByRawSchemaId(rawSchemaId),
          memoryManager, userTimeStart, // use userTime as ingestionTime for downsampled data
          userTimeStart, userTimeEnd)
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
    * Persist chunks in `downsampledChunksToPersist`
    */
  private def persistDownsampledChunks(downsampledChunksToPersist: MMap[FiniteDuration, Iterator[ChunkSet]]): Unit = {
    // write all chunks to cassandra
    val writeFut = downsampledChunksToPersist.map { case (res, chunks) =>
      PerSparkExecutorState.cassandraColStore.write(downsampleDatasetRefs(res),
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
