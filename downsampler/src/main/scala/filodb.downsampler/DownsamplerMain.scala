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
import filodb.core.store.ChunkSet
import filodb.memory.format.UnsafeUtils

object DownsamplerMain extends App with StrictLogging {

  import DownsamplerSettings._
  import PerSparkExecutorState._

  private val spark = SparkSession.builder()
    .appName("FiloDBDownsampler")
    .getOrCreate()
  private val filoCassRelation = new FiloCassRelation(spark.sqlContext)

  private val timeSeriesRdd = filoCassRelation.buildScan(rawDataset.ref,
                                               ingestionTimeStart, ingestionTimeEnd,
                                               userTimeStart, userTimeEnd, batchSize)

  timeSeriesRdd.foreach { rawPartsBatch =>
    downsampleBatch(rawPartsBatch)
  }
  cassandraColStore.shutdown()
  spark.sparkContext.stop()

  // TODO migrate index entries


  /**
    * Downsample batch of raw partitions, and store downsampled chunks to cassandra
    */
  private[downsampler] def downsampleBatch(rawPartsBatch: Seq[PartDataRow]) = {

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

      val rawSchemaId = RecordSchema.schemaID(rawPart.partData.partitionKey, UnsafeUtils.arayOffset)
      val rawPartSchema = schemas(rawSchemaId)
      val downsampleSchema = rawPartSchema.downsample.get
      logger.debug(s"Downsampling partition ${rawPartSchema.partKeySchema.stringify(rawPart.partData.partitionKey)} ")

      val rawReadablePart = new PagedReadablePartition(rawPartSchema, 0, 0,
        rawPart.partData, memoryManager)
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

  /**
    * Persist chunks in `downsampledChunksToPersist`
    */
  private def persistDownsampledChunks(downsampledChunksToPersist: MMap[FiniteDuration, Iterator[ChunkSet]]): Unit = {
    // write all chunks to cassandra
    val writeFut = downsampledChunksToPersist.map { case (res, chunks) =>
      PerSparkExecutorState.cassandraColStore.write(downsampleDatasets(res).ref,
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
