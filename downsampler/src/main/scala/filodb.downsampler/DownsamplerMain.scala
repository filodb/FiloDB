package filodb.downsampler

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession

import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore.PagedReadablePartition
import filodb.memory.format.UnsafeUtils

object DownsamplerMain extends App with StrictLogging {

  private val spark = SparkSession.builder()
    .appName("FiloDBDownsampler")
    .getOrCreate()
  private val filoCassRelation = new FiloCassRelation(spark.sqlContext)

  import DownsamplerSettings._
  import PerSparkExecutorState._

  private val rdd = filoCassRelation.buildScan(rawDataset.ref,
                                                ingestionTimeStart, ingestionTimeEnd,
                                                userTimeStart, userTimeEnd, batchSize)

  rdd.foreach { rawParts =>

    logger.debug(s"Starting downsampling job for rawDataset=$rawDatasetName for " +
      s"ingestionTimeStart=$ingestionTimeStart" +
      s"ingestionTimeEnd=$ingestionTimeEnd " +
      s"userTimeStart=$userTimeStart " +
      s"userTimeEnd=$userTimeEnd" +
      s"for ${rawParts.size} partitions")

    rawParts.foreach { rawPart =>
      val rawSchemaId = RecordSchema.schemaID(rawPart.partData.partitionKey, UnsafeUtils.arayOffset)
      val rawPartSchema = schemas(rawSchemaId)
      val downsampleSchema = rawPartSchema.downsample.get
      logger.debug(s"Downsampling partition ${rawPartSchema.partKeySchema.stringify(rawPart.partData.partitionKey)} ")

      val rawReadablePart = new PagedReadablePartition(rawPartSchema, 0, 0,
        rawPart.partData, memoryManager)
      val downsampledParts = PartitionDownsampler.downsample(shardStats,
        downsampleSchema,
        chunkDownsamplersByRawSchemaId(rawSchemaId),
        rawReadablePart, blockFactory,
        bufferPoolsByRawSchemaId(rawSchemaId),
        memoryManager,
        userTimeStart, // use userTime as ingestionTime for downsampled data
        userTimeStart, userTimeEnd)
      rawPartsToFree += rawReadablePart
      downsampledPartsPartsToFree ++= downsampledParts.values

      downsampledParts.foreach { case (res, dsPartition) =>
        dsPartition.switchBuffers(blockFactory, true)
        downsampledChunksToPersist(res) = downsampledChunksToPersist(res) ++ dsPartition.makeFlushChunks(blockFactory)
      }
    }
    persistChunks()
    logger.info(s"Finished iterating through and downsampling  ${rawParts.size} partitions in current executor")
  }

  cassandraColStore.shutdown()
  spark.sparkContext.stop()

  // TODO migrate index entries

}
