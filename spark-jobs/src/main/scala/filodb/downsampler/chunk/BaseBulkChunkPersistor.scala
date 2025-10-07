package filodb.downsampler.chunk

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf;

class BaseBulkChunkPersistor extends ChunkPersistor {

    override def persist(downsampledChunks: DataFrame, batchDownsampler: BatchDownsampler): Unit = {
        val downsamplerSettings = batchDownsampler.settings
        val cassandraConfig = downsamplerSettings.cassandraConfig
        val indexTtl = cassandraConfig.getDuration("write-time-index-ttl", TimeUnit.SECONDS).toInt
        downsamplerSettings.downsampleResolutions.foreach { res =>
            val chunksDf = downsampledChunks
              .filter(_.getString(0) == res.toString())
              .select("partition", "chunkid", "info", "chunks")
            persistChunks(chunksDf, res, batchDownsampler)

            val indicesDf = downsampledChunks
              .filter(_.getString(0) == res.toString())
              .select("partition", "ingestion_time", "start_time", "index_info")
              .withColumnRenamed("index_info", "info")
            persistIndices(indicesDf, indexTtl, res, batchDownsampler)
        }
    }

    private def persistChunks(chunksDf: DataFrame, res: FiniteDuration, batchDownsampler: BatchDownsampler): Unit = {
        val ref = batchDownsampler.downsampleRefsByRes(res)
        val chunksTable = batchDownsampler.downsampleCassandraColStore.getOrCreateChunkTable(ref)
        val ttl = batchDownsampler.settings.ttlByResolution(res)
        val writerOptions = getWriterOptions(batchDownsampler, ttl) + (
          "KEYSPACE" -> chunksTable.keyspace, "TABLE" -> chunksTable.tableName
        )
        val format = batchDownsampler.settings.downsamplerConfig.getString("bulk-writer-format")
        chunksDf.write
          .format(format)
          .options(writerOptions)
          .mode("append") // seems like this is required, though "append" is confusing for a key/value db
          .save()
    }

    private def persistIndices(
        indicesDf: DataFrame, ttl: Int, res: FiniteDuration, batchDownsampler: BatchDownsampler
    ): Unit = {

        val ref = batchDownsampler.downsampleRefsByRes(res)
        val indexTable = batchDownsampler.downsampleCassandraColStore.getOrCreateIngestionTimeIndexTable(ref)
        val writerOptions =
            getWriterOptions(batchDownsampler, ttl) + (
              "KEYSPACE" -> indexTable.keyspace,
              "TABLE" -> indexTable.tableName
            )
        val format = batchDownsampler.settings.downsamplerConfig.getString("bulk-writer-format")
        indicesDf.write
          .options(writerOptions)
          .format(format)
          .mode("append")
          .save()
    }

    private def getWriterOptions(batchDownsampler: BatchDownsampler, ttl: Int ): Map[String, String] = {
        val emptyMap = Map[String, String]()
        emptyMap
    }

    override def init(sparkConf: SparkConf): Unit = {}
}
