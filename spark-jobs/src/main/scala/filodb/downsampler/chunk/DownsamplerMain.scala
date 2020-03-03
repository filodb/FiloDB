package filodb.downsampler.chunk

import kamon.Kamon
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import filodb.downsampler.DownsamplerLogger

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
  * Important Note: The reason non-real-time data is not included in goals is because we
  * want chunk alignment between DCs in downsampled data to enable cross-dc repair without chunk surgery.
  * Without chunk-alignment in raw data and consistency in behavior across DCs, it would be difficult
  * to achieve chunk alignment in downsampled data. Once we solve that (deferred problem), we will
  * lift the constraint.
  */
object DownsamplerMain extends App {

  val settings = new DownsamplerSettings()
  val batchDownsampler = new BatchDownsampler(settings)

  val d = new Downsampler(settings, batchDownsampler)
  val sparkConf = new SparkConf(loadDefaults = true)
  d.run(sparkConf)
  d.shutdown()
}

class Downsampler(settings: DownsamplerSettings, batchDownsampler: BatchDownsampler) extends Serializable {

//  import java.time.Instant._

  def shutdown(): Unit = {
    batchDownsampler.rawCassandraColStore.shutdown()
    batchDownsampler.downsampleCassandraColStore.shutdown()
  }

  // Gotcha!! Need separate function (Cannot be within body of a class)
  // to create a closure for spark to serialize and move to executors.
  // Otherwise, config values below were not being sent over.
  // scalastyle:off method.length
  def run(sparkConf: SparkConf): Unit = {

    val spark = SparkSession.builder()
      .appName("FiloDBDownsampler")
      .config(sparkConf)
      .getOrCreate()

    DownsamplerLogger.dsLogger.info(s"Spark Job Properties: ${spark.sparkContext.getConf.toDebugString}")

    // Use the spark property spark.filodb.downsampler.user-time-override to override the
    // userTime period for which downsampling should occur.
    // Generally disabled, defaults the period that just ended prior to now.
    // Specified during reruns for downsampling old data
    val userTimeInPeriod: Long = spark.sparkContext.getConf.get("spark.filodb.downsampler.userTimeOverride",
      s"${System.currentTimeMillis() - settings.downsampleChunkDuration}").toLong
    // by default assume a time in the previous downsample period

    val userTimeStart: Long = (userTimeInPeriod / settings.downsampleChunkDuration) * settings.downsampleChunkDuration
    val userTimeEndExclusive: Long = userTimeStart + settings.downsampleChunkDuration
    val ingestionTimeStart: Long = userTimeStart - settings.widenIngestionTimeRangeBy.toMillis
    val ingestionTimeEnd: Long = userTimeEndExclusive + settings.widenIngestionTimeRangeBy.toMillis

//    DownsamplerLogger.dsLogger.info(s"This is the Downsampling driver. Starting downsampling job " +
//      s"rawDataset=${settings.rawDatasetName} for userTimeInPeriod=${ofEpochMilli(userTimeInPeriod)} " +
//      s"ingestionTimeStart=${ofEpochMilli(ingestionTimeStart)} " +
//      s"ingestionTimeEnd=${ofEpochMilli(ingestionTimeEnd)} " +
//      s"userTimeStart=${ofEpochMilli(userTimeStart)} userTimeEndExclusive=${ofEpochMilli(userTimeEndExclusive)}")

    val splits = batchDownsampler.rawCassandraColStore.getScanSplits(batchDownsampler.rawDatasetRef,
                                                                     settings.splitsPerNode)
    DownsamplerLogger.dsLogger.info(s"Cassandra split size: ${splits.size}. We will have this many spark partitions. " +
      s"Tune splitsPerNode which was ${settings.splitsPerNode} if parallelism is low")

    spark.sparkContext
      .makeRDD(splits)
      .mapPartitions { splitIter =>
        import filodb.core.Iterators._
        val rawDataSource = batchDownsampler.rawCassandraColStore
        val batchReadSpan = Kamon.spanBuilder("cassandra-raw-data-read-latency").start()
        val batchIter = rawDataSource.getChunksByIngestionTimeRange(datasetRef = batchDownsampler.rawDatasetRef,
          splits = splitIter, ingestionTimeStart = ingestionTimeStart,
          ingestionTimeEnd = ingestionTimeEnd,
          userTimeStart = userTimeStart, endTimeExclusive = userTimeEndExclusive,
          maxChunkTime = settings.rawDatasetIngestionConfig.storeConfig.maxChunkTime.toMillis,
          batchSize = settings.batchSize, batchTime = settings.batchTime).toIterator()
        batchReadSpan.finish()
        batchIter // iterator of batches
      }
      .foreach { rawPartsBatch =>
        batchDownsampler.downsampleBatch(rawPartsBatch, userTimeStart, userTimeEndExclusive)
      }

    DownsamplerLogger.dsLogger.info(s"Downsampling Driver completed successfully")
  }

}
