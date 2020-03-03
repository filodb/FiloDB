package filodb.downsampler.index

import kamon.Kamon
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import filodb.downsampler.DownsamplerLogger
import filodb.downsampler.chunk.DownsamplerSettings

object DSIndexJobMain extends App {

  val dsSettings = new DownsamplerSettings()
  val dsIndexJobSettings = new DSIndexJobSettings(dsSettings)

  val migrateUpto: Long = hour() - 1
  //migrate partkeys between these hours
  val iu = new IndexJobDriver(migrateUpto - dsIndexJobSettings.batchLookbackInHours,
                              migrateUpto, dsSettings, dsIndexJobSettings)
  val sparkConf = new SparkConf(loadDefaults = true)
  iu.run(sparkConf)

  def hour(millis: Long = System.currentTimeMillis()): Long = millis / 1000 / 60 / 60

}

/**
  * Migrate index updates from Raw dataset to Downsampled dataset.
  * Updates get applied only to the dataset with highest ttl.
  *
  * Updates are applied sequentially between the provided hours inclusive. As the updates are incremental, if a job run
  * fails and successive runs complete successfully, migration still needs to happen from the failed batch upto the
  * latest hour. This is to ensure that subsequent mutations were not overwritten. Hence job will be submitted once to
  * fix the failed cases.
  *
  * For e.g if there was a failure 12 hours ago. Job will be submitted to run once with 12 hours as lookback time to
  * fix the indexes before resuming the regular schedule.
  *
  * @param fromHour from epoch hour - inclusive
  * @param toHour to epoch hour - inclusive
  */
class IndexJobDriver(fromHour: Long,
                     toHour: Long,
                     dsSettings: DownsamplerSettings,
                     dsIndexJobSettings: DSIndexJobSettings) extends Serializable {

  def run(conf: SparkConf): Unit = {
    val spark = SparkSession.builder()
      .appName("FiloDB_DS_IndexUpdater")
      .config(conf)
      .getOrCreate()

    DownsamplerLogger.dsLogger.info(s"Spark Job Properties: ${spark.sparkContext.getConf.toDebugString}")
    val startHour = fromHour
    val endHour = toHour
    spark.sparkContext
      .makeRDD(0 until dsIndexJobSettings.numShards)
      .foreach { shard =>
        val job = new DSIndexJob(dsSettings, dsIndexJobSettings)
        job.updateDSPartKeyIndex(shard, startHour, endHour)
      }

    Kamon.counter("index-migration-completed").withoutTags().increment
    DownsamplerLogger.dsLogger.info(s"IndexUpdater Driver completed successfully")
  }

}