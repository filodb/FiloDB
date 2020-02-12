package filodb.downsampler.index

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DSIndexJobMain extends App {
  import DSIndexJobSettings._
  val migrateUpto: Long = hour() - 1
  val iu = new IndexJobDriver(migrateUpto - batchLookbackInHours, migrateUpto) //migrate partkeys between these hours
  val sparkConf = new SparkConf(loadDefaults = true)
  iu.run(sparkConf)
  iu.shutdown()
}

/**
  * Migrate index updates from Raw dataset to Downsampled dataset.
  * Updates get applied only to the dataset with highest ttl.
  *
  * Updates are applied sequentially between the provided hours inclusive. As the updates are incremental, if a job run
  * fails and successive runs complete successfully, migration still needs to happen from the failed batch upto the
  * latest hour. This is to avoid any unwanted overwrites. Hence job will be submitted once to fix the failed cases.
  * For e.g if there was a failure 12 hours ago. Job will be submitted to run once with 12 hours as lookback time to
  * fix the indexes before resuming the regular schedule.
  *
  * @param fromHour from epoch hour - inclusive
  * @param toHour to epoch hour - inclusive
  */
class IndexJobDriver(fromHour: Long, toHour: Long) extends StrictLogging {
  import DSIndexJobSettings._

  def run(conf: SparkConf): Unit = {
    import DSIndexJob._
    val spark = SparkSession.builder()
      .appName("FiloDB_DS_IndexUpdater")
      .config(conf)
      .getOrCreate()

    logger.info(s"Spark Job Properties: ${spark.sparkContext.getConf.toDebugString}")
    val startHour = fromHour
    val endHour = toHour
    val rdd = spark.sparkContext
      .makeRDD(0 until numShards)
      .foreach(updateDSPartKeyIndex(_, startHour, endHour))

    Kamon.counter("index-migration-completed").withoutTags().increment

    logger.info(s"IndexUpdater Driver completed successfully")
  }

  def shutdown(): Unit = {
    import DSIndexJob._
    rawCassandraColStore.shutdown()
    downsampleCassandraColStore.shutdown()
  }

}