package filodb.downsampler.index

import java.time.Instant
import java.time.format.DateTimeFormatter

import kamon.Kamon
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import filodb.downsampler.DownsamplerContext
import filodb.downsampler.chunk.DownsamplerSettings

/**
  *
  * Goal: Migrate Part keys into downsample cassandra tables.
  *
  * Strategy is to run this spark job every 6 hours at 7:15am, 1:15pm, 7:15pm, 1:15am UTC each day.
  *
  * Run at 7:15am: Will migrate all entries added for update hours 12am, 1am ... and 5am.
  * Run at 1:15pm: Will migrate all entries added for update hours 6am, 7am ... and 11am.
  * Run at 7:15pm: Will migrate all entries added for update hours 12pm, 1pm ... and 5pm.
  * Run at 1:15am: Will migrate all entries added for update hours 6pm, 7pm ... and 11pm.
  *
  * Job behavior can be overridden/controlled in two ways:
  * 1. If `spark.filodb.downsampler.index.doFullMigration` is set to true, full migration is done
  * 2. If `spark.filodb.downsampler.index.timeInPeriodOverride` is set to an ISO timestamp, index migration
  *    for that period will be done. For example: setting to `2020-03-13T15:44:56` will cause migration to
  *    be run for hours 12pm, 1pm ... and 5pm on 2020-03-13
  *
  */
object DSIndexJobMain extends App {

  Kamon.init()  // kamon init should be first thing in driver jvm
  val dsSettings = new DownsamplerSettings()
  val dsIndexJobSettings = new DSIndexJobSettings(dsSettings)

  //migrate partkeys between these hours
  val iu = new IndexJobDriver(dsSettings, dsIndexJobSettings)
  val sparkConf = new SparkConf(loadDefaults = true)
  iu.run(sparkConf)

}

class IndexJobDriver(dsSettings: DownsamplerSettings, dsIndexJobSettings: DSIndexJobSettings) extends Serializable {

  @transient lazy private val jobCompleted = Kamon.counter("index-migration-completed").withoutTags()

  // scalastyle:off method.length
  def run(conf: SparkConf): SparkSession = {
    val spark = SparkSession.builder()
      .appName("FiloDB_Index_Downsampler")
      .config(conf)
      .getOrCreate()

    def hour(millis: Long) = millis / 1000 / 60 / 60

    val timeInMigrationPeriod: Long = spark.sparkContext.getConf
      .getOption("spark.filodb.downsampler.index.timeInPeriodOverride") match {
      // by default assume a time in the previous downsample period
      case None => System.currentTimeMillis() - dsSettings.downsampleChunkDuration
      // examples: 2019-10-20T12:34:56Z  or  2019-10-20T12:34:56-08:00
      case Some(str) => Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(str)).toEpochMilli()
    }

    val hourInMigrationPeriod = hour(timeInMigrationPeriod)
    val jobIntervalInHours = dsIndexJobSettings.batchLookbackInHours
    val fromHour = hourInMigrationPeriod / jobIntervalInHours * jobIntervalInHours

    // Index migration cannot be rerun just for specific hours, since there could have been
    // subsequent updates. Perform migration for all hours until last downsample period's hour.
    val currentHour = hour(System.currentTimeMillis())
    val toHourExclDefault  = currentHour / jobIntervalInHours * jobIntervalInHours

    // this override should almost never used by operators - only for unit testing
    val toHourExcl = spark.sparkContext.getConf
      .getLong("spark.filodb.downsampler.index.toHourExclOverride", toHourExclDefault)

    // This is required in the following scenarios
    // 1. Initial refresh of partkey index to downsampler cluster
    // 2. For fixing corrupt downsampler index
    val doFullMigration = spark.sparkContext.getConf
      .getBoolean("spark.filodb.downsampler.index.doFullMigration", false)

    val job = new DSIndexJob(dsSettings, dsIndexJobSettings)

    DownsamplerContext.dsLogger.info(s"This is the Downsampling Index Migration driver. Starting job... " +
      s"fromHour=$fromHour " +
      s"toHourExcl=$toHourExcl " +
      s"timeInMigrationPeriod=${java.time.Instant.ofEpochMilli(timeInMigrationPeriod)} " +
      s"doFullMigration=$doFullMigration")

    val numShards = dsIndexJobSettings.numShards

    DownsamplerContext.dsLogger.info(s"Spark Job Properties: ${spark.sparkContext.getConf.toDebugString}")
    val startHour = fromHour
    val endHourExcl = toHourExcl
    spark.sparkContext
      .makeRDD(0 until numShards)
      .foreach { shard =>
        Kamon.init() // kamon init should be first thing in worker jvm
        job.updateDSPartKeyIndex(shard, startHour, endHourExcl, doFullMigration)
      }
    DownsamplerContext.dsLogger.info(s"IndexUpdater Driver completed successfully")
    jobCompleted.increment()
    spark
  }

}
