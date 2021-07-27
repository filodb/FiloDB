package filodb.downsampler.index

import java.time.Instant
import java.time.format.DateTimeFormatter

import kamon.Kamon
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.quartz._
import org.quartz.impl.StdSchedulerFactory

import filodb.coordinator.KamonShutdownHook
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
  val sparkSession = SparkSession.builder()
                      .appName("FiloDB_Index_Downsampler")
                      .config(sparkConf)
                      .getOrCreate()

  private[this] def scheduleJob(scheduler: Scheduler,
                                sparkSession: SparkSession,
                                cronExpression: String): Unit = {
    val job = JobBuilder.newJob(classOf[SparkDSIndexJob])
      .withIdentity("DSIndexJob", "Group")
      .build
    val trigger = TriggerBuilder
      .newTrigger()
      .withIdentity(s"cron trigger $cronExpression", "triggerGroup")
      .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)).build()
    DownsamplerContext.dsLogger.info(s"triggering job with schedule $cronExpression")
    scheduler.scheduleJob(job, trigger)
  }

  private[this] class SparkDSIndexJob extends Job {
    override def execute(context: JobExecutionContext): Unit = {
      // get the timeInPeriod from JobContext (rerun failed job) or derive from configuration
      val timeInPeriod = context.getScheduler.getContext.getOrDefault("timeInPeriod", -1L)
        .asInstanceOf[Long] match {
        case -1L => timeInMigrationPeriod(sparkSession)
        case time: Long => time
      }
      if (context.getRefireCount > 5) { // triggered job run failed count
        val jee = new JobExecutionException("Retries exceeded the threshold count 3")
        DownsamplerContext.dsLogger
          .error(s"Job failed to run for period: ${java.time.Instant.ofEpochMilli(timeInPeriod)}")
        //make sure job doesn't run again. index job need to run in sequence. so EXIT the job.
        jee.setUnscheduleAllTriggers(true)
        throw jee
      }
      try {
        // set the timeInPeriod in context, will be used for reruns
        context.getScheduler.getContext.put("timeInPeriod", timeInPeriod)
        iu.run(sparkSession, timeInPeriod)
      } catch {
        case e1: Exception =>
          DownsamplerContext.dsLogger.error("exception during scheduled job run", e1)
          val jee = new JobExecutionException(e1)
          Thread.sleep(300000) //backoff for 5 mins
          //fire it again
          jee.setRefireImmediately(true)
          throw jee
      }
    }
  }

  def timeInMigrationPeriod(spark: SparkSession): Long = {
    spark.sparkContext.getConf
      .getOption ("spark.filodb.downsampler.index.timeInPeriodOverride") match {
      // by default assume a time in the previous downsample period
      case None => System.currentTimeMillis () - dsSettings.downsampleChunkDuration
      // examples: 2019-10-20T12:34:56Z  or  2019-10-20T12:34:56-08:00
      case Some (str) => Instant.from (DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse (str) ).toEpochMilli
    }
  }

  if (dsIndexJobSettings.cronEnabled && sparkSession.sparkContext.getConf
      .getOption("spark.filodb.downsampler.index.timeInPeriodOverride").isEmpty) {
    val sf = new StdSchedulerFactory
    val sched = sf.getScheduler()
    scheduleJob(sched, sparkSession, dsIndexJobSettings.cronExpression.get)
  } else
    iu.run(sparkSession, timeInPeriod = timeInMigrationPeriod(sparkSession))
}



class IndexJobDriver(dsSettings: DownsamplerSettings, dsIndexJobSettings: DSIndexJobSettings) extends Serializable {

  @transient lazy private val jobCompleted = Kamon.counter("index-migration-completed").withoutTags()

  // scalastyle:off method.length
  def run(spark: SparkSession, timeInPeriod: Long): SparkSession = {

    def hour(millis: Long) = millis / 1000 / 60 / 60

    val timeInMigrationPeriod: Long = spark.sparkContext.getConf
      .getOption("spark.filodb.downsampler.index.timeInPeriodOverride") match {
      // by default assume a time in the previous downsample period
      case None => System.currentTimeMillis() - dsSettings.downsampleChunkDuration
      // examples: 2019-10-20T12:34:56Z  or  2019-10-20T12:34:56-08:00
      case Some(str) => Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(str)).toEpochMilli
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
    DownsamplerContext.dsLogger.info(s"To rerun this job add the following spark config: " +
      s""""spark.filodb.downsampler.index.timeInPeriodOverride": """ +
      s""""${java.time.Instant.ofEpochMilli(timeInMigrationPeriod)}""""")

    val numShards = dsIndexJobSettings.numShards

    KamonShutdownHook.registerShutdownHook()

    DownsamplerContext.dsLogger.info(s"Spark Job Properties: ${spark.sparkContext.getConf.toDebugString}")
    val startHour = fromHour
    val endHourExcl = toHourExcl
    spark.sparkContext
      .makeRDD(0 until numShards)
      .foreach { shard =>
        Kamon.init()
        KamonShutdownHook.registerShutdownHook()
        job.updateDSPartKeyIndex(shard, startHour, endHourExcl, doFullMigration)
      }
    DownsamplerContext.dsLogger.info(s"IndexUpdater Driver completed successfully")
    jobCompleted.increment()
    spark
  }

}
