package filodb.labelchurnfinder

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import com.typesafe.scalalogging.{Logger, StrictLogging}
import kamon.Kamon
import monix.execution.Scheduler
import net.ceedubs.ficus.Ficus._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import filodb.cassandra.columnstore.CassandraTokenRangeSplit
import filodb.downsampler.chunk.DownsamplerSettings
import filodb.downsampler.index.DSIndexJobSettings

object LabelChurnFinderMain extends App {
  val dsSettings = new DownsamplerSettings()
  val dsIndexJobSettings = new DSIndexJobSettings(dsSettings)
  val labelChurnFinder = new LabelChurnFinder(dsSettings)
  val sparkConf = new SparkConf(loadDefaults = true)
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val spark = SparkSession.builder()
    .appName("LabelChurnFinder")
    .config(sparkConf)
    .getOrCreate()

  val result = labelChurnFinder.run(spark)
  result.show(truncate = false)
  spark.stop()
}

object LCFContext extends StrictLogging {
  lazy protected[labelchurnfinder] val log: Logger = logger
  lazy val sched = Scheduler.io("cass-read-sched")
}

/**
 * Requires following typesafe config properties:
 *
 * {{{
 * filodb.labelchurnfinder.pk-filters = [
 *  {
 *     _ns_ = "bulk_ns"
 *     _ws_ = "tag_value_as_regex"
 *  }
 * ]
 * filodb.labelchurnfinder.dataset = "dataset_name"
 * # one of the two config below is needed
 * filodb.labelchurnfinder.for-time-period  = 2 days
 * filodb.labelchurnfinder.since-time  = <timestamp string in iso format>
 * }}}
 */
class LabelChurnFinder(dsSettings: DownsamplerSettings) extends Serializable with StrictLogging {

  // scalastyle:off
  def run(spark: SparkSession) = {

    val totalFromTs =
         dsSettings.filodbConfig.as[Option[FiniteDuration]]("labelchurnfinder.for-time-period")
           .map(dur => System.currentTimeMillis() - dur.toMillis)
           .orElse(dsSettings.filodbConfig.as[Option[String]]("labelchurnfinder.since-time")
                             .map( str => Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(str)).toEpochMilli))
           .getOrElse(System.currentTimeMillis() - FiniteDuration(7, TimeUnit.DAYS).toMillis)

    LCFContext.log.info(s"This is the Label Churn Finder. Starting job.")
    val lcfTask = new LcfTask(dsSettings)

    val splitShards = if (lcfTask.colStore.partKeysV2TableEnabled) {
      spark.sparkContext
        .parallelize(lcfTask.colStore.getScanSplits(lcfTask.datasetRef))
        .mapPartitions { split =>
          Kamon.init() // kamon init should be first thing in worker jvm
          split.flatMap(_.asInstanceOf[CassandraTokenRangeSplit].tokens)
        }.map( sp => (sp, -1))
    } else {
      spark.sparkContext
        .parallelize(lcfTask.colStore.getScanSplits(lcfTask.datasetRef))
        .mapPartitions { split =>
          Kamon.init() // kamon init should be first thing in worker jvm
          split.flatMap(_.asInstanceOf[CassandraTokenRangeSplit].tokens)
            .flatMap { split =>
              for { shard <- 0 until lcfTask.numShards } yield (split, shard)
            }
        }
    }

    val labels = splitShards.flatMap { case (split, shard) =>
      lcfTask.fetchLabelValues(split, shard)
    }

    lcfTask.computeChurn(spark, labels, totalFromTs)
  }

}
