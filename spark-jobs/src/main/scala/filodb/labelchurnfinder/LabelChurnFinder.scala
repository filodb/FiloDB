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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import filodb.cassandra.columnstore.CassandraTokenRangeSplit
import filodb.downsampler.chunk.DownsamplerSettings
import filodb.labelchurnfinder.LcfTask._

object LabelChurnFinderMain extends App {
  private val dsSettings = new DownsamplerSettings()
  private val labelChurnFinder = new LabelChurnFinder(dsSettings)
  private val sparkConf = new SparkConf(loadDefaults = true)
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val spark = SparkSession.builder()
    .appName("LabelChurnFinder")
    .config(sparkConf)
    .getOrCreate()
  private val result = labelChurnFinder.computeChurn(spark)
  private val highChurnLabels = labelChurnFinder.computeHighChurnLabels(result)
  labelChurnFinder.actionOnHighChurnLabels(highChurnLabels)
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
 *
 * # one of the two config below is needed
 * filodb.labelchurnfinder.for-time-period = 7 days
 * filodb.labelchurnfinder.since-time  = <timestamp string in iso format>
 *
 * # Thresholds for reporting high churn labels
 * filodb.labelchurnfinder.churn-threshold = 1.5
 *
 * # Thresholds for reporting high churn labels
 * filodb.labelchurnfinder.min-ats-threshold = 1000
 * }}}
 */
class LabelChurnFinder(dsSettings: DownsamplerSettings) extends Serializable {

  private val totalFromTs =
    dsSettings.filodbConfig.as[Option[FiniteDuration]]("labelchurnfinder.for-time-period")
      .map(dur => System.currentTimeMillis() - dur.toMillis)
      .orElse(dsSettings.filodbConfig.as[Option[String]]("labelchurnfinder.since-time")
        .map( str => Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(str)).toEpochMilli))
      .getOrElse(System.currentTimeMillis() - FiniteDuration(7, TimeUnit.DAYS).toMillis)

  // Read churn threshold from config, default to 1.0 if not set
  private val churnThreshold = dsSettings.filodbConfig.as[Option[Double]]("labelchurnfinder.churn-threshold")
                                        .getOrElse(1.5)

  // Read min active time series threshold from config,
  private val minAtsThreshold = dsSettings.filodbConfig.as[Option[Double]]("labelchurnfinder.min-ats-threshold")
                                        .getOrElse(1000)

  /**
   * Returns DataFrame with columns:
   * WsNsLabel: Array of [ws, ns, labelName]
   * ActiveCount: Approx count of distinct label values active now (endTime == Long.MaxValue)
   * TotalCount: Approx count of distinct label values active since totalFromTs
   * Churn: TotalCount / ActiveCount (0.0 if ActiveCount is 0)
   *
   * The spark session is not closed by this method.
   */
  // scalastyle:off
  def computeChurn(spark: SparkSession): DataFrame = {

    val lcfTask = new LcfTask(dsSettings)

    LCFContext.log.info(s"This is the Label Churn Finder. Starting job.")
    LCFContext.log.info(s"Computing churn for dataset=${lcfTask.datasetName}" +
      s" totalFromTs=${Instant.ofEpochMilli(totalFromTs).toString} churnThreshold=${churnThreshold}" +
      s" minAtsThreshold=$minAtsThreshold")

    val splitShards = if (lcfTask.colStore.partKeysV2TableEnabled) {
      spark.sparkContext
        .parallelize(lcfTask.colStore.getScanSplits(lcfTask.datasetRef))
        .mapPartitions { split =>
          Kamon.init() // kamon init should be first thing in worker jvm
          split.flatMap(_.asInstanceOf[CassandraTokenRangeSplit].tokens)
        }.map(sp => (sp, -1))
    } else {
      spark.sparkContext
        .parallelize(lcfTask.colStore.getScanSplits(lcfTask.datasetRef))
        .mapPartitions { split =>
          Kamon.init() // kamon init should be first thing in worker jvm
          split.flatMap(_.asInstanceOf[CassandraTokenRangeSplit].tokens)
            .flatMap { split =>
              for {shard <- 0 until lcfTask.numShards} yield (split, shard)
            }
        }
    }

    val labels = splitShards.flatMap { case (split, shard) =>
      LCFContext.log.info(s"Fetching label values iterator for shard=$shard and split=$split")
      lcfTask.fetchLabelValues(split, shard)
    }

    lcfTask.computeChurn(spark, labels, totalFromTs)
  }

  /**
   * Filters the churn DataFrame to return only those rows which have churn >= churnThreshold
   */
  def computeHighChurnLabels(df: DataFrame): DataFrame = {
    df.filter(col(ChurnColName) > churnThreshold && col(ActiveCountColName) >= minAtsThreshold)
  }

  /**
   * Placeholder for future actions on high churn labels like sending alerts, updating aggregation rules etc.
   */
  def actionOnHighChurnLabels(df: DataFrame): Unit = {
    df.withColumn(LabelAndChurnColName, concat_ws("/", col(LabelColName), col(ChurnColName)))
      .groupBy(WsNsColName)
      .agg(collect_list(LabelAndChurnColName).as(HCLabelsColName))
      .foreach { row =>
        LCFContext.log.info(s"High Churn Label Finder Result " +
          s"WsNs=${row.getAs[List[String]](WsNsColName).mkString("/")} " +
          s"HCLabels=${row.getAs[List[String]](HCLabelsColName).mkString(",")} ")
      }
  }

}
