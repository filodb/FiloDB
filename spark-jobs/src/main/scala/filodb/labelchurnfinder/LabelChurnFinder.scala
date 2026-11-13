package filodb.labelchurnfinder

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.execution.Scheduler
import net.ceedubs.ficus.Ficus._
import org.apache.datasketches.hll.HllSketch
import org.apache.spark.SparkConf
import org.apache.spark.sql.{functions, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import filodb.cassandra.columnstore.CassandraTokenRangeSplit
import filodb.coordinator.KamonShutdownHook
import filodb.core.DatasetRef
import filodb.core.metadata.Schemas
import filodb.core.metrics.FilodbMetrics
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.chunk.DownsamplerSettings
import filodb.memory.format.UnsafeUtils

object LabelChurnFinderMain extends App with StrictLogging {
  Kamon.init()
  KamonShutdownHook.registerShutdownHook()

  private val jobStartMs = System.currentTimeMillis()

  @transient private lazy val jobDuration         =
    FilodbMetrics.timeHistogram("lcf_job_duration", TimeUnit.MILLISECONDS)
  @transient private lazy val workspacesPublished =
    FilodbMetrics.counter("lcf_workspaces_published_total")
  @transient private lazy val labelsPublished     =
    FilodbMetrics.counter("lcf_labels_published_total")
  @transient private lazy val kafkaFailures       =
    FilodbMetrics.counter("lcf_kafka_publish_failures_total")
  @transient private lazy val icebergExportDuration =
    FilodbMetrics.timeHistogram("lcf_iceberg_export_duration", TimeUnit.MILLISECONDS)
  @transient private lazy val icebergExportFailures =
    FilodbMetrics.counter("lcf_iceberg_export_failures_total")

  private val dsSettings = new DownsamplerSettings()
  private val labelChurnFinder = new LabelChurnFinder(dsSettings)
  private val sparkConf = new SparkConf(loadDefaults = true)
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  // Team shared secrets are exposed as env vars
  // the AppleConnect S3A provider reads them from spark.hadoop.* properties
  private val appleConnectEnvToConf = Map(
    "s3_ac_username"    -> "spark.hadoop.fs.s3a.appleconnect.username",
    "s3_ac_password"    -> "spark.hadoop.fs.s3a.appleconnect.password",
    "s3_totp_secret"    -> "spark.hadoop.fs.s3a.appleconnect.totpSecret",
    "s3_ac_device_id"   -> "spark.hadoop.fs.s3a.appleconnect.deviceId",
    "aws_dev_role"      -> "spark.hadoop.fs.s3a.appleconnect.awsrole",
    "s3_aws_account_id" -> "spark.hadoop.fs.s3a.appleconnect.accountId"
  )
  // Only these are printed in full, for the rest only the length is logged
  private val nonSecretEnvNames = Set("s3_ac_username", "aws_dev_role", "s3_aws_account_id")
  appleConnectEnvToConf.foreach { case (envName, confKey) =>
    sys.env.get(envName) match {
      case Some(value) =>
        sparkConf.set(confKey, value)
        val shown = if (nonSecretEnvNames.contains(envName)) value else "<hidden>"
        logger.info(s"AppleConnect env var $envName is set: value=$shown length=${value.length}")
      case None =>
        logger.warn(s"AppleConnect env var $envName is not set")
    }
  }

  val spark = SparkSession.builder()
    .appName("LabelChurnFinder")
    .config(sparkConf)
    .getOrCreate()

  // Spark accumulators collect executor-side counts back to the driver
  private val failuresAcc   = spark.sparkContext.longAccumulator("lcf_kafka_failures")
  private val labelsAcc     = spark.sparkContext.longAccumulator("lcf_labels_published")
  private val workspacesAcc = spark.sparkContext.longAccumulator("lcf_workspaces_published")

  private val partKeys       = labelChurnFinder.scanPartKeys(spark)

  // Export to Iceberg before publishing to Kafka.
  // A failed export does not block the Kafka stats.
  // cache() lets the stats below reuse this single scan.
  if (dsSettings.filodbConfig.as[Boolean]("labelchurnfinder.iceberg-export.enabled")) {
    partKeys.cache()
    val exportStartMs = System.currentTimeMillis()
    try {
      new PartKeyIcebergExporter(dsSettings.filodbConfig).exportPartKeys(spark, partKeys)
    } catch {
      case NonFatal(e) =>
        icebergExportFailures.increment()
        logger.error("Export of partKeys to Iceberg failed, continuing with publishing label stats", e)
    }
    icebergExportDuration.record(System.currentTimeMillis() - exportStartMs)
  }

  private val labelStats     = labelChurnFinder.computeLabelStats(labelChurnFinder.explodeLabels(partKeys))
  private val publishToKafka = dsSettings.filodbConfig.as[Boolean]("labelchurnfinder.publish-to-kafka")
  private val producer = if (publishToKafka)
    Some(new LabelStatsKafkaProducer(dsSettings.filodbConfig, failuresAcc, labelsAcc, workspacesAcc))
  else None

  labelChurnFinder.actionOnLabelStats(labelStats, producer)
  partKeys.unpersist() // release the cache before the wait below, the partKeys are not used anymore

  // After the Spark job completes, read accumulator totals and record to OTel on the driver
  workspacesPublished.increment(workspacesAcc.value)
  labelsPublished.increment(labelsAcc.value)
  kafkaFailures.increment(failuresAcc.value)
  jobDuration.record(System.currentTimeMillis() - jobStartMs)
  Thread.sleep(62000) // allow Kamon MosaicReporter periodic tick to fire and complete gRPC publish before shutdown
  spark.stop()
}

object LabelChurnFinder {
  private lazy val sched = Scheduler.io("cass-read-sched")
  private[labelchurnfinder] val WsCol = "ws"
  private[labelchurnfinder] val NsGroupCol = "nsGroup"
  private[labelchurnfinder] val LabelCol = "label"
  private[labelchurnfinder] val LabelValCol = "labelVal"
  private[labelchurnfinder] val EndTimeCol = "endTime"
  private[labelchurnfinder] val LabelSketch1hCol = "labelSketch1h"
  private[labelchurnfinder] val LabelSketch7dCol = "labelSketch7d"
  private[labelchurnfinder] val LabelSketch3dCol = "labelSketch3d"
  private[labelchurnfinder] val Ats1hWithLabelCol = "ats1hWithLabel"
  private[labelchurnfinder] val Ats3dWithLabelCol = "ats3dWithLabel"
  private[labelchurnfinder] val Ats7dWithLabelCol = "ats7dWithLabel"
  private[labelchurnfinder] val LabelCard1h = "labelCard1h"
  private[labelchurnfinder] val LabelCard3d = "labelCard3d"
  private[labelchurnfinder] val LabelCard7d = "labelCard7d"
  private[labelchurnfinder] val SaltCol     = "salt"
}

case class LabelValRow(ws: String, ns: String, label: String, labelVal: String, endTime: Long)

/**
 * One time series (partKey) from the Cassandra partKeys table, with all of its labels decoded.
 */
case class PartKeyRow(ws: String, ns: String, metric: String, labels: Map[String, String],
                      startTime: Long, endTime: Long)

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
 * filodb.labelchurnfinder.raw-dataset-name = "dataset_name"
 * }}}
 */
class LabelChurnFinder(dsSettings: DownsamplerSettings) extends Serializable with StrictLogging {

  private val tMinus3d = System.currentTimeMillis() - 3.days.toMillis
  private val tMinus7d = System.currentTimeMillis() - 7.days.toMillis

  import LabelChurnFinder._


  private[labelchurnfinder] def colStore =
    DownsamplerContext.getOrCreateCassandraColumnStore(dsSettings.filodbConfig, dsSettings.cassandraConfig)(sched)

  @transient lazy val datasetName = dsSettings.filodbConfig.as[String]("labelchurnfinder.raw-dataset-name")
  @transient lazy val datasetRef = DatasetRef(datasetName)
  @transient lazy private[labelchurnfinder] val filters = dsSettings.filodbConfig
    .as[Seq[Map[String, String]]]("labelchurnfinder.pk-filters").map(_.map { case (k, v) => k -> v.r.pattern }.toSeq)

  @transient private[labelchurnfinder] val numShards = dsSettings.filodbSettings.streamConfigs
    .find(_.getString("dataset") == datasetName)
    .getOrElse(ConfigFactory.empty())
    .as[Option[Int]]("num-shards").get

  @transient private[labelchurnfinder] val numSalts =
    dsSettings.filodbConfig.as[Option[Int]]("labelchurnfinder.num-salts").getOrElse(50)

  /**
   * Returns iterator of PartKeyRow objects, one per time series, for given token range split and shard.
   */
  private def fetchPartKeys(split: (String, String),
                       shard: Int): Iterator[PartKeyRow] = {
    colStore.scanPartKeysByStartEndTimeRangeNoAsync(datasetRef, shard, split, 0,
        Long.MaxValue, 0, Long.MaxValue)
      .flatMap { pk  =>
        val pkPairs = Schemas.global.part.binSchema.toStringPairs(pk.partKey, UnsafeUtils.arayOffset)
        val filterMatches = filters.exists { filter => // at least one filter should match
          filter.forall { case (filterKey, filterValRegex) => // should match all tags in this filter
            pkPairs.exists { case (pkKey, pkVal) =>
              pkKey == filterKey && filterValRegex.matcher(pkVal).matches
            }
          }
        }
        if (filterMatches) {
          val ws = pkPairs.find(_._1 == "_ws_").map(_._2).getOrElse("unknown!!")
          val ns = pkPairs.find(_._1 == "_ns_").map(_._2).getOrElse("unknown!!")
          val metric = pkPairs.find(_._1 == "_metric_").map(_._2).getOrElse("unknown!!")
          Seq(PartKeyRow(ws, ns, metric, pkPairs.toMap, pk.startTime, pk.endTime))
        } else Nil
      }
  }

  /**
   * Scans the partKeys table and returns one PartKeyRow per time series matching the pk-filters.
   */
  // scalastyle:off
  def scanPartKeys(spark: SparkSession): Dataset[PartKeyRow] = {

    logger.info(s"This is the Label Churn Finder. Starting job for dataset=$datasetName with filters=$filters")

    val splits = colStore.getScanSplits(datasetRef).flatMap { split =>
      if (colStore.partKeysV2TableEnabled) {
        for {
          t <- split.asInstanceOf[CassandraTokenRangeSplit].tokens
        } yield (t._1, t._2, -1)
      } else {
        for {
          t <- split.asInstanceOf[CassandraTokenRangeSplit].tokens
          shard <- 0 until numShards
        } yield (t._1, t._2, shard)
      }
    }
    logger.info(s"Splits count: ${splits.size}, partKeysV2TableEnabled=${colStore.partKeysV2TableEnabled}")
    import spark.implicits._
    splits
        .toDF("startToken", "endToken", "shard")
        .flatMap { split =>
          fetchPartKeys((split.getString(0), split.getString(1)), split.getInt(2))
        }
  }

  /**
   * Returns one LabelValRow per label of every time series in partKeys.
   */
  def explodeLabels(partKeys: Dataset[PartKeyRow]): Dataset[LabelValRow] = {
    import partKeys.sparkSession.implicits._
    partKeys.flatMap { pk =>
      pk.labels.map { case (label, labelVal) => LabelValRow(pk.ws, pk.ns, label, labelVal, pk.endTime) }
    }
  }

  /**
   * Returns DataFrame with columns:
   * WsCol, NsCol, LabelCol, Ats1hWithLabelCol, LabelCard1hCol, LabelCard3dCol, LabelCard7dCol, Churn7dCol,
   * Churn3dCol, LabelValueUsageCol
   *
   * The spark session is not closed by this method.
   */
  def computeLabelStats(labelsAndValuesDf: Dataset[LabelValRow]): DataFrame =
    computeChurnAndUsage(labelsAndValuesDf, tMinus3d, tMinus7d)

  /**
   * Returns DataFrame with columns:
   * WsCol, NsGroupCol, LabelCol, Ats1hWithLabelCol, LabelSketch1hCol, LabelSketch3dCol, LabelSketch7dCol
   */
  private def computeChurnAndUsage(labelAndValuesDf: Dataset[LabelValRow],
                           tMinus3d: Long,
                           tMinus7d: Long): DataFrame = {

    val hllSketch = udaf(HllSketchAgg())
    val hllMerge  = udaf(HllSketchMergeAgg())

    // Phase 1: salt by row position to break up skewed (ws, label) keys across numSalts partitions.
    val partial = labelAndValuesDf
      .withColumn(SaltCol, pmod(monotonically_increasing_id(), lit(numSalts)))
      .groupBy(WsCol, LabelCol, SaltCol)
      .agg(
        count(when(col(EndTimeCol) === Long.MaxValue, col(LabelValCol))).alias(Ats1hWithLabelCol),
        count(when(col(EndTimeCol) > tMinus3d, col(LabelValCol))).alias(Ats3dWithLabelCol),
        count(when(col(EndTimeCol) > tMinus7d, col(LabelValCol))).alias(Ats7dWithLabelCol),
        hllSketch(when(col(EndTimeCol) === Long.MaxValue, col(LabelValCol))).alias(LabelSketch1hCol),
        hllSketch(when(col(EndTimeCol) >= tMinus3d, col(LabelValCol))).alias(LabelSketch3dCol),
        hllSketch(when(col(EndTimeCol) >= tMinus7d, col(LabelValCol))).alias(LabelSketch7dCol)
      )

    // Phase 2: merge partial results per (ws, label).
    partial
      .groupBy(WsCol, LabelCol)
      .agg(
        sum(col(Ats1hWithLabelCol)).alias(Ats1hWithLabelCol),
        sum(col(Ats3dWithLabelCol)).alias(Ats3dWithLabelCol),
        sum(col(Ats7dWithLabelCol)).alias(Ats7dWithLabelCol),
        hllMerge(col(LabelSketch1hCol)).alias(LabelSketch1hCol),
        hllMerge(col(LabelSketch3dCol)).alias(LabelSketch3dCol),
        hllMerge(col(LabelSketch7dCol)).alias(LabelSketch7dCol)
      )
      .withColumn(NsGroupCol, lit("All")) // placeholder for future ns grouping
  }

  /**
   * Publishes to Kafka if configured, otherwise logs label statistics summary.
   */
  def actionOnLabelStats(df: DataFrame, producer: Option[LabelStatsKafkaProducer]): Unit =
    producer match {
      case Some(p) => p.publishLabelStats(df)
      case None    => logLabelStats(df)
    }

  private def logLabelStats(df: DataFrame): Unit = {
    val cols = Seq(WsCol, LabelCol, Ats1hWithLabelCol, Ats3dWithLabelCol, Ats7dWithLabelCol,
      LabelCard1h, LabelCard3d, LabelCard7d)
    countsFromSketches(df).foreach { r =>
      logger.info(s"Label stats ${cols.map(c => s"$c=${r.getAs[Any](c)}").mkString(" ")}")
    }
  }

  protected[labelchurnfinder] def countsFromSketches(df: DataFrame): DataFrame = {
    val countUDF = functions.udf { sketch: Array[Byte] =>
      Math.round(HllSketch.heapify(sketch).getEstimate)
    }
    df.withColumn(LabelCard1h, countUDF(col(LabelSketch1hCol)))
      .withColumn(LabelCard3d, countUDF(col(LabelSketch3dCol)))
      .withColumn(LabelCard7d, countUDF(col(LabelSketch7dCol)))
  }

}
