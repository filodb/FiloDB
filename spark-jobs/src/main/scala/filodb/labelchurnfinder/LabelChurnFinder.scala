package filodb.labelchurnfinder

import scala.concurrent.duration.DurationInt

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import net.ceedubs.ficus.Ficus._
import org.apache.datasketches.hll.HllSketch
import org.apache.spark.SparkConf
import org.apache.spark.sql.{functions, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import filodb.cassandra.columnstore.{CassandraColumnStore, CassandraTokenRangeSplit}
import filodb.core.DatasetRef
import filodb.core.metadata.Schemas
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.chunk.DownsamplerSettings
import filodb.memory.format.UnsafeUtils

object LabelChurnFinderMain extends App {
  private val dsSettings = new DownsamplerSettings()
  private val labelChurnFinder = new LabelChurnFinder(dsSettings)
  private val sparkConf = new SparkConf(loadDefaults = true)
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val spark = SparkSession.builder()
    .appName("LabelChurnFinder")
    .config(sparkConf)
    .getOrCreate()
  private val labelStats = labelChurnFinder.computeLabelStats(spark)
  labelChurnFinder.actionOnLabelStats(labelStats)
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
}

case class LabelValRow(ws: String, ns: String, label: String, labelVal: String, endTime: Long)

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
 * }}}
 */
class LabelChurnFinder(dsSettings: DownsamplerSettings) extends Serializable with StrictLogging {

  private val tMinus3d = System.currentTimeMillis() - 3.days.toMillis
  private val tMinus7d = System.currentTimeMillis() - 7.days.toMillis

  import LabelChurnFinder._

  // lazy since they get initialized and used in executors as well
  @transient lazy private val session = DownsamplerContext.getOrCreateCassandraSession(dsSettings.cassandraConfig)
  @transient lazy private[labelchurnfinder] val colStore =
    new CassandraColumnStore(dsSettings.filodbConfig, sched, session, false)(sched)
  @transient lazy val datasetName = dsSettings.filodbConfig.as[String]("labelchurnfinder.dataset")
  @transient lazy val datasetRef = DatasetRef(datasetName)
  @transient lazy private[labelchurnfinder] val filters = dsSettings.filodbConfig
    .as[Seq[Map[String, String]]]("labelchurnfinder.pk-filters").map(_.mapValues(_.r.pattern).toSeq)

  @transient private[labelchurnfinder] val numShards = dsSettings.filodbSettings.streamConfigs
    .find(_.getString("dataset") == datasetName)
    .getOrElse(ConfigFactory.empty())
    .as[Option[Int]]("num-shards").get

  /**
   * Returns iterator of LcfRow objects containing label/value for given token range split and shard.
   */
  private def fetchLabelValues(split: (String, String),
                       shard: Int): Iterator[LabelValRow] = {
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
          pkPairs.map { case (label, labelVal) =>
            LabelValRow(ws, ns, label, labelVal, pk.endTime)
          }
        } else Nil
      }
  }

  /**
   * Returns DataFrame with columns:
   * WsCol, NsCol, LabelCol, Ats1hWithLabelCol, LabelCard1hCol, LabelCard3dCol, LabelCard7dCol, Churn7dCol,
   * Churn3dCol, LabelValueUsageCol
   *
   * The spark session is not closed by this method.
   */
  // scalastyle:off
  def computeLabelStats(spark: SparkSession): DataFrame = {

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
    import spark.implicits._
    val labelsAndValuesDf = splits
        .toDF("startToken", "endToken", "shard")
        .flatMap { split =>
          fetchLabelValues((split.getString(0), split.getString(1)), split.getInt(2))
        }
    computeChurnAndUsage(labelsAndValuesDf, tMinus3d, tMinus7d)
  }

  /**
   * Returns DataFrame with columns:
   * WsCol, NsGroupCol, LabelCol, Ats1hWithLabelCol, LabelSketch1hCol, LabelSketch3dCol, LabelSketch7dCol
   */
  private def computeChurnAndUsage(labelAndValuesDf: Dataset[LabelValRow],
                           tMinus3d: Long,
                           tMinus7d: Long): DataFrame = {

    val hllSketch = udaf(HllSketchAgg())
    labelAndValuesDf
      .groupBy(WsCol, LabelCol)
      .agg(
        count(when(col(EndTimeCol) === Long.MaxValue, col(LabelValCol))).alias(Ats1hWithLabelCol),
        count(when(col(EndTimeCol) > tMinus3d, col(LabelValCol))).alias(Ats3dWithLabelCol),
        count(when(col(EndTimeCol) > tMinus7d, col(LabelValCol))).alias(Ats7dWithLabelCol),
        hllSketch(when(col(EndTimeCol) === Long.MaxValue, col(LabelValCol))).alias(LabelSketch1hCol),
        hllSketch(when(col(EndTimeCol) >= tMinus3d, col(LabelValCol))).alias(LabelSketch3dCol),
        hllSketch(when(col(EndTimeCol) >= tMinus7d, col(LabelValCol))).alias(LabelSketch7dCol)
      )
      .withColumn(NsGroupCol, lit("All")) // placeholder for future ns grouping
  }

  /**
   * Placeholder for future actions on label stats - example, sending it to a churn monitoring system etc.
   */
  def actionOnLabelStats(df: DataFrame): Unit = {
    countsFromSketches(df).show(truncate = false)
  }

  protected[labelchurnfinder] def countsFromSketches(df: DataFrame): DataFrame = {
    val countUDF = functions.udf { sketch: Array[Byte] =>
      HllSketch.heapify(sketch).getEstimate
    }
    df.withColumn(LabelCard1h, countUDF(col(LabelSketch1hCol)))
      .withColumn(LabelCard3d, countUDF(col(LabelSketch3dCol)))
      .withColumn(LabelCard7d, countUDF(col(LabelSketch7dCol)))

  }

}
