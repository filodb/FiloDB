package filodb.labelchurnfinder

import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DoubleType, LongType, StringType, StructField, StructType}

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Schemas
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.chunk.DownsamplerSettings
import filodb.labelchurnfinder.LCFContext.sched
import filodb.labelchurnfinder.LcfTask._
import filodb.memory.format.UnsafeUtils

object LcfTask {

  val WsNsColName = "WsNs"
  val LabelColName = "Label"
  val LabelValColName = "LabelVal"
  val StartTimeColName = "StartTime"
  val EndTimeColName = "EndTime"
  val ActiveCountColName = "ActiveCount"
  val TotalCountColName = "TotalCount"
  val ChurnColName = "Churn"
  val LabelAndChurnColName = "LabelAndChurn"
  val HCLabelsColName = "HCLabels"

  val dfSchema: StructType = new StructType()
    .add(StructField(WsNsColName, ArrayType(StringType, containsNull = false), nullable = false))
    .add(StructField(LabelColName, StringType, nullable = false))
    .add(StructField(LabelValColName, StringType, nullable = false))
    .add(StructField(StartTimeColName, LongType, nullable = false))
    .add(StructField(EndTimeColName, LongType, nullable = false))
}

class LcfTask(dsSettings: DownsamplerSettings) extends Serializable {

  @transient lazy private val schemas = Schemas.fromConfig(dsSettings.filodbConfig).get
  @transient lazy val datasetName = dsSettings.filodbConfig.as[String]("labelchurnfinder.dataset")
  @transient lazy val datasetRef = DatasetRef(datasetName)

  @transient lazy private val session = DownsamplerContext.getOrCreateCassandraSession(dsSettings.cassandraConfig)
  @transient lazy private[labelchurnfinder] val colStore =
    new CassandraColumnStore(dsSettings.filodbConfig, sched, session, false)(sched)
  @transient lazy private[labelchurnfinder] val filters = dsSettings.filodbConfig
    .as[Seq[Map[String, String]]]("labelchurnfinder.pk-filters").map(_.mapValues(_.r.pattern).toSeq)

  @transient lazy private[labelchurnfinder] val numShards = dsSettings.filodbSettings.streamConfigs
    .find(_.getString("dataset") == datasetName)
    .getOrElse(ConfigFactory.empty())
    .as[Option[Int]]("num-shards").get

  /**
   * Returns iterator of Rows containing label/value for given token range split and shard.
   * Schema of Row is defined in LcfTask.dfSchema
   *
   * Returns iterator of Rows with columns:
   * WsNs: Array of [ws, ns] which can be used to group by
   * Label: label name
   * LabelVal: label value
   * StartTime: start time of part key
   * EndTime: end time of part key
   */
  def fetchLabelValues(split: (String, String),
                       shard: Int): Iterator[Row] = {
    colStore.scanPartKeysByStartEndTimeRangeNoAsync(datasetRef, shard, split, 0,
        Long.MaxValue, 0, Long.MaxValue)
    .flatMap { pk  =>
      val rawSchemaId = RecordSchema.schemaID(pk.partKey, UnsafeUtils.arayOffset)
      val schema = schemas(rawSchemaId)
      val wsNs = schema.partKeySchema.colValues(pk.partKey, UnsafeUtils.arayOffset, Seq("_ws_", "_ns_" /*,"_metric_"*/))
      val ws = wsNs(0)
      val ns = wsNs(1)

      val pkPairs = schema.partKeySchema.toStringPairs(pk.partKey, UnsafeUtils.arayOffset)
      val filterMatches = filters.exists { filter => // at least one filter should match
        filter.forall { case (filterKey, filterValRegex) => // should match all tags in this filter
          pkPairs.exists { case (pkKey, pkVal) =>
            pkKey == filterKey && filterValRegex.matcher(pkVal).matches
          }
        }
      }
      if (filterMatches) {
        pkPairs.map { case (label, labelVal) =>
          Row(Seq(ws, ns), label, labelVal, pk.startTime, pk.endTime)
        }
      } else Nil
    }
  }

  /**
   * Returns DataFrame with columns:
   * WsNs: Array of [ws, ns]
   * Label: labelName
   * ActiveCount: Approx count of distinct label values active now (endTime == Long.MaxValue)
   * TotalCount: Approx count of distinct label values active since totalFromTs
   * Churn: TotalCount / ActiveCount (0.0 if ActiveCount is 0)
   */
  def computeChurn(spark: SparkSession, labels: RDD[Row], totalFromTs: Long): DataFrame = {
    val flattenedDf = spark.createDataFrame(labels, LcfTask.dfSchema)
    val countDf = flattenedDf
      .groupBy(WsNsColName, LabelColName)
      .agg(
        approx_count_distinct(when(col(EndTimeColName) ===
            Long.MaxValue, col(LabelValColName))).alias(ActiveCountColName),
        approx_count_distinct(when(col(EndTimeColName) >= totalFromTs, col(LabelValColName))).alias(TotalCountColName)
      )

    countDf.withColumn(
      "Churn",
      when(col(ActiveCountColName) > 0, col(TotalCountColName).cast(DoubleType) / col(ActiveCountColName))
        .otherwise(0.0)
    )
  }
}
