package filodb.downsampler.chunk

import scala.collection.mutable
import scala.concurrent.duration._

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._
import org.apache.spark.sql.types._

import filodb.coordinator.{FilodbSettings, NodeClusterActor}
import filodb.core.store.{IngestionConfig, StoreConfig}
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.chunk.ExportConstants._
import filodb.prometheus.parse.Parser

/**
 * DownsamplerSettings is always used in the context of an object so that it need not be serialized to a spark executor
 * from the spark application driver.
 */
class DownsamplerSettings(conf: Config = ConfigFactory.empty()) extends Serializable with StrictLogging {

  @transient lazy val filodbSettings = new FilodbSettings(conf)

  @transient lazy val filodbConfig = filodbSettings.allConfig.getConfig("filodb")

  @transient lazy val downsamplerConfig = {
    val conf = filodbConfig.getConfig("downsampler")
    DownsamplerContext.dsLogger.info(s"Loaded following downsampler config: ${conf.root().render()}")
    conf
  }

  @transient lazy val chunkDownsamplerIsEnabled = downsamplerConfig.getBoolean("chunk-downsampler-enabled")

  @transient lazy val cassandraConfig = filodbConfig.getConfig("cassandra")

  @transient lazy val rawDatasetName = downsamplerConfig.getString("raw-dataset-name")

  @transient lazy val rawDatasetIngestionConfig = {
    DownsamplerContext.dsLogger.info(s"Parsing dataset configs at ${filodbSettings.datasetConfPaths}")
    val ingConf = filodbSettings.streamConfigs.map { config =>
      IngestionConfig(config, NodeClusterActor.noOpSource.streamFactoryClass).get
    }.find(_.ref.toString == rawDatasetName).get
    DownsamplerContext.dsLogger.info(s"DatasetConfig for dataset $rawDatasetName was $ingConf")
    ingConf
  }

  @transient lazy val rawSchemaNames = rawDatasetIngestionConfig.downsampleConfig.schemas

  @transient lazy val downsampleResolutions = rawDatasetIngestionConfig.downsampleConfig.resolutions

  @transient lazy val downsampleTtls = rawDatasetIngestionConfig.downsampleConfig.ttls.map(_.toSeconds.toInt)

  @transient lazy val downsampledDatasetRefs =
    rawDatasetIngestionConfig.downsampleConfig.downsampleDatasetRefs(rawDatasetName)

  @transient lazy val downsampleStoreConfig = StoreConfig(downsamplerConfig.getConfig("downsample-store-config"))

  @transient lazy val ttlByResolution = downsampleResolutions.zip(downsampleTtls).toMap

  @transient lazy val batchSize = downsamplerConfig.getInt("cass-write-batch-size")

  @transient lazy val cassFetchSize = downsamplerConfig.getInt("cass-read-fetch-size")

  @transient lazy val cassWriteTimeout = downsamplerConfig.as[FiniteDuration]("cassandra-write-timeout")

  @transient lazy val widenIngestionTimeRangeBy = downsamplerConfig.as[FiniteDuration]("widen-ingestion-time-range-by")

  @transient lazy val downsampleChunkDuration = downsampleStoreConfig.flushInterval.toMillis

  @transient lazy val shouldSleepForMetricsFlush = downsamplerConfig.as[Boolean]("should-sleep-for-metrics-flush")
  @transient lazy val sleepTimeForMetricsFlush = if (downsamplerConfig.hasPath ("sleep-time-for-metrics-flush"))
    downsamplerConfig.as[FiniteDuration]("sleep-time-for-metrics-flush") else 120 seconds


  @transient lazy val allow = downsamplerConfig.as[Seq[Map[String, String]]]("allow-filters").map(_.toSeq)

  @transient lazy val block = downsamplerConfig.as[Seq[Map[String, String]]]("block-filters").map(_.toSeq)

  @transient lazy val trace = downsamplerConfig.as[Seq[Map[String, String]]]("trace-filters").map(_.toSeq)

  @transient lazy val exportIsEnabled = downsamplerConfig.getBoolean("data-export.enabled")

  @transient lazy val exportParallelism = downsamplerConfig.getInt("data-export.parallelism")

  @transient lazy val sparkSessionFactoryClass = downsamplerConfig.getString("spark-session-factory")

  @transient lazy val exportRuleKey = downsamplerConfig.as[Seq[String]]("data-export.key-labels")

  @transient lazy val exportDropLabels = downsamplerConfig.as[Seq[String]]("data-export.drop-labels")

  @transient lazy val exportKeyToConfig = {
    downsamplerConfig.as[Seq[Config]]("data-export.groups").map { group =>
      val keyFilters = group.as[Seq[String]]("key").map(Parser.parseColumnFilter)
      val tableName = group.as[String]("table")
      val tablePath = group.as[String]("table-path")
      // label-column-mapping is defined like this in conf file ["_ws_", "workspace", "_ns_", "namespace"]
      // below creates Seq[(_ws_,workspace), (_ns_,namespace)] ["_ws_", "workspace", "_ns_", "namespace"]
      // above means _ws_ label key in time series will be used to populate column workspace
      // Similarly, _ns_ label key in time series will be used to populate column namespace
      // # 3rd param NOT NULL, specifies column can be NULL or NOT NULL
      val labelColumnMapping = group.as[Seq[String]]("label-column-mapping")
        .sliding(3, 3).map(seq => (seq.apply(0), seq.apply(1), seq.apply(2))).toSeq
      // Constructs dynamic exportSchema as per ExportTableConfig.
      // final schema is a combination of columns defined in conf file plus some
      // additional standardized columns
      val tableSchema = {
        // NOTE: ArrayBuffers are sometimes used instead of Seq's because otherwise
        //   ArrayIndexOutOfBoundsExceptions occur when Spark exports a batch.
        // fields size = 9 (fixed standard number of columns in Iceberg Table)
        // + no. of dynamic cols in labelColumnMapping.length
        val fields = new mutable.ArrayBuffer[StructField](labelColumnMapping.length + 9)
        // append all dynamic columns as StringType from conf
        labelColumnMapping.foreach { pair =>
          if (pair._3 == "NOT NULL")
            fields.append(StructField(pair._2, StringType, false))
          else
            fields.append(StructField(pair._2, StringType, true))
        }
        // append all fixed columns
        fields.append(
          StructField(COL_METRIC, StringType, false),
          StructField(COL_LABELS, MapType(StringType, StringType)),
          StructField(COL_EPOCH_TIMESTAMP, LongType, false),
          StructField(COL_TIMESTAMP, TimestampType, false),
          StructField(COL_VALUE, DoubleType),
          StructField(COL_YEAR, IntegerType, false),
          StructField(COL_MONTH, IntegerType, false),
          StructField(COL_DAY, IntegerType, false),
          StructField(COL_HOUR, IntegerType, false)
        )
        StructType(fields)
      }
      val partitionByCols = group.as[Seq[String]]("partition-by-columns")
      val rules = group.as[Seq[Config]]("rules").map { rule =>
        val allowFilterGroups = rule.as[Seq[Seq[String]]]("allow-filters").map(_.map(Parser.parseColumnFilter))
        val blockFilterGroups = rule.as[Seq[Seq[String]]]("block-filters").map(_.map(Parser.parseColumnFilter))
        val dropLabels = rule.as[Seq[String]]("drop-labels")
        ExportRule(allowFilterGroups, blockFilterGroups, dropLabels)
      }
      val config = ExportTableConfig(tableName, tableSchema, tablePath, rules, labelColumnMapping, partitionByCols)

      assert(keyFilters.map(_.column).toSet.subsetOf(exportRuleKey.toSet),
        s"expected filters only on key columns; filters=$keyFilters")
      assert(keyFilters.map(_.column).distinct.size == keyFilters.size,
        s"expected at most one filter per column; filters=$keyFilters")

      keyFilters -> config
    }
  }

  @transient lazy val exportFormat = downsamplerConfig.getString("data-export.format")

  @transient lazy val exportOptions = downsamplerConfig.as[Map[String, String]]("data-export.options")

  @transient lazy val exportCatalog = downsamplerConfig.getString("data-export.catalog")

  @transient lazy val exportDatabase = downsamplerConfig.getString("data-export.database")

  @transient lazy val logAllRowErrors = downsamplerConfig.getBoolean("data-export.log-all-row-errors")

  /**
   * Two conditions should satisfy for eligibility:
   * (a) If allow list is nonEmpty partKey should match a filter in the allow list.
   * (b) It should not match any filter in block
   */
  def isEligibleForDownsample(pkPairs: Seq[(String, String)]): Boolean = {
    if (allow.nonEmpty && !allow.exists(w => w.forall(pkPairs.contains))) {
      false
    } else {
      block.forall(w => !w.forall(pkPairs.contains))
    }
  }

  def shouldTrace(pkPairs: Seq[(String, String)]): Boolean = {
    trace.exists(w => w.forall(pkPairs.contains))
  }
}
