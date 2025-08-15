package filodb.downsampler.chunk

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.ForkJoinPool

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.ForkJoinTaskSupport

import kamon.Kamon
import kamon.metric.MeasurementUnit
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import filodb.coordinator.KamonShutdownHook
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore.PagedReadablePartition
import filodb.core.query.ColumnFilter
import filodb.core.store.{RawPartData, ScanSplit}
import filodb.downsampler.DownsamplerContext
import filodb.memory.format.UnsafeUtils

/**
 * Implement this trait and provide its fully-qualified name as the downsampler config:
 *     spark-session-factory = "org.fully.qualified.FactoryClass"
 */
trait SparkSessionFactory {
  def make(sparkConf: SparkConf): SparkSession
}

class DefaultSparkSessionFactory extends SparkSessionFactory {
  override def make(sparkConf: SparkConf): SparkSession = {
    SparkSession.builder()
      .appName("FiloDBDownsampler")
      .config(sparkConf)
      .getOrCreate()
  }
}

trait ChunkPersistor {
  def init(sparkConf: SparkConf): Unit
  def persist(downsampledChunks: DataFrame, batchDownsampler: BatchDownsampler): Unit
}

class DefaultChunkPersistor extends ChunkPersistor {
  override def init(sparkConf: SparkConf): Unit = ()

  override def persist(downsampledChunks: DataFrame, batchDownsampler: BatchDownsampler): Unit = {
    batchDownsampler.persistDownsampledChunks(downsampledChunks)
  }
}

trait DSPartitionReader {
  def read(spark: SparkSession, batchDownsampler: BatchDownsampler,
           ingestionTimeStart: Long, ingestionTimeEnd: Long,
           userTimeStart: Long, userTimeEndExclusive: Long): RDD[Seq[RawPartData]]
}

class DefaultDSPartitionReader extends DSPartitionReader {
  override def read(spark: SparkSession, batchDownsampler: BatchDownsampler,
                    ingestionTimeStart: Long, ingestionTimeEnd: Long,
                    userTimeStart: Long, userTimeEndExclusive: Long): RDD[Seq[RawPartData]] = {
    val splits: Seq[ScanSplit] = batchDownsampler.rawCassandraColStore.getScanSplits(batchDownsampler.rawDatasetRef)
    val settings = batchDownsampler.settings
    DownsamplerContext.dsLogger.info(s"Cassandra split size: ${splits.size}. We will have this many spark " +
      s"partitions. Tune num-token-range-splits-for-scans if parallelism is low or latency is high")
    spark.sparkContext
      .makeRDD(splits)
      .mapPartitions { splitIter: Iterator[ScanSplit] =>
        Kamon.init()
        KamonShutdownHook.registerShutdownHook()
        val rawDataSource = batchDownsampler.rawCassandraColStore
        rawDataSource.initialize(
          batchDownsampler.rawDatasetRef, -1, settings.rawDatasetIngestionConfig.resources
        )
        val batchIter = rawDataSource.getChunksByIngestionTimeRangeNoAsync(
          datasetRef = batchDownsampler.rawDatasetRef,
          splits = splitIter, ingestionTimeStart = ingestionTimeStart,
          ingestionTimeEnd = ingestionTimeEnd,
          userTimeStart = userTimeStart, endTimeExclusive = userTimeEndExclusive,
          maxChunkTime = settings.rawDatasetIngestionConfig.storeConfig.maxChunkTime.toMillis,
          batchSize = settings.batchSize,
          cassFetchSize = settings.cassFetchSize)
        batchIter
      }
  }
}
/**
  *
  * Goal: Downsample all real-time data.
  * Goal: Align chunks when this job is run in multiple DCs so that cross-dc repairs can be done.
  * Non-Goal: Downsampling of non-real time data or data with different epoch.
  *
  * Strategy is to run this spark job every 6 hours at 8am, 2pm, 8pm, 2am UTC each day.
  *
  * Run at 8am: We query data with ingestionTime from 10pm to 8am.
  *             Then query and downsample data with userTime between 12am to 6am.
  *             Downsampled chunk would have an ingestionTime of 12am.
  * Run at 2pm: We query data with ingestionTime from 4am to 2pm.
  *             Then query and downsample data with userTime between 6am to 12pm.
  *             Downsampled chunk would have an ingestionTime of 6am.
  * Run at 8pm: We query data with ingestionTime from 10am to 8pm.
  *             Then query and downsample data with userTime between 12pm to 6pm.
  *             Downsampled chunk would have an ingestionTime of 12pm.
  * Run at 2am: We query data with ingestionTime from 4pm to 2am.
  *             Then query and downsample data with userTime between 6pm to 12am.
  *             Downsampled chunk would have an ingestionTime of 6pm.
  *
  * This will cover all data with userTime 12am to 12am.
  * Since we query for a broader ingestionTime, it will include data arriving early/late by 2 hours.
  *
  * Important Note: The reason non-real-time data is not included in goals is because we
  * want chunk alignment between DCs in downsampled data to enable cross-dc repair without chunk surgery.
  * Without chunk-alignment in raw data and consistency in behavior across DCs, it would be difficult
  * to achieve chunk alignment in downsampled data. Once we solve that (deferred problem), we will
  * lift the constraint.
  */
object DownsamplerMain extends App {

  Kamon.init()  // kamon init should be first thing in driver jvm
  val settings = new DownsamplerSettings()
  val d = new Downsampler(settings)
  val sparkConf = new SparkConf(loadDefaults = true)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  d.run(sparkConf)
}

class Downsampler(settings: DownsamplerSettings) extends Serializable {

  @transient lazy val exportLatency =
    Kamon.histogram("export-latency", MeasurementUnit.time.milliseconds).withoutTags()

  @transient lazy val numRowsExported = Kamon.counter("num-rows-exported").withoutTags()

  /**
   * Exports an RDD for a specific export key.
   * (1) Create the export destination address from the key.
   * (2) Generate all rows to be exported from the argument RDD.
   * (3) Filter for only rows relevant to the export key.
   * (4) Write the filtered rows to the destination address.
   * NOTE: the export schema is required to define a column for each field of an export key.
   * E.g. if a key is defined as [foo, bar], then the result rows must contain a column
   * for each of "foo" and "bar".
   */
  private def exportForKey(rdd: RDD[Seq[PagedReadablePartition]],
                           exportKeyFilters: Seq[ColumnFilter],
                           exportTableConfig: ExportTableConfig,
                           batchExporter: BatchExporter,
                           sparkSession: SparkSession): Unit = {
    val exportStartMs = System.currentTimeMillis()
    val filteredRowRdd = rdd
      .flatMap(batchExporter.getExportRows(_, exportKeyFilters, exportTableConfig))
      .map { row =>
        numRowsExported.increment()
        row
      }

    // write filteredRowRdd to iceberg table
    batchExporter.writeDataToIcebergTable(sparkSession, settings, exportTableConfig, filteredRowRdd)

    val exportEndMs = System.currentTimeMillis()
    exportLatency.record(exportEndMs - exportStartMs)
  }

  // Gotcha!! Need separate function (Cannot be within body of a class)
  // to create a closure for spark to serialize and move to executors.
  // Otherwise, config values below were not being sent over.
  // See https://medium.com/onzo-tech/serialization-challenges-with-spark-and-scala-a2287cd51c54
  // scalastyle:off method.length
  def run(sparkConf: SparkConf): SparkSession = {
    // Chunk Persistor has to be initialized BEFORE spark session is created
    val chunkPersistor = if (settings.shouldUseChunksPersistor) {
      val persistor: ChunkPersistor = Class.forName(settings.chunksPersistor)
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[ChunkPersistor]
      persistor.init(sparkConf)
      Some(persistor)
    } else {
      None
    }

    val spark = Class.forName(settings.sparkSessionFactoryClass)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[SparkSessionFactory]
      .make(sparkConf)

    DownsamplerContext.dsLogger.info(s"Spark Job Properties: ${spark.sparkContext.getConf.toDebugString}")

    // Use the spark property spark.filodb.downsampler.user-time-override to override the
    // userTime period for which downsampling should occur.
    // Generally disabled, defaults the period that just ended prior to now.
    // Specified during reruns for downsampling old data
    val userTimeInPeriod: Long = spark.sparkContext.getConf
      .getOption("spark.filodb.downsampler.userTimeOverride") match {
      // by default assume a time in the previous downsample period
      case None => System.currentTimeMillis() - settings.downsampleChunkDuration
      // examples: 2019-10-20T12:34:56Z  or  2019-10-20T12:34:56-08:00
      case Some(str) => Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(str)).toEpochMilli()
    }

    val userTimeStart: Long = (userTimeInPeriod / settings.downsampleChunkDuration) * settings.downsampleChunkDuration
    val userTimeEndExclusive: Long = userTimeStart + settings.downsampleChunkDuration
    val ingestionTimeStart: Long = userTimeStart - settings.widenIngestionTimeRangeBy.toMillis
    val ingestionTimeEnd: Long = userTimeEndExclusive + settings.widenIngestionTimeRangeBy.toMillis

    val downsamplePeriodStr = java.time.Instant.ofEpochMilli(userTimeStart).toString

    val batchDownsampler = new BatchDownsampler(settings, userTimeStart, userTimeEndExclusive)
    val batchExporter = new BatchExporter(settings, userTimeStart, userTimeEndExclusive)

    DownsamplerContext.dsLogger.info(s"This is the Downsampling driver. Starting downsampling job " +
      s"rawDataset=${settings.rawDatasetName} for " +
      s"userTimeInPeriod=${java.time.Instant.ofEpochMilli(userTimeInPeriod)} " +
      s"ingestionTimeStart=${java.time.Instant.ofEpochMilli(ingestionTimeStart)} " +
      s"ingestionTimeEnd=${java.time.Instant.ofEpochMilli(ingestionTimeEnd)} " +
      s"userTimeStart=$downsamplePeriodStr " +
      s"userTimeEndExclusive=${java.time.Instant.ofEpochMilli(userTimeEndExclusive)}")
    DownsamplerContext.dsLogger.info(s"To rerun this job add the following spark config: " +
      s""""spark.filodb.downsampler.userTimeOverride": "${java.time.Instant.ofEpochMilli(userTimeInPeriod)}"""")

    val splits = batchDownsampler.rawCassandraColStore.getScanSplits(batchDownsampler.rawDatasetRef)
    DownsamplerContext.dsLogger.info(s"Cassandra split size: ${splits.size}. We will have this many spark " +
      s"partitions. Tune num-token-range-splits-for-scans if parallelism is low or latency is high")

    KamonShutdownHook.registerShutdownHook()
    DownsamplerContext.dsLogger.info(s"Downsample Index Reader: ${settings.dsIndexReader}")
    val dsIndexReader = Class.forName(settings.dsIndexReader)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[DSPartitionReader]

    val sourceRdd: RDD[Seq[RawPartData]] = dsIndexReader.read(
      spark, batchDownsampler,
      ingestionTimeStart, ingestionTimeEnd, userTimeStart, userTimeEndExclusive
    )

    val pagedReadablePartitionsRdd: RDD[Seq[PagedReadablePartition]] =
      sourceRdd.map { rawPartsBatch: Seq[RawPartData] =>
        Kamon.init()
        KamonShutdownHook.registerShutdownHook()
        // convert each RawPartData to a ReadablePartition
        rawPartsBatch.map { rawPart =>
          val rawSchemaId = RecordSchema.schemaID(rawPart.partitionKey, UnsafeUtils.arayOffset)
          val rawPartSchema = batchDownsampler.schemas(rawSchemaId)
          new PagedReadablePartition(rawPartSchema, shard = 0, partID = 0, partData = rawPart, minResolutionMs = 1)
        }
      }

    // exportIsEnabled - this controls whether we dump the downsampled rows to some storage which is NOT FiloDB
    // downsample cluster. This is deprecated now and needs to be removed.
    if (settings.exportIsEnabled && settings.exportKeyToConfig.nonEmpty) {
      // Used to process tasks in parallel. Allows configurable parallelism.
      val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(settings.exportParallelism))
      val exportTasks = {
        // downsample the data as the first key is exported
        val firstExportTaskWithDs = Seq(() => {
          val (filters, config) = settings.exportKeyToConfig.head
          exportForKey(pagedReadablePartitionsRdd, filters, config, batchExporter, spark)
        })
        // export all remaining keys without the downsample step
        val remainingExportTasksWithoutDs = settings.exportKeyToConfig.tail.map { case (filters, config) =>
          () => exportForKey(pagedReadablePartitionsRdd, filters, config, batchExporter, spark)
        }
        // create a parallel sequence of tasks
        (firstExportTaskWithDs ++ remainingExportTasksWithoutDs).par
      }
      // applies the configured parallelism
      exportTasks.tasksupport = taskSupport
      // export/downsample RDDs in parallel
      exportTasks.foreach(_.apply())
    }

    // chunkDownsamplerIsEnabled - this controls whether the downsample job actually downsamples anything, the flag
    // seemingly does not make sense (why would you run a downsample job if you do not downsample anything?) but
    // you can run the job to export the data, not to downsample anything at all (deprecated functionality).
    if (settings.chunkDownsamplerIsEnabled) {
      val downsampledRowsRdd: RDD[ListBuffer[Row]] = {
        // Downsample the data.
        pagedReadablePartitionsRdd.map { part =>
          // Here we do NOT save any data to C* if settings.shouldUseChunksPersistor == true, we will get
          // a list of downsampled rows that we can persist LATER
          // If, however, shouldUseChunksPersistor == false we will not only downsample but also persist the data
          // to C* using C* driver and get back ListBuffer.empty[Row]
          val rows: ListBuffer[Row] = batchDownsampler.downsampleBatch(part)
          rows
        }
      }

      // instead of saving downsampled data using BatchDownsampler, we will make a dataframe and pass it to
      // the chunk persistor that can persist the dataframe, ie BatchDownsampler does not perform both functions
      // (1) downsampleing and (2) persisting. The function of persisiting the data is delegated to ChunkPersitor
      if (settings.shouldUseChunksPersistor) {
        DownsamplerContext.dsLogger.info(s"Using Chunk Persistor ${settings.chunksPersistor}")
        val persistor = chunkPersistor.get

        val chunkRows: RDD[Row] = downsampledRowsRdd.flatMap(x => x)
        val schema = StructType(Seq(
          StructField("res", StringType, true),
          StructField("partition", BinaryType, true),
          StructField("chunkid", LongType, true),
          StructField("info", BinaryType, true),
          StructField("chunks", ArrayType(BinaryType), true),
          StructField("ingestion_time", LongType, true),
          StructField("start_time", LongType, true),
          StructField("index_info", BinaryType, true)
        ))
        val downsampledDf = spark.createDataFrame(chunkRows, schema)
        val cachedDownsampledDf = downsampledDf.cache()
        val rows = cachedDownsampledDf.count()
        DownsamplerContext.dsLogger.info(s"Downsampled rows/time series: $rows")

        persistor.persist(cachedDownsampledDf, batchDownsampler)
      } else {
        downsampledRowsRdd.foreach(_ => {})
      }
    }

    DownsamplerContext.dsLogger.info(s"Chunk Downsampling Driver completed successfully for downsample period " +
      s"$downsamplePeriodStr")
    val jobCompleted = Kamon.counter("chunk-migration-completed")
      .withTag("downsamplePeriod", downsamplePeriodStr)
    val jobCompletedNoTags = Kamon.counter("chunk-migration-completed-success").withoutTags()
    jobCompleted.increment()
    jobCompletedNoTags.increment()
    val downsampleHourStartGauge = Kamon.gauge("chunk-downsampler-period-start-hour")
      .withTag("downsamplePeriod", downsamplePeriodStr)
    downsampleHourStartGauge.update(userTimeStart / 1000 / 60 / 60)
    if (settings.shouldSleepForMetricsFlush)
      Thread.sleep(62000) // quick & dirty hack to ensure that the completed metric gets published
    spark
  }

}
