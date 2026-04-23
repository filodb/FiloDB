package filodb.downsampler.index

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import kamon.Kamon
import monix.reactive.Observable

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.{DatasetRef, Instance}
import filodb.core.binaryrecord2.{RecordBuilder, RecordSchema}
import filodb.core.metadata.Schemas
import filodb.core.metrics.FilodbMetrics
import filodb.core.store.PartKeyRecord
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.chunk.DownsamplerSettings
import filodb.memory.format.UnsafeUtils

class DSIndexJob(dsSettings: DownsamplerSettings,
                 dsJobsettings: DSIndexJobSettings) extends Instance with Serializable {

  @transient lazy private val sparkTasksStarted = FilodbMetrics.counter("spark-tasks-started")
  @transient lazy private val sparkForeachTasksCompleted = FilodbMetrics.counter("spark-foreach-tasks-completed")
  @transient lazy private val sparkTasksFailed = FilodbMetrics.counter("spark-tasks-failed")
  @transient lazy private val numPartKeysUnknownSchema = FilodbMetrics.counter("num-partkeys-unknown-schema")
  @transient lazy private val numPartKeysNoDownsampleSchema = FilodbMetrics.counter("num-partkeys-no-downsample")
  @transient lazy private val numPartKeysMigrated = FilodbMetrics.counter("num-partkeys-migrated")
  @transient lazy private val numPartKeysBlocked = FilodbMetrics.counter("num-partkeys-blocked")
  @transient lazy val perShardIndexMigrationLatency = FilodbMetrics.timeHistogram("per-shard-index-migration-latency",
                                                                                  TimeUnit.MILLISECONDS)

  @transient lazy private[downsampler] val schemas = Schemas.fromConfig(dsSettings.filodbConfig).get

  /**
    * Datasets to which we write downsampled data. Keyed by Downsample resolution.
    */
  @transient lazy private[downsampler] val downsampleRefsByRes = dsSettings.downsampleResolutions
    .zip(dsSettings.downsampledDatasetRefs).toMap

  /**
    * Raw dataset from which we downsample data
    */
  @transient lazy private[downsampler] val rawDatasetRef = DatasetRef(dsSettings.rawDatasetName)

  @transient lazy private val session = DownsamplerContext.getOrCreateCassandraSession(dsSettings.cassandraConfig)

  @transient lazy private[index] val downsampleCassandraColStore =
    new CassandraColumnStore(dsJobsettings.filodbConfig, DownsamplerContext.readSched,
                             session, true)(DownsamplerContext.writeSched)

  @transient lazy private[index] val rawCassandraColStore =
    new CassandraColumnStore(dsJobsettings.filodbConfig, DownsamplerContext.readSched, session,
                              false)(DownsamplerContext.writeSched)

  @transient lazy private val dsDatasource = downsampleCassandraColStore
  // data retained longest
  @transient lazy private val highestDSResolution =
      dsSettings.rawDatasetIngestionConfig.downsampleConfig.resolutions.last
  @transient lazy private val dsDatasetRef = downsampleRefsByRes(highestDSResolution)

  def updateDSPartKeyIndex(shard: Int, fromHour: Long, toHourExcl: Long, fullIndexMigration: Boolean): Unit = {
    sparkTasksStarted.increment()
    val rawDataSource = rawCassandraColStore
    @volatile var count = 0
    try {
      val start = System.currentTimeMillis()
      if (fullIndexMigration) {
        DownsamplerContext.dsLogger.info(s"Starting Full PartKey Migration for shard=$shard")
        val partKeys = rawDataSource.scanPartKeys(ref = rawDatasetRef,
          shard = shard.toInt)
        count += migrateWithDownsamplePartKeys(partKeys, shard)
        DownsamplerContext.dsLogger.info(s"Successfully Completed Full PartKey Migration for shard=$shard count=$count")
      } else {
        DownsamplerContext.dsLogger.info(s"Starting Partial PartKey Migration for shard=$shard")
        for (epochHour <- fromHour until toHourExcl) {
          val partKeys = rawDataSource.getPartKeysByUpdateHour(ref = rawDatasetRef,
            shard = shard.toInt, updateHour = epochHour)
          count += migrateWithDownsamplePartKeys(partKeys, shard)
        }
        DownsamplerContext.dsLogger.info(s"Successfully Completed Partial PartKey Migration for shard=$shard " +
          s"count=$count fromHour=$fromHour toHourExcl=$toHourExcl")
      }
      sparkForeachTasksCompleted.increment()
      perShardIndexMigrationLatency.record(System.currentTimeMillis() - start)
    } catch { case e: Exception =>
      DownsamplerContext.dsLogger.error(s"Exception in task count=$count " +
        s"shard=$shard fromHour=$fromHour toHourExcl=$toHourExcl fullIndexMigration=$fullIndexMigration", e)
      sparkTasksFailed.increment()
      throw e
    }

    // quick & dirty hack to ensure that the completed metric gets published
    if (dsSettings.shouldSleepForMetricsFlush) {
      Thread.sleep(62000) // quick & dirty hack to ensure that the completed metric gets published
      Await.result(Kamon.stopModules(), 62.seconds)
    }

  }

  def migrateWithDownsamplePartKeys(partKeys: Observable[PartKeyRecord], shard: Int): Int = {
    @volatile var count = 0
    val rawDataSource = rawCassandraColStore
    rawDataSource.initialize(rawDatasetRef, -1, dsSettings.rawDatasetIngestionConfig.resources)
    downsampleCassandraColStore.initialize(dsDatasetRef, -1, dsSettings.rawDatasetIngestionConfig.resources)
    val pkRecords = partKeys.filter { pk =>
      val rawSchemaId = RecordSchema.schemaID(pk.partKey, UnsafeUtils.arayOffset)
      val schema = schemas(rawSchemaId)
      if (schema == Schemas.UnknownSchema) {
        DownsamplerContext.dsLogger.warn(s"Partition with unknownSchemaId=$rawSchemaId " +
          s"startTime=${pk.startTime} endTime=${pk.endTime} shard=$shard")
        numPartKeysUnknownSchema.increment()
        false
      } else {
        val pkPairs = schema.partKeySchema.toStringPairs(pk.partKey, UnsafeUtils.arayOffset)
        val blocked = !dsSettings.isEligibleForDownsample(pkPairs)
        val hasDownsampleSchema = schema.downsample.isDefined
        if (blocked) numPartKeysBlocked.increment()
        if (!hasDownsampleSchema) numPartKeysNoDownsampleSchema.increment()
        DownsamplerContext.dsLogger.debug(s"Migrating partition partKey=$pkPairs schema=${schema.name} " +
          s"startTime=${pk.startTime} endTime=${pk.endTime} blocked=$blocked shard=$shard " +
          s"hasDownsampleSchema=$hasDownsampleSchema")
        val eligible = hasDownsampleSchema && !blocked
        if (eligible) count += 1
        eligible
      }
    }.map(pkr => rawDataSource.getPartKeyRecordOrDefault(ref = rawDatasetRef, shard = shard,
        pkr = pkr)) // Merge with persisted (if exists) partKey.
      .map(toDownsamplePkrWithHash)
    val updateHour = System.currentTimeMillis() / 1000 / 60 / 60
    Await.result(dsDatasource.writePartKeys(ref = dsDatasetRef, shard = shard,
      partKeys = pkRecords,
      diskTTLSeconds = dsSettings.ttlByResolution(highestDSResolution), updateHour,
      writeToPkUTTable = false), dsSettings.cassWriteTimeout)
    numPartKeysMigrated.increment(count)
    count
  }

  /**
    * Builds a new PartKeyRecord with downsample schema.
    * This method will throw an exception if schema of part key does not have downsample schema
    */
  private def toDownsamplePkrWithHash(pkRecord: PartKeyRecord): PartKeyRecord = {
    val dsPartKey = RecordBuilder.buildDownsamplePartKey(pkRecord.partKey, schemas)
    PartKeyRecord(dsPartKey.get, pkRecord.startTime, pkRecord.endTime, pkRecord.shard)
  }
}
