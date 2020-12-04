package filodb.downsampler.index

import scala.concurrent.Await

import kamon.Kamon
import kamon.metric.MeasurementUnit
import monix.reactive.Observable

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.{DatasetRef, Instance}
import filodb.core.binaryrecord2.{RecordBuilder, RecordSchema}
import filodb.core.metadata.Schemas
import filodb.core.store.PartKeyRecord
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.chunk.DownsamplerSettings
import filodb.memory.format.UnsafeUtils

class DSIndexJob(dsSettings: DownsamplerSettings,
                 dsJobsettings: DSIndexJobSettings) extends Instance with Serializable {

  @transient lazy private val sparkTasksStarted = Kamon.counter("spark-tasks-started").withoutTags()
  @transient lazy private val sparkForeachTasksCompleted = Kamon.counter("spark-foreach-tasks-completed")
                                                               .withoutTags()
  @transient lazy private val sparkTasksFailed = Kamon.counter("spark-tasks-failed").withoutTags()
  @transient lazy private val numPartKeysNoDownsampleSchema = Kamon.counter("num-partkeys-no-downsample").withoutTags()
  @transient lazy private val numPartKeysMigrated = Kamon.counter("num-partkeys-migrated").withoutTags()
  @transient lazy private val numPartKeysBlocked = Kamon.counter("num-partkeys-blocked").withoutTags()
  @transient lazy val perShardIndexMigrationLatency = Kamon.histogram("per-shard-index-migration-latency",
    MeasurementUnit.time.milliseconds).withoutTags()

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
    sparkTasksStarted.increment
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
      sparkTasksFailed.increment
      throw e
    }
  }

  def migrateWithDownsamplePartKeys(partKeys: Observable[PartKeyRecord], shard: Int): Int = {
    @volatile var count = 0
    val pkRecords = partKeys.filter { pk =>
      val rawSchemaId = RecordSchema.schemaID(pk.partKey, UnsafeUtils.arayOffset)
      val schema = schemas(rawSchemaId)
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
    }.map(toDownsamplePkrWithHash)
    val updateHour = System.currentTimeMillis() / 1000 / 60 / 60
    Await.result(dsDatasource.writePartKeys(ref = dsDatasetRef, shard = shard.toInt,
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
    val hash = Option(schemas.part.binSchema.partitionHash(dsPartKey, UnsafeUtils.arayOffset))
    PartKeyRecord(dsPartKey.get, pkRecord.startTime, pkRecord.endTime, hash)
  }
}
