package filodb.downsampler.index

import scala.concurrent.Await

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.cassandra.FiloSessionProvider
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.{DatasetRef, Instance}
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Schemas
import filodb.core.store.PartKeyRecord
import filodb.downsampler.DownsamplerSettings
import filodb.downsampler.DownsamplerSettings.rawDatasetIngestionConfig
import filodb.downsampler.index.DSIndexJobSettings.cassWriteTimeout
import filodb.memory.format.UnsafeUtils

object DSIndexJob extends StrictLogging with Instance {

  val settings = DownsamplerSettings
  val dsJobsettings = DownsamplerSettings

  private val readSched = Scheduler.io("cass-index-read-sched")
  private val writeSched = Scheduler.io("cass-index-write-sched")

  val sparkTasksStarted = Kamon.counter("spark-tasks-started").withoutTags()
  val sparkForeachTasksCompleted = Kamon.counter("spark-foreach-tasks-completed").withoutTags()
  val sparkTasksFailed = Kamon.counter("spark-tasks-failed").withoutTags()
  val totalPartkeysUpdated = Kamon.counter("total-partkeys-updated").withoutTags()

  private val kamonTags = Map( "rawDataset" -> settings.rawDatasetName,
    "owner" -> "DSIndexJob")

  private[downsampler] val schemas = Schemas.fromConfig(settings.filodbConfig).get

  /**
    * Downsample Schemas
    */
  private val dsSchemas = settings.rawSchemaNames.map { s => schemas.schemas(s).downsample.get}


  // FIXME * 4 exists to workaround an issue where we see under-allocation for metaspan due to
  // possible mis-calculation of max block meta size.
  private val maxMetaSize = dsSchemas.map(_.data.blockMetaSize).max * 4

  /**
    * Datasets to which we write downsampled data. Keyed by Downsample resolution.
    */
  private[downsampler] val downsampleRefsByRes = settings.downsampleResolutions
    .zip(settings.downsampledDatasetRefs).toMap

  /**
    * Raw dataset from which we downsample data
    */
  private[downsampler] val rawDatasetRef = DatasetRef(settings.rawDatasetName)

  private val session = FiloSessionProvider.openSession(settings.cassandraConfig)

  private[index] val downsampleCassandraColStore =
    new CassandraColumnStore(dsJobsettings.filodbConfig, readSched, session, true)(writeSched)

  private[index] val rawCassandraColStore =
    new CassandraColumnStore(dsJobsettings.filodbConfig, readSched, session, false)(writeSched)

  val dsDatasource = downsampleCassandraColStore
  val highestDSResolution = rawDatasetIngestionConfig.downsampleConfig.resolutions.last // data retained longest
  val dsDatasetRef = downsampleRefsByRes(highestDSResolution)

  def updateDSPartKeyIndex(shard: Int, fromHour: Long, toHour: Long): Unit = {
    import DSIndexJobSettings._

    sparkTasksStarted.increment

    val span = Kamon.spanBuilder("per-shard-index-migration-latency")
      .asChildOf(Kamon.currentSpan())
      .tag("shard", shard)
      .start
    val rawDataSource = rawCassandraColStore

    @volatile var count = 0
    try {
      if (migrateRawIndex) {
        logger.info("migrating complete partkey index")
        val partKeys = rawDataSource.scanPartKeys(ref = rawDatasetRef,
          shard = shard.toInt)
        count += updateDSPartkeys(partKeys, shard)
        logger.info(s"Complete Partkey index migration successful for shard=$shard count=$count")
      } else {
        for (epochHour <- fromHour to toHour) {
          val partKeys = rawDataSource.getPartKeysByUpdateHour(ref = rawDatasetRef,
            shard = shard.toInt, updateHour = epochHour)
          count += updateDSPartkeys(partKeys, shard)
        }
        logger.info(s"Partial Partkey index migration successful for shard=$shard count=$count" +
          s" from=$fromHour to=$toHour")
      }
      sparkForeachTasksCompleted.increment()
      totalPartkeysUpdated.increment(count)
    } catch {
      case e: Exception =>
        logger.error(s"Exception in task count=$count " +
          s"shard=$shard from=$fromHour to=$toHour", e)
        sparkTasksFailed.increment
        throw e
    } finally {
      span.finish()
    }
  }

  def updateDSPartkeys(partKeys: Observable[PartKeyRecord], shard: Int): Int = {
    @volatile var count = 0
    val pkRecords = partKeys.map(toPartKeyRecordWithHash).map{pkey =>
        count += 1
        logger.debug(s"migrating partition pkstring=${schemas.part.binSchema.stringify(pkey.partKey)}" +
          s" start=${pkey.startTime} end=${pkey.endTime}")
        pkey
    }
    Await.result(dsDatasource.writePartKeys(ref = dsDatasetRef, shard = shard.toInt,
      partKeys = pkRecords,
      diskTTLSeconds = dsJobsettings.ttlByResolution(highestDSResolution),
      writeToPkUTTable = false), cassWriteTimeout)

    count
  }

  private def toPartKeyRecordWithHash(pkRecord: PartKeyRecord): PartKeyRecord = {
    val dsPartKey = RecordSchema.buildDSPartKey(pkRecord.partKey, schemas)
    val hash = Option(schemas.part.binSchema.partitionHash(dsPartKey, UnsafeUtils.arayOffset))
    PartKeyRecord(dsPartKey, pkRecord.startTime, pkRecord.endTime, hash)
  }

}
