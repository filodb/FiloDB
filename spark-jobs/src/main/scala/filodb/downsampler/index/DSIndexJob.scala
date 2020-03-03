package filodb.downsampler.index

import scala.concurrent.Await

import kamon.Kamon
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.cassandra.FiloSessionProvider
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.{DatasetRef, Instance}
import filodb.core.metadata.Schemas
import filodb.core.store.PartKeyRecord
import filodb.downsampler.Housekeeping
import filodb.downsampler.chunk.DownsamplerSettings
import filodb.memory.format.UnsafeUtils

class DSIndexJob(dsSettings: DownsamplerSettings,
                 dsJobsettings: DSIndexJobSettings) extends Instance with Serializable {

  @transient lazy private val sparkTasksStarted = Kamon.counter("spark-tasks-started").withoutTags()
  @transient lazy private val sparkForeachTasksCompleted = Kamon.counter("spark-foreach-tasks-completed")
                                                               .withoutTags()
  @transient lazy private val sparkTasksFailed = Kamon.counter("spark-tasks-failed").withoutTags()
  @transient lazy private val totalPartkeysUpdated = Kamon.counter("total-partkeys-updated").withoutTags()

  /**
    * Datasets to which we write downsampled data. Keyed by Downsample resolution.
    */
  @transient lazy private[downsampler] val downsampleRefsByRes = dsSettings.downsampleResolutions
    .zip(dsSettings.downsampledDatasetRefs).toMap


  @transient lazy private[downsampler] val schemas = Schemas.fromConfig(dsSettings.filodbConfig).get

  /**
    * Raw dataset from which we downsample data
    */
  @transient lazy private[downsampler] val rawDatasetRef = DatasetRef(dsSettings.rawDatasetName)

  @transient lazy private val session = {
    import filodb.core._
    Housekeeping.sessionMap.getOrElseUpdate(dsSettings.cassandraConfig, { conf =>
      Housekeeping.dsLogger.info(s"Creating new Cassandra session")
      FiloSessionProvider.openSession(conf)
    })
  }

  @transient lazy private[index] val downsampleCassandraColStore =
    new CassandraColumnStore(dsJobsettings.filodbConfig, Housekeeping.readSched, session, true)(Housekeeping.writeSched)

  @transient lazy private[index] val rawCassandraColStore =
    new CassandraColumnStore(dsJobsettings.filodbConfig, Housekeeping.readSched, session,
                              false)(Housekeeping.writeSched)

  @transient lazy private val dsDatasource = downsampleCassandraColStore
  // data retained longest
  @transient lazy private val highestDSResolution =
      dsSettings.rawDatasetIngestionConfig.downsampleConfig.resolutions.last
  @transient lazy private val dsDatasetRef = downsampleRefsByRes(highestDSResolution)

  def updateDSPartKeyIndex(shard: Int, fromHour: Long, toHour: Long): Unit = {

    sparkTasksStarted.increment
    val span = Kamon.spanBuilder("per-shard-index-migration-latency")
      .asChildOf(Kamon.currentSpan())
      .tag("shard", shard)
      .start
    val rawDataSource = rawCassandraColStore

    @volatile var count = 0
    try {
      if (dsJobsettings.migrateRawIndex) {
        Housekeeping.dsLogger.info("migrating complete partkey index")
        val partKeys = rawDataSource.scanPartKeys(ref = rawDatasetRef,
          shard = shard.toInt)
        count += updateDSPartkeys(partKeys, shard)
        Housekeeping.dsLogger.info(s"Complete Partkey index migration successful for shard=$shard count=$count")
      } else {
        for (epochHour <- fromHour to toHour) {
          val partKeys = rawDataSource.getPartKeysByUpdateHour(ref = rawDatasetRef,
            shard = shard.toInt, updateHour = epochHour)
          count += updateDSPartkeys(partKeys, shard)
        }
        Housekeeping.dsLogger.info(s"Partial Partkey index migration successful for shard=$shard count=$count" +
          s" from=$fromHour to=$toHour")
      }
      sparkForeachTasksCompleted.increment()
      totalPartkeysUpdated.increment(count)
    } catch { case e: Exception =>
      Housekeeping.dsLogger.error(s"Exception in task count=$count " +
        s"shard=$shard from=$fromHour to=$toHour", e)
      sparkTasksFailed.increment
      throw e
    } finally {
      span.finish()
//      rawCassandraColStore.shutdown()
//      downsampleCassandraColStore.shutdown()
    }
  }

  def updateDSPartkeys(partKeys: Observable[PartKeyRecord], shard: Int): Int = {
    @volatile var count = 0
    val pkRecords = partKeys.map(toPartkeyRecordWithHash).map{pkey =>
      count += 1
      Housekeeping.dsLogger.debug(s"migrating partition " +
        s"pkstring=${schemas.part.binSchema.stringify(pkey.partKey)}" +
        s" start=${pkey.startTime} end=${pkey.endTime}")
      pkey
    }
    Await.result(dsDatasource.writePartKeys(ref = dsDatasetRef, shard = shard.toInt,
      partKeys = pkRecords,
      diskTTLSeconds = dsSettings.ttlByResolution(highestDSResolution),
      writeToPkUTTable = false), dsSettings.cassWriteTimeout)
    count
  }

  def toPartkeyRecordWithHash(pkRecord: PartKeyRecord): PartKeyRecord = {
    val hash = Option(schemas.part.binSchema.partitionHash(pkRecord.partKey, UnsafeUtils.arayOffset))
    PartKeyRecord(pkRecord.partKey, pkRecord.startTime, pkRecord.endTime, hash)
  }
}
