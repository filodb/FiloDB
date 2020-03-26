package filodb.downsampler.index

import scala.concurrent.Await

import kamon.Kamon
import monix.reactive.Observable

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.{DatasetRef, Instance}
import filodb.core.binaryrecord2.RecordBuilder
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
  @transient lazy private val totalPartkeysUpdated = Kamon.counter("total-partkeys-updated").withoutTags()

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
      val span = Kamon.spanBuilder("per-shard-index-migration-latency")
        .asChildOf(Kamon.currentSpan())
        .tag("shard", shard)
        .start
      if (fullIndexMigration) {
        DownsamplerContext.dsLogger.info("migrating complete partkey index")
        val partKeys = rawDataSource.scanPartKeys(ref = rawDatasetRef,
          shard = shard.toInt)
        count += updateDSPartkeys(partKeys, shard)
        DownsamplerContext.dsLogger.info(s"Complete PartKey index migration successful for shard=$shard count=$count")
      } else {
        for (epochHour <- fromHour until toHourExcl) {
          val partKeys = rawDataSource.getPartKeysByUpdateHour(ref = rawDatasetRef,
            shard = shard.toInt, updateHour = epochHour)
          count += updateDSPartkeys(partKeys, shard)
        }
        DownsamplerContext.dsLogger.info(s"Partial PartKey index migration successful for shard=$shard count=$count" +
          s" fromHour=$fromHour toHourExcl=$toHourExcl")
      }
      sparkForeachTasksCompleted.increment()
      totalPartkeysUpdated.increment(count)
      span.finish()
    } catch { case e: Exception =>
      DownsamplerContext.dsLogger.error(s"Exception in task count=$count " +
        s"shard=$shard fromHour=$fromHour toHourExcl=$toHourExcl fullIndexMigration=$fullIndexMigration", e)
      sparkTasksFailed.increment
      throw e
    }
  }

  def updateDSPartkeys(partKeys: Observable[PartKeyRecord], shard: Int): Int = {
    @volatile var count = 0
    val pkRecords = partKeys.flatMap(toPartKeyRecordWithHash).map{ pkey =>
      count += 1
      DownsamplerContext.dsLogger.debug(s"Migrating partition " +
        s"partKey=${schemas.part.binSchema.stringify(pkey.partKey)}" +
        s" startTime=${pkey.startTime} endTime=${pkey.endTime}")
      pkey
    }
    val updateHour = System.currentTimeMillis() / 1000 / 60 / 60
    Await.result(dsDatasource.writePartKeys(ref = dsDatasetRef, shard = shard.toInt,
      partKeys = pkRecords,
      diskTTLSeconds = dsSettings.ttlByResolution(highestDSResolution), updateHour,
      writeToPkUTTable = false), dsSettings.cassWriteTimeout)
    count
  }

  private def toPartKeyRecordWithHash(pkRecord: PartKeyRecord): Observable[PartKeyRecord] = {
    val dsPartKey = RecordBuilder.buildDownsamplePartKey(pkRecord.partKey, schemas)
    val pkr = dsPartKey.map { dpk =>
      val hash = Option(schemas.part.binSchema.partitionHash(dsPartKey, UnsafeUtils.arayOffset))
      PartKeyRecord(dpk, pkRecord.startTime, pkRecord.endTime, hash)
    }
    if (pkr.isEmpty) {
      DownsamplerContext.dsLogger.debug(s"Skipping partition without downsample schema " +
        s"partKey=${schemas.part.binSchema.stringify(pkRecord.partKey)}" +
        s" startTime=${pkRecord.startTime} endTime=${pkRecord.endTime}")
    }
    Observable.fromIterable(pkr)
  }
}
