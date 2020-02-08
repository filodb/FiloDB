package filodb.downsampler.index

import scala.concurrent.Await

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.execution.Scheduler

import filodb.cassandra.FiloSessionProvider
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.{DatasetRef, Instance}
import filodb.core.metadata.Schemas
import filodb.core.store.PartKeyRecord
import filodb.downsampler.DownsamplerSettings
import filodb.downsampler.DownsamplerSettings.rawDatasetIngestionConfig
import filodb.memory.format.UnsafeUtils

object DSIndexJob extends StrictLogging with Instance {

  val settings = DownsamplerSettings
  val dsJobsettings = DownsamplerSettings

  private val readSched = Scheduler.io("cass-index-read-sched")
  private val writeSched = Scheduler.io("cass-index-write-sched")

  val taskCounter = Kamon.counter("num-tasks")
  val completionCounter = Kamon.counter("num-completed-tasks")
  val failedCounter = Kamon.counter("num-failed-tasks")
  val totalKeysCounter = Kamon.counter("total-partkey-updated")

  /**
    * Datasets to which we write downsampled data. Keyed by Downsample resolution.
    */
  private[downsampler] val downsampleRefsByRes = settings.downsampleResolutions
    .zip(settings.downsampledDatasetRefs).toMap


  private[downsampler] val schemas = Schemas.fromConfig(settings.filodbConfig).get

  /**
    * Raw dataset from which we downsample data
    */
  private[downsampler] val rawDatasetRef = DatasetRef(settings.rawDatasetName)

  private val sessionProvider = dsJobsettings.sessionProvider.map { p =>
    val clazz = createClass(p).get
    val args = Seq(classOf[Config] -> dsJobsettings.cassandraConfig)
    createInstance[FiloSessionProvider](clazz, args).get
  }

  private[index] val downsampleCassandraColStore =
    new CassandraColumnStore(dsJobsettings.filodbConfig, readSched, sessionProvider, true)(writeSched)

  private[index] val rawCassandraColStore =
    new CassandraColumnStore(dsJobsettings.filodbConfig, readSched, sessionProvider, false)(writeSched)

  def updateDSPartKeyIndex(shard: Int, epochHour: Long): Unit = {
    import DSIndexJobSettings._

    taskCounter.increment

    val span = Kamon.buildSpan("timetaken-index-migration")
      .withTag("shard", shard)
      .withTag("epochHour", epochHour)
      .start
    val rawDataSource = rawCassandraColStore
    val dsDatasource = downsampleCassandraColStore
    val highestDSResolution = rawDatasetIngestionConfig.downsampleConfig.resolutions.last // data retained longest
    val dsDatasetRef = downsampleRefsByRes(highestDSResolution)
    @volatile var count = 0
    try {
      val partKeys = rawDataSource.getPartKeysByUpdateHour(ref = rawDatasetRef,
        shard = shard.toInt, updateHour = epochHour)
      val pkRecords = partKeys.map(toPartkeyRecordWithHash).map{pkey => count += 1; pkey}
      Await.result(dsDatasource.writePartKeys(ref = dsDatasetRef, shard = shard.toInt,
        partKeys = pkRecords,
        diskTTLSeconds = dsJobsettings.ttlByResolution(highestDSResolution),
        writeToPkUTTable = false), cassWriteTimeout)
      completionCounter.increment()
      totalKeysCounter.increment(count)
      logger.info(s"Number of partitionKeys written numPkeysWritten=$count shard=$shard hour=$epochHour")
    } catch {
      case e: Exception =>
        logger.error(s"Exception in task count=$count " +
          s"shard=$shard hour=$epochHour", e)
        failedCounter.increment
        throw e
    } finally {
      span.finish()
    }
  }

  def toPartkeyRecordWithHash(pkRecord: PartKeyRecord): PartKeyRecord = {
    val hash = Option(schemas.part.binSchema.partitionHash(pkRecord.partKey, UnsafeUtils.arayOffset))
    PartKeyRecord(pkRecord.partKey, pkRecord.startTime, pkRecord.endTime, hash)
  }

}