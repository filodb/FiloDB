package filodb.downsampler.index

import scala.concurrent.Await

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.execution.{Scheduler, UncaughtExceptionReporter}

import filodb.cassandra.FiloSessionProvider
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.store.PartKeyRecord
import filodb.downsampler.BatchDownsampler._
import filodb.downsampler.DownsamplerSettings
import filodb.downsampler.DownsamplerSettings.rawDatasetIngestionConfig
import filodb.memory.format.UnsafeUtils

object DSIndexJob extends StrictLogging {

  val dsJobsettings = DownsamplerSettings

  private val kamonTags = Map( "rawDataset" -> dsJobsettings.rawDatasetName,
    "owner" -> "BatchIndexUpdater")

  private val readSched = Scheduler.io("cass-index-read-sched")
  private val writeSched = Scheduler.io("cass-index-write-sched")

  /**
    * Datasets to which we write downsampled data. Keyed by Downsample resolution.
    */
  private[downsampler] val downsampleRefsByRes = settings.downsampleResolutions
    .zip(settings.downsampledDatasetRefs).toMap

  private val sessionProvider = dsJobsettings.sessionProvider.map { p =>
    val clazz = createClass(p).get
    val args = Seq(classOf[Config] -> dsJobsettings.cassandraConfig)
    createInstance[FiloSessionProvider](clazz, args).get
  }

  private[index] val downsampleCassandraColStore =
    new CassandraColumnStore(dsJobsettings.filodbConfig, readSched, sessionProvider, true)(writeSched)

  private[index] val rawCassandraColStore =
    new CassandraColumnStore(dsJobsettings.filodbConfig, readSched, sessionProvider, false)(writeSched)

  implicit lazy val globalImplicitScheduler = Scheduler.computation(
    parallelism = 15,
    name = "global-implicit",
    reporter = UncaughtExceptionReporter(logger.error("Uncaught Exception in GlobalScheduler", _)))

  def updateDSPartKeyIndex(shard: Int): Unit = {
    import DSIndexJobSettings._

    val rawDataSource = rawCassandraColStore
    val dsDtasource = downsampleCassandraColStore
    val highestDSResolution = rawDatasetIngestionConfig.downsampleConfig.resolutions.last // data retained longest
    val dsDatasetRef = downsampleRefsByRes(highestDSResolution)
    val partKeys = rawDataSource.getPartKeysByUpdateHour(ref = rawDatasetRef,
      shard = shard.toInt, updateHour = hour())
    val partKeyIter = partKeys.map(toPartkeyRecordWithHash)
    Await.result(dsDtasource.writePartKeys(ref = dsDatasetRef, shard = shard.toInt,
      partKeys = partKeyIter,
      diskTTLSeconds = dsJobsettings.ttlByResolution(highestDSResolution),
      writeToPkUTTable = false), batchTime)
  }

  def toPartkeyRecordWithHash(pkRecord: PartKeyRecord): PartKeyRecord = {
    val hash = Option(schemas.part.binSchema.partitionHash(pkRecord.partKey, UnsafeUtils.arayOffset))
    PartKeyRecord(pkRecord.partKey, pkRecord.startTime, pkRecord.endTime, hash)
  }

  private def hour(millis: Long = System.currentTimeMillis()) = millis / 1000 / 60 / 60
}
