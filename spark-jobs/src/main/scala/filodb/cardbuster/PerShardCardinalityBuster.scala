package filodb.cardbuster

import java.time.Instant
import java.time.format.DateTimeFormatter

import scala.concurrent.Await

import kamon.Kamon
import monix.execution.Scheduler
import net.ceedubs.ficus.Ficus._

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Schemas
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.chunk.DownsamplerSettings
import filodb.memory.format.UnsafeUtils

class PerShardCardinalityBuster(dsSettings: DownsamplerSettings,
                                inDownsampleTables: Boolean) extends Serializable {

  @transient lazy private val numPartKeysDeleting = Kamon.counter("num-partkeys-deleting").withoutTags()
  @transient lazy protected val readSched = Scheduler.io("cass-read-sched")
  @transient lazy protected val writeSched = Scheduler.io("cass-write-sched")
  @transient lazy private val session = DownsamplerContext.getOrCreateCassandraSession(dsSettings.cassandraConfig)
  @transient lazy private val schemas = Schemas.fromConfig(dsSettings.filodbConfig).get

  @transient lazy private val colStore =
                new CassandraColumnStore(dsSettings.filodbConfig, readSched, session, inDownsampleTables)(writeSched)

  @transient lazy private val downsampleRefsByRes = dsSettings.downsampleResolutions
                                                              .zip(dsSettings.downsampledDatasetRefs).toMap
  @transient lazy private val rawDatasetRef = DatasetRef(dsSettings.rawDatasetName)
  @transient lazy private val highestDSResolution =
                                  dsSettings.rawDatasetIngestionConfig.downsampleConfig.resolutions.last
  @transient lazy private val dsDatasetRef = downsampleRefsByRes(highestDSResolution)

  @transient lazy private val dataset = if (inDownsampleTables) dsDatasetRef else rawDatasetRef

  @transient lazy val deleteFilter = dsSettings.filodbConfig
    .as[Seq[Map[String, String]]]("cardbuster.delete-pk-filters").map(_.toSeq)
  @transient lazy val startTimeGTE = dsSettings.filodbConfig
    .as[Option[String]]("cardbuster.delete-startTimeGTE").map { str =>
    Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(str)).toEpochMilli
  }
  @transient lazy val startTimeLTE = dsSettings.filodbConfig
    .as[Option[String]]("cardbuster.delete-startTimeLTE").map { str =>
    Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(str)).toEpochMilli
  }
  @transient lazy val endTimeGTE = dsSettings.filodbConfig
    .as[Option[String]]("cardbuster.delete-endTimeGTE").map { str =>
    Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(str)).toEpochMilli
  }
  @transient lazy val endTimeLTE = dsSettings.filodbConfig
    .as[Option[String]]("cardbuster.delete-endTimeLTE").map { str =>
    Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(str)).toEpochMilli
  }

  def bustIndexRecords(shard: Int): Unit = {
    BusterContext.log.info(s"Busting cardinality in shard=$shard with " +
      s"filter=$deleteFilter " +
      s"inDownsampleTables=$inDownsampleTables " +
      s"startTimeGTE=$startTimeGTE " +
      s"startTimeLTE=$startTimeLTE " +
      s"endTimeGTE=$endTimeGTE " +
      s"endTimeLTE=$endTimeLTE "
    )
    val toDelete = colStore.scanPartKeys(dataset, shard)
      .filter { pkr =>
        val timeOk = startTimeGTE.forall(pkr.startTime >= _) &&
                          startTimeLTE.forall(pkr.startTime <= _) &&
                          endTimeGTE.forall(pkr.endTime >= _) &&
                          endTimeLTE.forall(pkr.endTime <= _)

        if (timeOk) {
          val pk = pkr.partKey
          val rawSchemaId = RecordSchema.schemaID(pk, UnsafeUtils.arayOffset)
          val schema = schemas(rawSchemaId)
          val pkPairs = schema.partKeySchema.toStringPairs(pk, UnsafeUtils.arayOffset)
          val willDelete = deleteFilter.exists(filter => filter.forall(pkPairs.contains))
          if (willDelete) {
            BusterContext.log.debug(s"Deleting part key ${schema.partKeySchema.stringify(pk)}")
            numPartKeysDeleting.increment()
          }
          willDelete
        } else {
          false
        }
      }.map(_.partKey)
    val fut = colStore.deletePartKeys(dataset, shard, toDelete)
    val numKeysDeleted = Await.result(fut, dsSettings.cassWriteTimeout)
    BusterContext.log.info(s"Deleted keys from shard shard=$shard numKeysDeleted=$numKeysDeleted")
  }
}
