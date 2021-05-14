package filodb.cardbuster

import java.time.Instant
import java.time.format.DateTimeFormatter

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

  @transient lazy protected val readSched = Scheduler.io("cass-read-sched")
  @transient lazy protected val writeSched = Scheduler.io("cass-write-sched")
  @transient lazy private val session = DownsamplerContext.getOrCreateCassandraSession(dsSettings.cassandraConfig)
  @transient lazy private val schemas = Schemas.fromConfig(dsSettings.filodbConfig).get

  @transient lazy private[cardbuster] val colStore =
                new CassandraColumnStore(dsSettings.filodbConfig, readSched, session, inDownsampleTables)(writeSched)

  @transient lazy private val downsampleRefsByRes = dsSettings.downsampleResolutions
                                                              .zip(dsSettings.downsampledDatasetRefs).toMap
  @transient lazy private val rawDatasetRef = DatasetRef(dsSettings.rawDatasetName)
  @transient lazy private val highestDSResolution =
                                  dsSettings.rawDatasetIngestionConfig.downsampleConfig.resolutions.last
  @transient lazy private val dsDatasetRef = downsampleRefsByRes(highestDSResolution)

  @transient lazy private[cardbuster] val dataset = if (inDownsampleTables) dsDatasetRef else rawDatasetRef

  @transient lazy val deleteFilter = dsSettings.filodbConfig
    .as[Seq[Map[String, String]]]("cardbuster.delete-pk-filters").map(_.mapValues(_.r.pattern).toSeq)

  @transient lazy val startTimeGTE = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(dsSettings.filodbConfig
    .as[String]("cardbuster.delete-startTimeGTE"))).toEpochMilli

  @transient lazy val startTimeLTE = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(dsSettings.filodbConfig
    .as[String]("cardbuster.delete-startTimeLTE"))).toEpochMilli

  @transient lazy val endTimeGTE = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(dsSettings.filodbConfig
    .as[String]("cardbuster.delete-endTimeGTE"))).toEpochMilli

  @transient lazy val endTimeLTE = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(dsSettings.filodbConfig
    .as[String]("cardbuster.delete-endTimeLTE"))).toEpochMilli

  def bustIndexRecords(shard: Int, split: (String, String)): Unit = {
    val numPartKeysDeleted = Kamon.counter("num-partkeys-deleted").withTag("dataset", dataset.toString)
        .withTag("shard", shard)
    val numPartKeysCouldNotDelete = Kamon.counter("num-partkeys-could-not-delete").withTag("dataset", dataset.toString)
      .withTag("shard", shard)
    require(deleteFilter.nonEmpty, "cardbuster.delete-pk-filters should be non-empty")
    BusterContext.log.info(s"Starting to bust cardinality in shard=$shard with " +
      s"filter=$deleteFilter " +
      s"inDownsampleTables=$inDownsampleTables " +
      s"startTimeGTE=$startTimeGTE " +
      s"startTimeLTE=$startTimeLTE " +
      s"endTimeGTE=$endTimeGTE " +
      s"endTimeLTE=$endTimeLTE " +
      s"split=$split"
    )
    val candidateKeys = colStore.scanPartKeysByStartEndTimeRangeNoAsync(dataset, shard,
      split, startTimeGTE, startTimeLTE,
      endTimeGTE, endTimeLTE)

    var numDeleted = 0
    var numCouldNotDelete = 0
    candidateKeys.filter { pk =>
      val rawSchemaId = RecordSchema.schemaID(pk, UnsafeUtils.arayOffset)
      val schema = schemas(rawSchemaId)
      val pkPairs = schema.partKeySchema.toStringPairs(pk, UnsafeUtils.arayOffset)
      val willDelete = deleteFilter.exists { filter => // at least one filter should match
        filter.forall { case (filterKey, filterValRegex) => // should match all tags in this filter
          pkPairs.exists { case (pkKey, pkVal) =>
            pkKey == filterKey && filterValRegex.matcher(pkVal).matches
          }
        }
      }
      if (willDelete) {
        BusterContext.log.debug(s"Deleting part key $pkPairs from shard=$shard")
      }
      willDelete
    }.foreach { pk =>
      try {
        colStore.deletePartKeyNoAsync(dataset, shard, pk)
        numPartKeysDeleted.increment()
        numDeleted += 1
      } catch { case e: Exception =>
        BusterContext.log.error(s"Could not delete a part key: ${e.getMessage}")
        numPartKeysCouldNotDelete.increment()
        numCouldNotDelete += 1
      }
      Unit
    }
    BusterContext.log.info(s"Finished deleting keys from shard shard=$shard " +
      s"numDeleted=$numDeleted numCouldNotDelete=$numCouldNotDelete")
  }
}
