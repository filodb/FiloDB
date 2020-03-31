package filodb.cardbuster

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
import filodb.downsampler.index.DSIndexJobSettings
import filodb.memory.format.UnsafeUtils

class PerShardCardinalityBuster(dsSettings: DownsamplerSettings,
                                dsIndexJobSettings: DSIndexJobSettings,
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

  def filterFunc(pkPairs: Seq[(String, String)]): Boolean = {
    deleteFilter.exists(filter => filter.forall(pkPairs.contains))
  }

  def bustIndexRecords(shard: Int): Unit = {
    val toDelete = colStore.scanPartKeys(dataset, shard).map(_.partKey)
      .filter { pk =>
        val rawSchemaId = RecordSchema.schemaID(pk, UnsafeUtils.arayOffset)
        val schema = schemas(rawSchemaId)
        val pkPairs = schema.partKeySchema.toStringPairs(pk, UnsafeUtils.arayOffset)
        val willDelete = filterFunc(pkPairs)
        if (willDelete) {
          BusterContext.log.debug(s"Deleting part key ${schema.partKeySchema.stringify(pk)}")
          numPartKeysDeleting.increment()
        }
        willDelete
      }
    val fut = colStore.deletePartKeys(dataset, shard, toDelete)
    val numKeysDeleted = Await.result(fut, dsSettings.cassWriteTimeout)
    BusterContext.log.info(s"Deleted keys from shard shard=$shard numKeysDeleted=$numKeysDeleted")
  }
}
