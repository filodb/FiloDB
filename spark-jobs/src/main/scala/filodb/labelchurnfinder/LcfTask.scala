package filodb.labelchurnfinder

import scala.collection.mutable
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._
import org.apache.datasketches.cpc.CpcSketch
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Schemas
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.chunk.DownsamplerSettings
import filodb.labelchurnfinder.LCFContext.sched
import filodb.memory.format.UnsafeUtils

class LcfTask(dsSettings: DownsamplerSettings) extends Serializable with StrictLogging {

  @transient lazy private[labelchurnfinder] val logK = 10
  @transient lazy private val schemas = Schemas.fromConfig(dsSettings.filodbConfig).get
  @transient lazy private[labelchurnfinder] val rawDatasetRef = DatasetRef(dsSettings.rawDatasetName)

  @transient lazy private val session = DownsamplerContext.getOrCreateCassandraSession(dsSettings.cassandraConfig)
  @transient lazy private[labelchurnfinder] val colStore =
    new CassandraColumnStore(dsSettings.filodbConfig, sched, session, false)(sched)
  @transient lazy val filter = dsSettings.filodbConfig
    .as[Seq[Map[String, String]]]("labelchurnfinder.pk-filters").map(_.mapValues(_.r.pattern).toSeq)

  @transient lazy val numShards = dsSettings.filodbSettings.streamConfigs
    .find(_.getString("dataset") == rawDatasetRef.dataset)
    .getOrElse(ConfigFactory.empty())
    .as[Option[Int]]("num-shards").get

  def computeLabelChurn(split: (String, String),
                        shard: Int): mutable.HashMap[Seq[String], ChurnSketches] = {
    colStore.scanPartKeysByStartEndTimeRangeNoAsync(rawDatasetRef, shard, split, 0, Long.MaxValue, 0, Long.MaxValue)
    .filter(_ => true) // TODO add filters
    .foldLeft(mutable.HashMap.empty[Seq[String], ChurnSketches]) { case (acc, pk) =>
      val rawSchemaId = RecordSchema.schemaID(pk.partKey, UnsafeUtils.arayOffset)
      val schema = schemas(rawSchemaId)
      val wsNs = schema.partKeySchema.colValues(pk.partKey, UnsafeUtils.arayOffset, Seq("_ws_", "_ns_", "_metric_"))
      val ws = wsNs(0)
      val ns = wsNs(1)
      val metric = wsNs(2)
      schema.partKeySchema.toStringPairs(pk.partKey, UnsafeUtils.arayOffset).foreach { case (pkKey, pkVal) =>
        val key = Seq(ws, ns, pkKey)
        val sketch = acc.getOrElseUpdate(key, ChurnSketches(new CpcSketch(logK), new CpcSketch(logK)))
        val valBytes = pkVal.getBytes
        sketch.total.update(valBytes)
        if (pk.endTime == Long.MaxValue) sketch.active.update(valBytes)
      }
      acc
    }
  }
}
