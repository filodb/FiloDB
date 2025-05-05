package filodb.labelchurnfinder

import scala.collection.mutable

import com.typesafe.scalalogging.{Logger, StrictLogging}
import kamon.Kamon
import monix.execution.Scheduler
import net.ceedubs.ficus.Ficus._
import org.apache.datasketches.cpc.{CpcSketch, CpcUnion}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import filodb.cassandra.columnstore.{CassandraColumnStore, CassandraTokenRangeSplit}
import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Schemas
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.chunk.DownsamplerSettings
import filodb.downsampler.index.DSIndexJobSettings
import filodb.labelchurnfinder.LCFContext.sched
import filodb.memory.format.UnsafeUtils

object LabelChurnFinder extends App {

  val dsSettings = new DownsamplerSettings()
  val dsIndexJobSettings = new DSIndexJobSettings(dsSettings)

  val labelChurnFinder = new LabelChurnFinder(dsSettings, dsIndexJobSettings)
  val sparkConf = new SparkConf(loadDefaults = true)
  labelChurnFinder.run(sparkConf)

}

object LCFContext extends StrictLogging {
  lazy protected[labelchurnfinder] val log: Logger = logger
  lazy val sched = Scheduler.io("cass-read-sched")
}

case class ChurnSketches(active: CpcSketch, total: CpcSketch) {
  def churn(): Double = {
    val activeCard = active.getEstimate
    val totalCard = total.getEstimate
    if (activeCard == 0) 0.0 else totalCard / activeCard
  }
}

/**
 * Requires following typesafe config properties:
 *
 *
 * filodb.labelchurnfinder.pk-filters = [
 *  {
 *     _ns_ = "bulk_ns"
 *     _ws_ = "tag_value_as_regex"
 *  }
 * ]
 *
 */
class LabelChurnFinder(dsSettings: DownsamplerSettings, dsIndexJobSettings: DSIndexJobSettings) extends Serializable {
  private val logK = 10
  private val schemas = Schemas.fromConfig(dsSettings.filodbConfig).get
  private val rawDatasetRef = DatasetRef(dsSettings.rawDatasetName)


  @transient lazy private val session = DownsamplerContext.getOrCreateCassandraSession(dsSettings.cassandraConfig)
  @transient lazy private[cardbuster] val colStore =
    new CassandraColumnStore(dsSettings.filodbConfig, sched,
      session, false)(sched)
  @transient lazy val filter = dsSettings.filodbConfig
    .as[Seq[Map[String, String]]]("labelchurnfinder.pk-filters").map(_.mapValues(_.r.pattern).toSeq)

  // scalastyle:off
  def run(conf: SparkConf): SparkSession = {
    val spark = SparkSession.builder()
      .appName("FiloDB_Cardinality_Buster")
      .config(conf)
      .getOrCreate()

    LCFContext.log.info(s"This is the Label Churn Finder. Starting job.")

    val pkGroups = if (colStore.partKeysV2TableEnabled) {
      spark.sparkContext
        .makeRDD(colStore.getScanSplits(rawDatasetRef))
        .mapPartitions { split =>
          Kamon.init() // kamon init should be first thing in worker jvm
          split.flatMap(_.asInstanceOf[CassandraTokenRangeSplit].tokens)
        }.flatMap { sp =>
          Kamon.init() // kamon init should be first thing in worker jvm
          colStore.scanPartKeysByStartEndTimeRangeNoAsync(rawDatasetRef, -1 /* not used for v2 */, sp, 0,
            Long.MaxValue, 0, Long.MaxValue)
            .filter( _ => true) // TODO add filters
            .grouped(10000)
        }
    } else {
      spark.sparkContext
        .makeRDD(colStore.getScanSplits(rawDatasetRef))
        .mapPartitions { split =>
          Kamon.init() // kamon init should be first thing in worker jvm
          split.flatMap(_.asInstanceOf[CassandraTokenRangeSplit].tokens)
            .flatMap { split =>
              val numShards = dsIndexJobSettings.numShards
              for { shard <- 0 to numShards } yield (split, shard)
            }
        }.flatMap { case (split, shard) =>
          Kamon.init() // kamon init should be first thing in worker jvm
          colStore.scanPartKeysByStartEndTimeRangeNoAsync(rawDatasetRef, shard, split, 0,
              Long.MaxValue, 0, Long.MaxValue)
            .filter( _ => true) // TODO add filters
            .grouped(10000)
        }
    }

    val result = pkGroups.map { pks =>
        val sketches = mutable.HashMap.empty[Seq[String], ChurnSketches]
        Kamon.init() // kamon init should be first thing in worker jvm
        pks.foreach { pk =>
          val rawSchemaId = RecordSchema.schemaID(pk.partKey, UnsafeUtils.arayOffset)
          val schema = schemas(rawSchemaId)
          val wsNs = schema.partKeySchema.colValues(pk.partKey, UnsafeUtils.arayOffset, Seq("_ws_", "_ns_"))
          val ws = wsNs(0)
          val ns = wsNs(1)
          schema.partKeySchema.toStringPairs(pk.partKey, UnsafeUtils.arayOffset).foreach { case (pkKey, pkVal) =>
            val key = Seq(ws, ns, pkKey)
            val sketch = sketches.getOrElseUpdate(key, ChurnSketches(new CpcSketch(logK), new CpcSketch(logK)))
            val valBytes = pkVal.getBytes
            sketch.total.update(valBytes)
            if (pk.endTime == Long.MaxValue) sketch.active.update(valBytes)
          }
        }
        sketches
      }.fold(mutable.HashMap.empty[Seq[String], ChurnSketches]) { (acc, m) =>
        m.foreach({
          case (key, sketch) =>
            if (acc.contains(key)) {
              val unionActive = new CpcUnion(logK)
              unionActive.update(acc(key).active)
              unionActive.update(sketch.active)
              val unionTotal = new CpcUnion(logK)
              unionTotal.update(acc(key).total)
              unionTotal.update(sketch.total)
              acc + (key -> ChurnSketches(unionActive.getResult, unionTotal.getResult))
            } else {
              acc + (key -> sketch)
            }
        })
        acc
      }
      val sortedResult = result.toArray.sortBy(-_._2.churn()) // negative for descending order
      sortedResult.foreach { case (key, sketch) =>
          // TODO, can write this to durable store
          val labelActiveCard = sketch.active.getEstimate
          val labelTotalCard = sketch.total.getEstimate
          LCFContext.log.info(s"Label $key has estimated cardinality " +
            s"active=$labelActiveCard total=$labelTotalCard churn=${labelTotalCard / labelActiveCard}")
      }
    LCFContext.log.info(s"LabelChurnFinder completed successfully")
    spark
  }

}
