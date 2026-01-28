package filodb.cardbuster

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import scala.concurrent.Await

import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.atomic.AtomicInt
import monix.reactive.Observable
import net.ceedubs.ficus.Ficus._

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.metadata.Schemas
import filodb.core.metrics.FilodbMetrics
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.chunk.DownsamplerSettings
import filodb.memory.format.UnsafeUtils

object BusterSchedulers {
  lazy val readSched = Scheduler.io("cass-read-sched")
  lazy val writeSched = Scheduler.io("cass-write-sched")
  lazy val computeSched = Scheduler.computation(name = "buster-compute")
}

class PerShardCardinalityBuster(dsSettings: DownsamplerSettings,
                                inDownsampleTables: Boolean) extends Serializable {

  @transient lazy private val session = DownsamplerContext.getOrCreateCassandraSession(dsSettings.cassandraConfig)
  @transient lazy private val schemas = Schemas.fromConfig(dsSettings.filodbConfig).get

  @transient lazy private[cardbuster] val colStore =
                new CassandraColumnStore(dsSettings.filodbConfig, BusterSchedulers.readSched,
                  session, inDownsampleTables)(BusterSchedulers.writeSched)

  @transient lazy private val downsampleRefsByRes = dsSettings.downsampleResolutions
                                                              .zip(dsSettings.downsampledDatasetRefs).toMap
  @transient lazy private val rawDatasetRef = DatasetRef(dsSettings.rawDatasetName)
  @transient lazy private val highestDSResolution =
                                  dsSettings.rawDatasetIngestionConfig.downsampleConfig.resolutions.last
  @transient lazy private val dsDatasetRef = downsampleRefsByRes(highestDSResolution)

  @transient lazy private[cardbuster] val dataset = if (inDownsampleTables) dsDatasetRef else rawDatasetRef

  @transient lazy val numParallelDeletesPerSparkThread = dsSettings.filodbConfig
                      .as[Option[Int]]("cardbuster.cass-delete-parallelism-per-spark-thread")

  @transient lazy val deleteFilter = dsSettings.filodbConfig
    .as[Seq[Map[String, String]]]("cardbuster.delete-pk-filters").map(_.map { case (k, v) => k -> v.r.pattern }.toSeq)

  @transient lazy val startTimeGTE = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(dsSettings.filodbConfig
    .as[String]("cardbuster.delete-startTimeGTE"))).toEpochMilli

  @transient lazy val startTimeLTE = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(dsSettings.filodbConfig
    .as[String]("cardbuster.delete-startTimeLTE"))).toEpochMilli

  @transient lazy val endTimeGTE = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(dsSettings.filodbConfig
    .as[String]("cardbuster.delete-endTimeGTE"))).toEpochMilli

  @transient lazy val endTimeLTE = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(dsSettings.filodbConfig
    .as[String]("cardbuster.delete-endTimeLTE"))).toEpochMilli

  // scalastyle:off method.length
  def bustIndexRecords(shard: Int, split: (String, String), isSimulation: Boolean): Int = {
    val numPartKeysDeleted = FilodbMetrics.counter("num-partkeys-deleted", Map("dataset" -> dataset.toString,
        "shard" -> shard.toString, "simulation" -> isSimulation.toString))
    val numPartKeysCouldNotDelete = FilodbMetrics.counter("num-partkeys-could-not-delete",
                      Map("dataset" -> dataset.toString,
      "shard" -> shard.toString, "simulation" -> isSimulation.toString))
    val cassDeleteLatency = FilodbMetrics.timeHistogram("pk-delete-latency", TimeUnit.NANOSECONDS,
      Map("dataset" -> dataset.toString, "shard" -> shard.toString, "simulation" -> isSimulation.toString))
    require(deleteFilter.nonEmpty, "cardbuster.delete-pk-filters should be non-empty")
    BusterContext.log.info(s"Starting to bust cardinality in shard=$shard with isSimulation=$isSimulation " +
      s"filter=$deleteFilter inDownsampleTables=$inDownsampleTables startTimeGTE=$startTimeGTE " +
      s"startTimeLTE=$startTimeLTE endTimeGTE=$endTimeGTE  endTimeLTE=$endTimeLTE split=$split")
    val candidateKeys = colStore.scanPartKeysByStartEndTimeRangeNoAsync(dataset, shard,
      split, startTimeGTE, startTimeLTE, endTimeGTE, endTimeLTE)
    val numDeleted = AtomicInt(0)
    val numCouldNotDelete = AtomicInt(0)
    val numCandidateKeys = AtomicInt(0)
    val keysToDelete = candidateKeys.filter { pk =>
      try {
        val rawSchemaId = RecordSchema.schemaID(pk.partKey, UnsafeUtils.arayOffset)
        val schema = schemas(rawSchemaId)
        val pkPairs = schema.partKeySchema.toStringPairs(pk.partKey, UnsafeUtils.arayOffset)
        val willDelete = deleteFilter.exists { filter => // at least one filter should match
          filter.forall { case (filterKey, filterValRegex) => // should match all tags in this filter
            pkPairs.exists { case (pkKey, pkVal) =>
              pkKey == filterKey && filterValRegex.matcher(pkVal).matches
            }
          }
        }
        if (willDelete) {
          BusterContext.log.debug(s"Deleting part key $pkPairs from shard=$shard startTime=${pk.startTime} " +
            s"endTime=${pk.endTime} split=$split isSimulation=$isSimulation")
        }
        numCandidateKeys += 1
        willDelete
      } catch {
        case e : Exception =>
          BusterContext.log.warn(s"skip busting pk=$pk because of exception $e")
          false
      }
    }
    val fut = Observable.fromIteratorUnsafe(keysToDelete)
                        .mapParallelUnordered(numParallelDeletesPerSparkThread.getOrElse(1)) { pk =>
      Task.eval {
        try {
          if (!isSimulation) {
            val startNs = System.nanoTime()
            try {
              colStore.deletePartKeyNoAsync(dataset, pk.shard, pk.partKey)
            } finally {
              cassDeleteLatency.record(System.nanoTime() - startNs)
            }
          }
          numPartKeysDeleted.increment()
          numDeleted += 1
        } catch {
          case e: Exception =>
            BusterContext.log.error(s"Could not delete a part key: ${e.getMessage}")
            numPartKeysCouldNotDelete.increment()
            numCouldNotDelete += 1
        }
        Unit
      }
    }.completedL.runToFuture(BusterSchedulers.computeSched)
    import scala.concurrent.duration._
    Await.result(fut, 1.day)
    BusterContext.log.info(s"Finished deleting keys from a shard split=$split in shard=$shard " +
      s"numCandidateKeys=${numCandidateKeys.get()} numDeleted=${numDeleted.get()} " +
      s"numCouldNotDelete=${numCouldNotDelete.get()} isSimulation=$isSimulation")
    numDeleted.get()
  }
}
