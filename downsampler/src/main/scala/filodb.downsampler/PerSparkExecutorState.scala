package filodb.downsampler

import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.memstore.{TimeSeriesShardStats, WriteBufferPool}
import filodb.core.metadata.{Dataset, Schemas}
import filodb.memory._

object PerSparkExecutorState extends StrictLogging {

  import DownsamplerSettings._

  val readSched = Scheduler.io("cass-read-sched")
  val writeSched = Scheduler.io("cass-write-sched")
  val cassandraColStore = new CassandraColumnStore(filodbConfig, readSched, None)(writeSched)

  val kamonTags = Map( "rawDataset" -> rawDatasetName,
                       "run" -> "Downsampler",
                       "userTimeStart" -> userTimeStart.toString,
                       "userTimeEnd" -> userTimeEnd.toString)

  val schemas = Schemas.fromConfig(filodbConfig).get

  val rawSchemas = rawSchemaNames.map { s => schemas.schemas(s)}
  /**
    * Downsample Schemas
    */
  val dsSchemas = rawSchemaNames.map { s => schemas.schemas(s).downsample.get}

  /**
    * Chunk Downsamplers by Raw Schema Id
    */
  val chunkDownsamplersByRawSchemaId = rawSchemas.map { s => s.data.hash -> s.data.downsamplers }.toMap

  /**
    * Raw dataset from which we downsample data
    */
  val rawDataset = Dataset(rawDatasetName, rawSchemas.head)

  val maxMetaSize = dsSchemas.map(_.data.blockMetaSize).max

  /**
    * Datasets to which we write downsampled data. Keyed by Downsample resolution.
    */
  val downsampleDatasets = downsampleResolutions.map { res =>
    // TODO downsample dataset should have more than one schema
    res -> Dataset(s"${rawDataset.ref}_ds_${res.toMinutes}", rawDataset.schema.downsample.get)
  }.toMap

  val blockStore = new PageAlignedBlockManager(blockMemorySize,
    stats = new MemoryStats(kamonTags),
    reclaimer = new ReclaimListener {
      override def onReclaim(metadata: Long, numBytes: Int): Unit = {}
    },
    numPagesPerBlock = 50)
  val blockFactory = new BlockMemFactory(blockStore, None, maxMetaSize,
    kamonTags, false)

  val memoryManager = new NativeMemoryManager(nativeMemManagerSize, kamonTags)

  /**
    * Buffer Pool keyed by Raw schema Id
    */
  val bufferPoolByRawSchemaId = rawSchemas.map { s =>
    val pool = new WriteBufferPool(memoryManager, s.downsample.get.data, downsampleStoreConfig)
    s.data.hash -> pool
  }.toMap

  val shardStats = new TimeSeriesShardStats(rawDataset.ref, -1) // TODO fix

}
