package filodb.downsampler

import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.DatasetRef
import filodb.core.downsample.ChunkDownsampler
import filodb.core.memstore.{TimeSeriesShardStats, WriteBufferPool}
import filodb.core.metadata.Schemas
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
  val chunkDownsamplersByRawSchemaId = debox.Map.empty[Int, scala.Seq[ChunkDownsampler]]
  rawSchemas.foreach { s => chunkDownsamplersByRawSchemaId += s.schemaHash -> s.data.downsamplers }

  /**
    * Raw dataset from which we downsample data
    */
  val rawDatasetRef = DatasetRef(rawDatasetName)

  val maxMetaSize = dsSchemas.map(_.data.blockMetaSize).max

  /**
    * Datasets to which we write downsampled data. Keyed by Downsample resolution.
    */
  val downsampleDatasetRefs = downsampleResolutions.map { res =>
    res -> DatasetRef(s"${rawDatasetRef}_ds_${res.toMinutes}")
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
  val bufferPoolByRawSchemaId = debox.Map.empty[Int, WriteBufferPool]
  rawSchemas.foreach { s =>
    val pool = new WriteBufferPool(memoryManager, s.downsample.get.data, downsampleStoreConfig)
    bufferPoolByRawSchemaId += s.schemaHash -> pool
  }

  val shardStats = new TimeSeriesShardStats(rawDatasetRef, -1) // TODO fix

}
