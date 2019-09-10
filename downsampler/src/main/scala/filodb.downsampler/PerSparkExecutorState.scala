package filodb.downsampler

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.ErrorResponse
import filodb.core.memstore.{PagedReadablePartition, TimeSeriesPartition, TimeSeriesShardStats, WriteBufferPool}
import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.store.ChunkSet
import filodb.memory.{BlockMemFactory, MemoryStats, NativeMemoryManager, PageAlignedBlockManager, ReclaimListener}

object PerSparkExecutorState extends StrictLogging {

  import DownsamplerSettings._

  val readSched = Scheduler.io("cass-read-sched")
  val writeSched = Scheduler.io("cass-write-sched")
  val cassandraColStore = new CassandraColumnStore(filodbConfig, readSched, None)(writeSched)

  // TODO populate tags
  val kamonTags = Map("run" -> "TODO", "downsample" -> "TODO")

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

  // Batch Items
  val downsampledChunksToPersist = scala.collection.mutable.Map[FiniteDuration, Iterator[ChunkSet]]()
  downsampleResolutions.foreach { res =>
    downsampledChunksToPersist(res) = Iterator.empty
  }
  val rawPartsToFree = ArrayBuffer[PagedReadablePartition]()
  val downsampledPartsPartsToFree = ArrayBuffer[TimeSeriesPartition]()
  val shardStats = new TimeSeriesShardStats(rawDataset.ref, -1) // TODO fix

  /**
    * Buffer Pool keyed by Raw schema Id
    */
  val bufferPoolsByRawSchemaId = rawSchemas.map { s =>
    val pools = downsampleResolutions.map { r =>
      r -> new WriteBufferPool(memoryManager, s.downsample.get.data, downsampleStoreConfig)
    }.toMap
    s.data.hash -> pools
  }.toMap

  def persistChunks(): Unit = {
    logger.info(s"Persisting batch of chunks from ${rawPartsToFree.size} partitions")
    // write all chunks to cassandra
    val writeFut = downsampledChunksToPersist.map { case (res, chunks) =>
      PerSparkExecutorState.cassandraColStore.write(downsampleDatasets(res).ref,
        Observable.fromIterator(chunks), ttlByResolution(res))
    }

    writeFut.foreach { fut =>
      val response = Await.result(fut, cassWriteTimeout)
      logger.debug(s"Got message $response for cassandra write call")
      if (response.isInstanceOf[ErrorResponse])
        throw new IllegalStateException(s"Got response $response when writing to Cassandra")
    }

    // free partitions
    rawPartsToFree.foreach(_.free())
    rawPartsToFree.clear()
    downsampledPartsPartsToFree.foreach(_.shutdown())
    downsampledPartsPartsToFree.clear()

    // reclaim all blocks
    blockFactory.markUsedBlocksReclaimable()
  }

}
