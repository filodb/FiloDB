package filodb.core.memstore

import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.Observable
import org.jctools.maps.NonBlockingHashMapLong

import filodb.core.{DatasetRef, Perftools}
import filodb.core.Types.PartitionKey
import filodb.core.metadata.Dataset
import filodb.core.store._
import filodb.memory.format.ZeroCopyUTF8String

class TimeSeriesMemStore(config: Config, val sink: ColumnStore, val metastore: MetaStore)
                        (implicit val ec: ExecutionContext)
extends MemStore with StrictLogging {
  import collection.JavaConverters._

  type Shards = NonBlockingHashMapLong[TimeSeriesShard]
  private val datasets = new HashMap[DatasetRef, Shards]
  val numGroups = config.getInt("memstore.groups-per-shard")

  val stats = new ChunkSourceStats

  private val numParallelFlushes = config.getInt("memstore.flush-task-parallelism")

  // TODO: Change the API to return Unit Or ShardAlreadySetup, instead of throwing.  Make idempotent.
  def setup(dataset: Dataset, shard: Int): Unit = synchronized {
    val shards = datasets.getOrElseUpdate(dataset.ref, {
                   val shardMap = new NonBlockingHashMapLong[TimeSeriesShard](32, false)
                   val tags = Map("dataset" -> dataset.ref.toString)
                   Perftools.pollingGauge(s"num-partitions", 5.seconds, tags) {
                     shardMap.values.asScala.map(_.numActivePartitions).sum.toLong
                   }
                   shardMap
                 })
    if (shards contains shard) {
      throw ShardAlreadySetup(dataset.ref, shard)
    } else {
      val tsdb = new TimeSeriesShard(dataset, config, shard, sink, metastore)
      shards.put(shard, tsdb)
    }
  }

  /**
    * Retrieve shard for given dataset and shard number as an Option
    */
  private def getShard(dataset: DatasetRef, shard: Int): Option[TimeSeriesShard] =
    datasets.get(dataset).flatMap { shards => Option(shards.get(shard)) }

  /**
    * Retrieve shard for given dataset and shard number. Raises exception if
    * the shard is not setup.
    */
  private def getShardE(dataset: DatasetRef, shard: Int): TimeSeriesShard = {
    datasets.get(dataset)
            .flatMap(shards => Option(shards.get(shard)))
            .getOrElse(throw new IllegalArgumentException(s"Dataset $dataset and shard $shard have not been set up"))
  }

  def ingest(dataset: DatasetRef, shard: Int, rows: Seq[IngestRecord]): Unit =
    getShardE(dataset, shard).ingest(rows)

  def restorePartitions(dataset: Dataset, shardNum: Int)(implicit sched: Scheduler): Future[Long] = {
    val partitionKeys = sink.scanPartitionKeys(dataset, shardNum)
    val shard = getShardE(dataset.ref, shardNum)
    partitionKeys.map { p =>
      shard.addPartition(p, false)
    }.countL.runAsync.map { count =>
      logger.info(s"Restored $count time series partitions into memstore for shard $shardNum")
      count
    }
  }

  // Should only be called once per dataset/shard
  def ingestStream(dataset: DatasetRef,
                   shardNum: Int,
                   stream: Observable[Seq[IngestRecord]],
                   flushStream: Observable[FlushCommand] = FlushStream.empty)
                  (errHandler: Throwable => Unit)
                  (implicit sched: Scheduler): CancelableFuture[Unit] = {
    val shard = getShardE(dataset, shardNum)
    val combinedStream = Observable.merge(stream.map(SomeData), flushStream)
    combinedStream.map {
                    case SomeData(records) => shard.ingest(records)
                                              None
                    // The write buffers for all partitions in a group are switched here, in line with ingestion
                    // stream.  This avoids concurrency issues and ensures that buffers for a group are switched
                    // at the same offset/watermark
                    case FlushCommand(group) => shard.switchGroupBuffers(group)
                                                Some(FlushGroup(shard.shardNum, group, shard.latestOffset))
                  }.collect { case Some(flushGroup) => flushGroup }
                  .mapAsync(numParallelFlushes)(shard.createFlushTask _)
                  .foreach { x => }
                  .recover { case ex: Exception => errHandler(ex) }
  }

  // a more optimized ingest stream handler specifically for recovery
  // TODO: See if we can parallelize ingestion stream for even better throughput
  def recoverStream(dataset: DatasetRef,
                    shardNum: Int,
                    stream: Observable[Seq[IngestRecord]],
                    checkpoints: Map[Int, Long],
                    reportingInterval: Long): Observable[Long] = {
    val shard = getShardE(dataset, shardNum)
    shard.setGroupWatermarks(checkpoints)
    var targetOffset = checkpoints.values.min + reportingInterval
    stream.map(shard.ingest(_))
          .collect {
            case offset if offset > targetOffset =>
              targetOffset += reportingInterval
              offset
          }
  }

  def indexNames(dataset: DatasetRef): Iterator[(String, Int)] =
    datasets.get(dataset).map { shards =>
      shards.entrySet.iterator.asScala.flatMap { entry =>
        val shardNum = entry.getKey.toInt
        entry.getValue.indexNames.map { s => (s, shardNum) }
      }
    }.getOrElse(Iterator.empty)

  def indexValues(dataset: DatasetRef, shard: Int, indexName: String): Iterator[ZeroCopyUTF8String] =
    getShard(dataset, shard).map(_.indexValues(indexName)).getOrElse(Iterator.empty)

  def numPartitions(dataset: DatasetRef, shard: Int): Int =
    getShard(dataset, shard).map(_.numActivePartitions).getOrElse(-1)

  def scanPartitions(dataset: Dataset,
                     partMethod: PartitionScanMethod): Observable[FiloPartition] =
    datasets(dataset.ref).get(partMethod.shard).scanPartitions(partMethod)

  def numRowsIngested(dataset: DatasetRef, shard: Int): Long =
    getShard(dataset, shard).map(_.numRowsIngested).getOrElse(-1L)

  def latestOffset(dataset: DatasetRef, shard: Int): Long =
    getShard(dataset, shard).get.latestOffset

  def activeShards(dataset: DatasetRef): Seq[Int] =
    datasets.get(dataset).map(_.keySet.asScala.map(_.toInt).toSeq).getOrElse(Nil)

  def getScanSplits(dataset: DatasetRef, splitsPerNode: Int = 1): Seq[ScanSplit] =
    activeShards(dataset).map(ShardSplit)

  def reset(): Unit = {
    datasets.clear()
    sink.reset()
  }

  def truncate(dataset: DatasetRef): Unit = {
    datasets.get(dataset).foreach(_.values.asScala.foreach(_.reset()))
    sink.truncate(dataset)
  }

  // Release memory etc.
  def shutdown(): Unit = {
    datasets.values.foreach(_.values.asScala.foreach(_.shutdown()))
    reset()
  }

  override def scanPartitionKeys(dataset: Dataset, shardNum: Int): Observable[PartitionKey] = {
    scanPartitions(dataset, FilteredPartitionScan(ShardSplit(shardNum))).map(_.binPartition)
  }
}
