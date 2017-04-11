package filodb.core.memstore

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import monix.reactive.Observable
import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}

import filodb.core.DatasetRef
import filodb.core.metadata.{Column, RichProjection}
import filodb.core.query.{ChunkSetReader, PartitionChunkIndex}
import filodb.core.store._
import filodb.core.Types.{PartitionKey, ChunkID}

final case class DatasetAlreadySetup(dataset: DatasetRef) extends Exception(s"Dataset $dataset already setup")

class TimeSeriesMemStore(config: Config)(implicit val ec: ExecutionContext) extends MemStore
with StrictLogging {
  import ChunkSetReader._

  private val datasets = new HashMap[DatasetRef, TimeSeriesDataset]

  def setup(projection: RichProjection): Unit = synchronized {
    val dataset = projection.datasetRef
    if (datasets contains dataset) {
      throw DatasetAlreadySetup(dataset)
    } else {
      val tsdb = new TimeSeriesDataset(projection, config)
      datasets(dataset) = tsdb
    }
  }

  def ingest(dataset: DatasetRef, rows: Seq[RowWithOffset]): Unit = datasets(dataset).ingest(rows)

  def scanPartitions(projection: RichProjection,
                     version: Int,
                     partMethod: PartitionScanMethod): Observable[PartitionChunkIndex] =
    datasets(projection.datasetRef).scanPartitions(partMethod)

  // Use our own readChunks implementation, because it is faster for us to directly create readers
  def readChunks(projection: RichProjection,
                 columns: Seq[Column],
                 version: Int,
                 partMethod: PartitionScanMethod,
                 chunkMethod: ChunkScanMethod = AllChunkScan,
                 colToMaker: ColumnToMaker = defaultColumnToMaker): Observable[ChunkSetReader] = {
    val positions = datasets(projection.datasetRef).getPositions(columns)
    scanPartitions(projection, version, partMethod)
      .flatMap { case p: TimeSeriesPartition =>
        p.streamReaders(p.findByMethod(chunkMethod), positions)
      }
  }

  def getScanSplits(dataset: DatasetRef, splitsPerNode: Int = 1): Seq[ScanSplit] = Seq(InMemoryWholeSplit)

  def reset(): Unit = {
    datasets.clear()
  }

  def shutdown(): Unit = {}
}

// TODO for scalability: get rid of stale partitions?
class TimeSeriesDataset(projection: RichProjection, config: Config) extends StrictLogging {
  private val partitions = new HashMap[PartitionKey, TimeSeriesPartition]
  private final val partKeyFunc = projection.partitionKeyFunc

  private val chunksToKeep = config.getInt("memstore.chunks-to-keep")
  private val maxChunksSize = config.getInt("memstore.max-chunks-size")

  // TODO(velvia): Make this multi threaded
  def ingest(rows: Seq[RowWithOffset]): Unit = {
    // now go through each row, find the partition and call partition ingest
    rows.foreach { case RowWithOffset(reader, offset) =>
      val partKey = partKeyFunc(reader)
      val partition = partitions.getOrElseUpdate(partKey, {
        new TimeSeriesPartition(projection, partKey, chunksToKeep, maxChunksSize)
      })
      partition.ingest(reader, offset)
    }
  }

  def getPositions(columns: Seq[Column]): Array[Int] =
    columns.map { c =>
      val pos = projection.dataColumns.indexOf(c)
      if (pos < 0) throw new IllegalArgumentException(s"Column $c not found in dataset ${projection.datasetRef}")
      pos
    }.toArray

  def scanPartitions(partMethod: PartitionScanMethod): Observable[PartitionChunkIndex] = {
    val indexIt = partMethod match {
      case SinglePartitionScan(partition) =>
        partitions.get(partition).map(Iterator.single).getOrElse(Iterator.empty)
      case MultiPartitionScan(partKeys)   =>
        partKeys.toIterator.flatMap(partitions.get)
      case FilteredPartitionScan(split, filterFunc) =>
        partitions.toIterator.collect { case (part, index) if filterFunc(part) => index }
    }
    Observable.fromIterator(indexIt)
  }

}