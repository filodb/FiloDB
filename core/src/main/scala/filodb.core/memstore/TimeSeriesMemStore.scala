package filodb.core.memstore

import com.googlecode.javaewah.IntIterator
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import kamon.Kamon
import kamon.metric.instrument.Gauge
import monix.reactive.Observable
import org.velvia.filo.ZeroCopyUTF8String
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

import filodb.core.DatasetRef
import filodb.core.metadata.{Column, RichProjection}
import filodb.core.query.{ChunkSetReader, KeyFilter, PartitionChunkIndex, PartitionKeyIndex}
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

  def indexNames(dataset: DatasetRef): Iterator[String] =
    datasets.get(dataset).map(d => d.indexNames).getOrElse(Iterator.empty)

  def indexValues(dataset: DatasetRef, indexName: String): Iterator[ZeroCopyUTF8String] =
    datasets.get(dataset).map(d => d.indexValues(indexName)).getOrElse(Iterator.empty)

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

object TimeSeriesDataset {
  val rowsIngested = Kamon.metrics.counter("memstore-rows-ingested")
  val partitionsCreated = Kamon.metrics.counter("memstore-partitions-created")
}

// TODO for scalability: get rid of stale partitions?
// This would involve something like this:
//    - Go through oldest (lowest index number) partitions
//    - If partition still used, move it to a higher (latest) index
//    - Re-use number for newer partition?  Something like a ring index
class TimeSeriesDataset(projection: RichProjection, config: Config) extends StrictLogging {
  import TimeSeriesDataset._

  private final val partitions = new ArrayBuffer[TimeSeriesPartition]
  private final val keyMap = new HashMap[PartitionKey, Int]
  private final val keyIndex = new PartitionKeyIndex(projection)
  private final val partKeyFunc = projection.partitionKeyFunc

  private val chunksToKeep = config.getInt("memstore.chunks-to-keep")
  private val maxChunksSize = config.getInt("memstore.max-chunks-size")
  private val numPartitions = Kamon.metrics.gauge(s"num-partitions-${projection.datasetRef.dataset}",
                                                  5.seconds)(new Gauge.CurrentValueCollector {
                                                   def currentValue: Long = partitions.size.toLong
                                                  })

  class PartitionIterator(intIt: IntIterator) extends Iterator[TimeSeriesPartition] {
    def hasNext: Boolean = intIt.hasNext
    def next: TimeSeriesPartition = partitions(intIt.next)
  }

  // TODO(velvia): Make this multi threaded
  def ingest(rows: Seq[RowWithOffset]): Unit = {
    rowsIngested.increment(rows.length)
    // now go through each row, find the partition and call partition ingest
    rows.foreach { case RowWithOffset(reader, offset) =>
      val partKey = partKeyFunc(reader)
      val partIndex = keyMap.getOrElseUpdate(partKey, addPartition(partKey))
      val partition = partitions(partIndex)
      partition.ingest(reader, offset)
    }
  }

  def indexNames: Iterator[String] = keyIndex.indexNames

  def indexValues(indexName: String): Iterator[ZeroCopyUTF8String] = keyIndex.indexValues(indexName)

  // Creates a new TimeSeriesPartition, updating indexes
  private def addPartition(newPartKey: PartitionKey): Int = {
    val newPart = new TimeSeriesPartition(projection, newPartKey, chunksToKeep, maxChunksSize)
    val newIndex = partitions.length
    keyIndex.addKey(newPartKey, newIndex)
    partitions += newPart
    partitionsCreated.increment
    newIndex
  }

  private def getPartition(partKey: PartitionKey): Option[PartitionChunkIndex] =
    keyMap.get(partKey).map(partitions.apply)

  def getPositions(columns: Seq[Column]): Array[Int] =
    columns.map { c =>
      val pos = projection.dataColumns.indexOf(c)
      if (pos < 0) throw new IllegalArgumentException(s"Column $c not found in dataset ${projection.datasetRef}")
      pos
    }.toArray

  def scanPartitions(partMethod: PartitionScanMethod): Observable[PartitionChunkIndex] = {
    val indexIt = partMethod match {
      case SinglePartitionScan(partition) =>
        getPartition(partition).map(Iterator.single).getOrElse(Iterator.empty)
      case MultiPartitionScan(partKeys)   =>
        partKeys.toIterator.flatMap(getPartition)
      case FilteredPartitionScan(split, filters) =>
        // TODO: Use filter func for columns not in index
        if (filters.nonEmpty) {
          val (indexIt, _) = keyIndex.parseFilters(filters)
          new PartitionIterator(indexIt)
        } else {
          partitions.toIterator
        }
    }
    Observable.fromIterator(indexIt)
  }

}