package filodb.core.memstore

import com.googlecode.javaewah.IntIterator
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import kamon.Kamon
import kamon.metric.instrument.Gauge
import monix.reactive.Observable
import org.velvia.filo.{SchemaRowReader, ZeroCopyUTF8String}
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

import filodb.core.DatasetRef
import filodb.core.metadata.{Column, RichProjection}
import filodb.core.query.{ChunkSetReader, KeyFilter, PartitionChunkIndex, PartitionKeyIndex, FiloPartition}
import filodb.core.store._
import filodb.core.Types.{PartitionKey, ChunkID}

final case class DatasetAlreadySetup(dataset: DatasetRef) extends Exception(s"Dataset $dataset already setup")

class TimeSeriesMemStore(config: Config)(implicit val ec: ExecutionContext)
extends MemStore with ColumnStoreAggregator with StrictLogging {
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

  def ingest(dataset: DatasetRef, rows: Seq[IngestRecord]): Unit = datasets(dataset).ingest(rows)

  def indexNames(dataset: DatasetRef): Iterator[String] =
    datasets.get(dataset).map(d => d.indexNames).getOrElse(Iterator.empty)

  def indexValues(dataset: DatasetRef, indexName: String): Iterator[ZeroCopyUTF8String] =
    datasets.get(dataset).map(d => d.indexValues(indexName)).getOrElse(Iterator.empty)

  def numPartitions(dataset: DatasetRef): Int =
    datasets.get(dataset).map(_.numActivePartitions).getOrElse(-1)

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
    val positions = projection.getPositions(columns)
    scanPartitions(projection, version, partMethod)
      .flatMap { case p: PartitionChunkIndex with FiloPartition =>
        p.streamReaders(p.findByMethod(chunkMethod), positions)
      }
  }

  def indexToPartition(index: PartitionChunkIndex): FiloPartition = index match {
    case p: TimeSeriesPartition => p
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
  private final val keyMap = new HashMap[SchemaRowReader, TimeSeriesPartition]
  private final val keyIndex = new PartitionKeyIndex(projection)

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
  // TODO(velvia): OR, for efficiency, allow multiple data records for each partition key
  def ingest(rows: Seq[IngestRecord]): Unit = {
    rowsIngested.increment(rows.length)
    // now go through each row, find the partition and call partition ingest
    rows.foreach { case IngestRecord(partKey, data, offset) =>
      val partition = keyMap.getOrElse(partKey, addPartition(partKey))
      partition.ingest(data, offset)
    }
  }

  def indexNames: Iterator[String] = keyIndex.indexNames

  def indexValues(indexName: String): Iterator[ZeroCopyUTF8String] = keyIndex.indexValues(indexName)

  def numActivePartitions: Int = keyMap.size

  // Creates a new TimeSeriesPartition, updating indexes
  // NOTE: it's important to use an actual BinaryRecord instead of just a RowReader in the internal
  // data structures.  The translation to BinaryRecord only happens here (during first time creation
  // of a partition) and keeps internal data structures from having to keep copies of incoming records
  // around, which might be much more expensive memory wise.  One consequence though is that internal
  // and external partition key components need to yield the same hashCode.  IE, use UTF8Strings everywhere.
  private def addPartition(newPartKey: SchemaRowReader): TimeSeriesPartition = {
    val binPartKey = projection.partKey(newPartKey)
    val newPart = new TimeSeriesPartition(projection, binPartKey, chunksToKeep, maxChunksSize)
    val newIndex = partitions.length
    keyIndex.addKey(binPartKey, newIndex)
    partitions += newPart
    partitionsCreated.increment
    keyMap(binPartKey) = newPart
    newPart
  }

  private def getPartition(partKey: PartitionKey): Option[PartitionChunkIndex] = keyMap.get(partKey)

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