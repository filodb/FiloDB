package filodb.core.store

import monix.reactive.Observable
import scala.concurrent.Future
import scala.util.Try

import filodb.core.DatasetRef
import filodb.core.metadata.RichProjection
import filodb.core.Types._

case class Histogram(min: Int, max: Int, sum: Int, numElems: Int, buckets: Map[Int, Int]) {
  /** Adds an element to the Histogram, adding it to histogram buckets according to the
   *  bucket keys
   */
  def add(elem: Int, bucketKeys: Array[Int]): Histogram = {
    // See the JavaDoc for binarySearch...
    val bucket = java.util.Arrays.binarySearch(bucketKeys, elem) match {
      case x: Int if x < 0  =>  -x - 2
      case x: Int if x >= bucketKeys.size =>  bucketKeys.size - 1
      case x: Int => x
    }
    val key = bucketKeys(bucket)
    copy(min = Math.min(elem, this.min),
         max = Math.max(elem, this.max),
         sum = this.sum + elem,
         numElems = this.numElems + 1,
         buckets = buckets + (key -> (buckets.getOrElse(key, 0) + 1)))
  }

  def prettify(name: String, maxBarLen: Int = 60): String = {
    val head = s"===== $name =====\n  Min: $min\n  Max: $max\n  Average: ${sum/numElems.toDouble} ($numElems)"
    val maxQuantity = Try(buckets.values.max).getOrElse(0)
    buckets.toSeq.sortBy(_._1).foldLeft(head + "\n") { case (str, (bucket, num)) =>
      val numChars = (num * maxBarLen) / maxQuantity
      str + "| %08d: %08d %s".format(bucket, num, "X" * numChars) + "\n"
    }
  }
}

object Histogram {
  val empty: Histogram = Histogram(Int.MaxValue, Int.MinValue, 0, 0, Map.empty)
}

case class ColumnStoreAnalysis(numPartitions: Int,
                               totalRows: Int,
                               skippedRows: Int,
                               rowsInPartition: Histogram,
                               skippedRowsInPartition: Histogram,
                               chunksInPartition: Histogram,
                               rowsPerChunk: Histogram) {
  import ColumnStoreAnalysis._

  def prettify(): String = {
    s"ColumnStoreAnalysis\n  numPartitions: $numPartitions\n" +
    s"  total rows: $totalRows\n  skipped rows: $skippedRows\n" +
    rowsInPartition.prettify("# Rows in a partition") +
    skippedRowsInPartition.prettify("# Skipped Rows in a partition") +
    chunksInPartition.prettify("# Chunks in a partition") +
    rowsPerChunk.prettify("# Rows Per Chunk")
  }

  def addPartitionStats(numRows: Int, numSkipped: Int, numChunks: Int): ColumnStoreAnalysis =
    ColumnStoreAnalysis(this.numPartitions + 1,
                        this.totalRows + numRows,
                        this.skippedRows + numSkipped,
                        this.rowsInPartition.add(numRows, NumRowsPerPartBucketKeys),
                        this.skippedRowsInPartition.add(numSkipped, NumRowsPerPartBucketKeys),
                        this.chunksInPartition.add(numChunks, NumChunksPerPartBucketKeys),
                        this.rowsPerChunk.add(numRows / numChunks, NumRowsPerChunkBucketKeys))
}

object ColumnStoreAnalysis {
  val empty = ColumnStoreAnalysis(0, 0, 0,
                                  Histogram.empty, Histogram.empty, Histogram.empty, Histogram.empty)

  val NumRowsPerChunkBucketKeys = Array(0, 10, 50, 100, 500, 1000, 5000)
  val NumChunksPerPartBucketKeys = Array(0, 10, 50, 100, 500, 1000)
  val NumRowsPerPartBucketKeys = Array(0, 100, 10000, 100000, 500000, 1000000, 5000000)
}

case class ChunkInfo(partKey: PartitionKey, chunkInfo: ChunkSetInfo)

/**
 * Analyzes the segments and chunks for a given dataset/version.  Gives useful information
 * about distribution of segments and chunks within segments.  Should be run offline, as could take a while.
 */
object Analyzer {
  import monix.execution.Scheduler.Implicits.global

  def analyze(cs: ColumnStore with ColumnStoreScanner,
              metaStore: MetaStore,
              dataset: DatasetRef,
              version: Int,
              splitCombiner: Seq[ScanSplit] => ScanSplit,
              maxPartitions: Int = 1000): Future[ColumnStoreAnalysis] = {
    val split = splitCombiner(cs.getScanSplits(dataset, 1))
    for { projection <- getProjection(dataset, version, metaStore)
          analysis   <- cs.scanPartitions(projection, version, FilteredPartitionScan(split))
                          .take(maxPartitions)
                          .foldLeftL(ColumnStoreAnalysis.empty) { case (analysis, chunkIndex) =>
                            // Figure out # chunks and rows per partition
                            val numRows = chunkIndex.allChunks.map(_._1.numRows).sum
                            val numSkipped = chunkIndex.allChunks.map(_._2.cardinality).sum
                            val numChunks = chunkIndex.numChunks
                            analysis.addPartitionStats(numRows, numSkipped, numChunks)
                          }.runAsync
    } yield { analysis }
  }

  def getChunkInfos(cs: ColumnStore with ColumnStoreScanner,
                    metaStore: MetaStore,
                    dataset: DatasetRef,
                    version: Int,
                    splitCombiner: Seq[ScanSplit] => ScanSplit,
                    maxPartitions: Int = 100): Observable[ChunkInfo] = {
    val split = splitCombiner(cs.getScanSplits(dataset, 1))
    for { projection <- Observable.fromFuture(getProjection(dataset, version, metaStore))
          index      <- cs.scanPartitions(projection, version, FilteredPartitionScan(split))
                          .take(maxPartitions)
          it = index.allChunks.map { case (info, skips) => ChunkInfo(index.binPartition, info) }
          info       <- Observable.fromIterator(it)
    } yield { info }
  }

  private def getProjection(dataset: DatasetRef, version: Int, metaStore: MetaStore):
      Future[RichProjection] =
    for { datasetObj <- metaStore.getDataset(dataset)
          schema     <- metaStore.getSchema(dataset, version) }
    yield { RichProjection(datasetObj, schema.values.toSeq) }
}