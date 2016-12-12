package filodb.core.store

import monix.reactive.Observable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
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
  def prettify(): String = {
    s"ColumnStoreAnalysis\n  numPartitions: $numPartitions\n" +
    s"  total rows: $totalRows\n  skipped rows: $skippedRows\n" +
    rowsInPartition.prettify("# Rows in a partition") +
    skippedRowsInPartition.prettify("# Skipped Rows in a partition") +
    chunksInPartition.prettify("# Chunks in a partition") +
    rowsPerChunk.prettify("# Rows Per Chunk")
  }
}

case class ChunkInfo(partKey: BinaryPartition, chunkInfo: ChunkSetInfo)

/**
 * Analyzes the segments and chunks for a given dataset/version.  Gives useful information
 * about distribution of segments and chunks within segments.  Should be run offline, as could take a while.
 */
object Analyzer {
  val NumSegmentsBucketKeys = Array(0, 10, 50, 100, 500)
  val NumChunksPerSegmentBucketKeys = Array(0, 5, 10, 25, 50, 100)
  val NumRowsPerSegmentBucketKeys = Array(0, 10, 100, 1000, 5000, 10000, 50000)

  import monix.execution.Scheduler.Implicits.global

  def analyze(cs: ColumnStore with ColumnStoreScanner,
              metaStore: MetaStore,
              dataset: DatasetRef,
              version: Int,
              splitCombiner: Seq[ScanSplit] => ScanSplit,
              maxPartitions: Int = 10000): ColumnStoreAnalysis = {
    var numPartitions = 0
    var totalRows = 0
    var skippedRows = 0
    var rowsInPartition: Histogram = Histogram.empty
    var skippedInPart: Histogram = Histogram.empty
    var chunksInPartition: Histogram = Histogram.empty
    var rowsPerChunk: Histogram = Histogram.empty

    val datasetObj = Await.result(metaStore.getDataset(dataset), 1.minutes)
    val schema = Await.result(metaStore.getSchema(dataset, version), 1.minutes)
    val projection = RichProjection(datasetObj, schema.values.toSeq)
    val split = splitCombiner(cs.getScanSplits(dataset, 1))

    cs.scanPartitions(projection, version, FilteredPartitionScan(split))
      .take(maxPartitions)
      .foreach { chunkIndex =>
      // Figure out # chunks and rows per partition
      val numRows = chunkIndex.allChunks.map(_._1.numRows).sum
      val numSkipped = chunkIndex.allChunks.map(_._2.size).sum
      val numChunks = chunkIndex.numChunks

      numPartitions = numPartitions + 1
      totalRows += numRows
      skippedRows += numSkipped
      rowsInPartition = rowsInPartition.add(numRows, NumRowsPerSegmentBucketKeys)
      skippedInPart = skippedInPart.add(numSkipped, NumRowsPerSegmentBucketKeys)
      chunksInPartition = chunksInPartition.add(numChunks, NumChunksPerSegmentBucketKeys)
      rowsPerChunk = rowsPerChunk.add(numRows / numChunks, NumRowsPerSegmentBucketKeys)
    }

    ColumnStoreAnalysis(numPartitions, totalRows, skippedRows,
                        rowsInPartition, skippedInPart, chunksInPartition, rowsPerChunk)
  }

  def getChunkInfos(cs: ColumnStore with ColumnStoreScanner,
                    metaStore: MetaStore,
                    dataset: DatasetRef,
                    version: Int,
                    splitCombiner: Seq[ScanSplit] => ScanSplit,
                    maxPartitions: Int = 100): Observable[ChunkInfo] = {
    val datasetObj = Await.result(metaStore.getDataset(dataset), 1.minutes)
    val schema = Await.result(metaStore.getSchema(dataset, version), 1.minutes)
    val projection = RichProjection(datasetObj, schema.values.toSeq)
    val split = splitCombiner(cs.getScanSplits(dataset, 1))

    cs.scanPartitions(projection, version, FilteredPartitionScan(split))
      .take(maxPartitions)
      .flatMap { index =>
        val it = index.allChunks.map { case (info, skips) => ChunkInfo(index.binPartition, info) }
        Observable.fromIterator(it)
      }
  }
}