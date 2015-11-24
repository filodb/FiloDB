package filodb.core.columnstore

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Try

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

case class ColumnStoreAnalysis(numSegments: Int,
                               numPartitions: Int,
                               rowsInSegment: Histogram,
                               chunksInSegment: Histogram,
                               segmentsInPartition: Histogram) {
  def prettify(): String = {
    s"ColumnStoreAnalysis\n  numSegments: $numSegments\n  numPartitions: $numPartitions\n" +
    rowsInSegment.prettify("# Rows in a segment") +
    chunksInSegment.prettify("# Chunks in a segment") +
    segmentsInPartition.prettify("# Segments in a partition")
  }
}

/**
 * Analyzes the segments and chunks for a given dataset/version.  Gives useful information
 * about distribution of segments and chunks within segments.  Should be run offline, as could take a while.
 */
object Analyzer {
  val NumSegmentsBucketKeys = Array(0, 10, 50, 100, 500)
  val NumChunksPerSegmentBucketKeys = Array(0, 5, 10, 25, 50, 100)
  val NumRowsPerSegmentBucketKeys = Array(0, 10, 100, 1000, 5000, 10000, 50000)

  def analyze(cs: CachedMergingColumnStore, dataset: TableName, version: Int): ColumnStoreAnalysis = {
    var numSegments = 0
    var rowsInSegment: Histogram = Histogram.empty
    var chunksInSegment: Histogram = Histogram.empty
    var segmentsInPartition: Histogram = Histogram.empty
    val partitionSegments = (new collection.mutable.HashMap[PartitionKey, Int]).withDefaultValue(0)

    for { param <- cs.getScanSplits(dataset)
          rowmaps = Await.result(cs.scanChunkRowMaps(dataset, version, (p: PartitionKey) => true,
                                                     params = param), 5.minutes)
          (partKey, _, rowmap) <- rowmaps } yield {
      // Figure out # chunks and rows per segment
      val numRows = rowmap.chunkIds.length
      val numChunks = rowmap.nextChunkId

      numSegments = numSegments + 1
      rowsInSegment = rowsInSegment.add(numRows, NumRowsPerSegmentBucketKeys)
      chunksInSegment = chunksInSegment.add(numChunks, NumChunksPerSegmentBucketKeys)
      partitionSegments(partKey) += 1
    }

    for { (partKey, numSegments) <- partitionSegments } {
      segmentsInPartition = segmentsInPartition.add(numSegments, NumSegmentsBucketKeys)
    }

    ColumnStoreAnalysis(numSegments, partitionSegments.size,
                        rowsInSegment, chunksInSegment, segmentsInPartition)
  }
}