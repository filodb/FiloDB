package filodb.cassandra.query

import java.net.InetAddress

import com.datastax.driver.core.{Metadata, Session, TokenRange => DriverTokenRange}
import filodb.core.Types._
import filodb.core.metadata.{KeyRange, Projection}
import filodb.core.query.{ScanInfo, SegmentedScanInfo}

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

case class TokenRangeScanInfo(projection: Projection,
                              columns: Seq[ColumnId],
                              tokenRange: TokenRange) extends ScanInfo {
  override def locationInfo: Seq[InetAddress] = tokenRange.replicas.toSeq
}

case class SegmentedTokenRangeScanInfo(projection: Projection,
                                       columns: Seq[ColumnId],
                                       tokenRange: TokenRange,
                                       segmentRange: KeyRange[_])
  extends SegmentedScanInfo {
  override def locationInfo: Seq[InetAddress] = tokenRange.replicas.toSeq
}


case class TokenRange(start: Long, end: Long,
                      replicas: Set[InetAddress],
                      dataSize: Long) {

  def isWrapAround: Boolean =
    start >= end

  def unwrap(): Seq[TokenRange] = {
    val minToken = Long.MinValue
    if (isWrapAround) {
      Seq(
        TokenRange(start, minToken, replicas, dataSize / 2),
        TokenRange(minToken, end, replicas, dataSize / 2))
    } else {
      Seq(this)
    }
  }
}

object Cluster {

  import scala.collection.JavaConversions._

  val MaxParallelism = 16
  private val pool: ForkJoinPool = new ForkJoinPool(MaxParallelism)

  def getClusterTokenRanges(session: Session,
                            keySpaceName: String,
                            splitCount: Int,
                            splitSize: Long): Array[Seq[TokenRange]] = {
    val totalDataSize = splitCount * splitSize
    val tokenRanges = describeRing(keySpaceName, totalDataSize, session)
    val endpointCount = tokenRanges.map(_.replicas).reduce(_ ++ _).size
    val splits = splitsOf(splitSize, tokenRanges).toSeq
    val maxGroupSize = tokenRanges.size / endpointCount
    val tokenRangeGrouper = new TokenRangeGrouper(splitSize, maxGroupSize)
    tokenRangeGrouper.group(splits).toArray
  }

  private def describeRing(keySpaceName: String, totalDataSize: Long, session: Session) = {
    val cluster = session.getCluster
    val metadata = cluster.getMetadata
    for {tr <- metadata.getTokenRanges.toSeq}
      yield tokenRange(keySpaceName, totalDataSize, tr, metadata)
  }

  private def tokenRange(keyspaceName: String,
                         totalDataSize: Long,
                         range: DriverTokenRange,
                         metadata: Metadata): TokenRange = {
    val startToken = range.getStart.getValue.toString.toLong
    val endToken = range.getEnd.getValue.toString.toLong
    val replicas = metadata.getReplicas(Metadata.quote(keyspaceName), range).map(_.getAddress).toSet
    val dataSize = (ringFraction(startToken, endToken) * totalDataSize).toLong
    new TokenRange(startToken, endToken, replicas, dataSize)
  }

  private def splitsOf(splitSize: Long,
                       tokenRanges: Iterable[TokenRange]): Iterable[TokenRange] = {
    val parTokenRanges = tokenRanges.par
    parTokenRanges.tasksupport = new ForkJoinTaskSupport(Cluster.pool)

    (for {tokenRange <- parTokenRanges; split <- split(tokenRange, splitSize)}
      yield split).seq
  }

  val totalTokenCount = BigInt(Long.MaxValue) - BigInt(Long.MinValue)

  private def ringFraction(token1: Long, token2: Long): Double =
    distance(token1, token2).toDouble / totalTokenCount.toDouble

  private def distance(left: Long, right: Long): BigInt = {
    if (right > left) {
      BigInt(right) - BigInt(left)
    }
    else {
      BigInt(right) - BigInt(left) + totalTokenCount
    }
  }

  private def split(range: TokenRange, splitSize: Long): Seq[TokenRange] = {
    val rangeSize = range.dataSize
    val rangeTokenCount = distance(range.start, range.end)
    val n = math.max(1, math.round(rangeSize.toDouble / splitSize).toInt)

    val left = range.start
    val right = range.end
    val splitPoints =
      (for {i <- 0 until n} yield left + (rangeTokenCount * i / n).toLong) :+ right

    for {Seq(l, r) <- splitPoints.sliding(2).toSeq} yield
    new TokenRange(l, r, range.replicas, rangeSize / n)
  }

}

import java.net.InetAddress

import scala.Ordering.Implicits._
import scala.annotation.tailrec

/** Divides a set of token ranges into groups containing not more than `maxRowCountPerGroup` rows
  * and not more than `maxGroupSize` token ranges. Each group will form a single `ScanInfo`.
  *
  * The algorithm is as follows:
  * 1. Sort token ranges by endpoints lexicographically.
  * 2. Take the highest possible number of token ranges from the beginning of the list,
  * such that their sum of rowCounts does not exceed `maxRowCountPerGroup` and they all contain at
  * least one common endpoint. If it is not possible, take at least one item.
  * Those token ranges will make a group.
  * 3. Repeat the previous step until no more token ranges left. */
class TokenRangeGrouper(maxRowCountPerGroup: Long, maxGroupSize: Int = Int.MaxValue) {

  private implicit object InetAddressOrdering extends Ordering[InetAddress] {
    override def compare(x: InetAddress, y: InetAddress): Int =
      x.getHostAddress.compareTo(y.getHostAddress)
  }

  @tailrec
  private def group(tokenRanges: Stream[TokenRange],
                    result: Vector[Seq[TokenRange]]): Iterable[Seq[TokenRange]] = {
    tokenRanges match {
      case Stream.Empty => result
      case head #:: rest =>
        val firstEndpoint = head.replicas.min
        val rowCounts = tokenRanges.map(_.dataSize)
        val cumulativeRowCounts = rowCounts.scanLeft(0L)(_ + _).tail // drop first item always == 0
      val rowLimit = math.max(maxRowCountPerGroup, head.dataSize) // make sure first element will be always included
      val cluster = tokenRanges
          .take(math.max(1, maxGroupSize))
          .zip(cumulativeRowCounts)
          .takeWhile { case (tr, count) => count <= rowLimit && tr.replicas.min == firstEndpoint }
          .map(_._1)
          .toVector
        val remainingTokenRanges = tokenRanges.drop(cluster.length)
        group(remainingTokenRanges, result :+ cluster)
    }
  }

  /** Groups small token ranges on the same server(s) in order to reduce task scheduling overhead.
    * Useful mostly with virtual nodes, which may create lots of small token range splits.
    * Each group will for example make a single Spark task. */
  def group(tokenRanges: Seq[TokenRange]): Iterable[Seq[TokenRange]] = {
    // sort by endpoints lexicographically
    // this way ranges on the same host are grouped together
    val sortedRanges = tokenRanges.sortBy(_.replicas.toSeq.sorted)
    group(sortedRanges.toStream, Vector.empty)
  }

}
