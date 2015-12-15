package filodb.cassandra.cluster

import java.net.InetAddress

import com.datastax.driver.core.{Metadata, Session, TokenRange => DriverTokenRange}
import filodb.cassandra.query.TokenRange

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

object Cluster {

  import scala.collection.JavaConversions._

  val MaxParallelism = 16
  private val pool: ForkJoinPool = new ForkJoinPool(MaxParallelism)

  def getNodeGroupedTokenRanges(tokenRanges:Seq[TokenRange],
                            keySpaceName: String,
                            splitSize: Long): Array[Seq[TokenRange]] = {

    val endpointCount = tokenRanges.map(_.replicas).reduce(_ ++ _).size
    val splits = splitsOf(splitSize, tokenRanges).toSeq
    val maxGroupSize = tokenRanges.size / endpointCount
    val tokenRangeGrouper = new TokenRangeGrouper(splitSize, maxGroupSize)
    tokenRangeGrouper.group(splits).toArray
  }

  def describeRing(keySpaceName: String,
                   totalDataSize: Long,
                   session: Session):Seq[TokenRange] = {
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




