package filodb.coordinator.queryengine

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.reactive.Observable
import org.scalactic._

import filodb.coordinator.ShardMapper
import filodb.core.{ErrorResponse, SpreadProvider}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Dataset
import filodb.core.query.{ColumnFilter, Filter, QueryContext, QueryStats, QueryWarnings, RangeVector, ResultSchema}
import filodb.core.store._
import filodb.query.{QueryError, QueryResponse, QueryResult, StreamQueryError, StreamQueryResponse,
                     StreamQueryResult, StreamQueryResultFooter, StreamQueryResultHeader}

final case class ChildErrorResponse(source: ActorRef, resp: ErrorResponse) extends
    Exception(s"From [$source] - $resp")

/**
 * Some utility methods useful for the rest of Query Engine, especially for helping
 * Logical -> Physical Plan conversion and implementing the Distribute* physical primitives
 */
object Utils extends StrictLogging {
  import filodb.coordinator.client.QueryCommands._
  import TrySugar._

  /**
   * Validates a PartitionQuery, returning a set of PartitionScanMethods with shard numbers.
   * @param dataset the Dataset to query
   * @param shardMap a ShardMapper containing the routing from shards to nodes/coordinators
   * @param partQuery the PartitionQuery to validate
   * @param options the QueryContext
   */
  def validatePartQuery(dataset: Dataset, shardMap: ShardMapper,
                        partQuery: PartitionQuery,
                        options: QueryContext, spreadProvider: SpreadProvider):
  Seq[PartitionScanMethod] Or ErrorResponse =
    Try(partQuery match {
      case SinglePartitionQuery(keyParts) =>
        val partKey = dataset.partKey(keyParts: _*)
        val shard = shardMap.partitionToShardNode(partKey.hashCode).shard
        Seq(SinglePartitionScan(partKey, shard))

      case MultiPartitionQuery(keys) =>
        val partKeys = keys.map { k => dataset.partKey(k: _*) }
        partKeys.groupBy { pk => shardMap.partitionToShardNode(pk.hashCode).shard }
          .toSeq
          .filterNot { case (shard, keys) =>
            val emptyShard = shardMap.unassigned(shard)
            if (emptyShard) logger.warn(s"Ignoring ${keys.length} keys from unassigned shard $shard")
            emptyShard
          }
          .map { case (shard, keys) => MultiPartitionScan(keys, shard) }

      case FilteredPartitionQuery(filters) =>
        // get limited # of shards if shard key available, otherwise query all shards
        // TODO: monitor ratio of queries using shardKeyHash to queries that go to all shards
        val shards = options.plannerParams.shardOverrides.getOrElse {
          val shardCols = dataset.options.shardKeyColumns
          if (shardCols.length > 0) {
            shardHashFromFilters(filters, shardCols, dataset) match {
              case Some(shardHash) => shardMap.queryShards(shardHash,
                                                           spreadProvider.spreadFunc(filters).last.spread)
              case None            => throw new IllegalArgumentException(s"Must specify filters for $shardCols")
            }
          } else {
            shardMap.assignedShards
          }
        }
        logger.debug(s"Translated filters $filters into shards $shards using spread")
        shards.map { s => FilteredPartitionScan(ShardSplit(s), filters) }
    }).toOr.badMap {
      case m: MatchError => BadQuery(s"Could not parse $partQuery: ${m.getMessage}")
      case e: Exception => BadArgument(e.getMessage)
    }

  private def shardHashFromFilters(filters: Seq[ColumnFilter],
                                   shardColumns: Seq[String],
                                   dataset: Dataset): Option[Int] = {
    val shardValMap = shardColumns.map { shardCol =>
      // So to compute the shard hash we need shardCol == value filter (exact equals) for each shardColumn
      filters.find(f => f.column == shardCol) match {
        case Some(ColumnFilter(_, Filter.Equals(filtVal: String))) => shardCol -> filtVal
        case Some(ColumnFilter(_, filter)) =>
          logger.debug(s"Found filter for shard column $shardCol but $filter cannot be used for shard key routing")
          return None
        case _ =>
          logger.debug(s"Could not find filter for shard key column $shardCol, shard key hashing disabled")
          return None
      }
    }.toMap
    val metricColumn = dataset.options.metricColumn
    val metric = shardValMap(metricColumn)
    Some(RecordBuilder.shardKeyHash((shardValMap - metricColumn).values.toSeq, metricColumn, metric))
  }

  /**
   * Performs a scatter gather of a request to different NodeCoordinator's,
   * handling error responses, and returning it as an observable.
   * @param coordsAndMsgs a Seq of each coordinator ref to send a request to and the request message
   * @param parallelism at most this many requests will be outstanding at a time
   * @return an Observable[A] where A is the desired return type
   */
  def scatterGather[A](coordsAndMsgs: Seq[(ActorRef, Any)], parallelism: Int)
                      (implicit t: Timeout, ec: ExecutionContext): Observable[A] =
    Observable.fromIterable(coordsAndMsgs)
              .filter(_._1 != ActorRef.noSender)               // Filter out null ActorRef's
              .mapParallelUnordered(parallelism) { case (coordRef, msg) =>
                val future: Future[A] = (coordRef ? msg).map {
                  case err: ErrorResponse => throw ChildErrorResponse(coordRef, err)
                  case a: A @unchecked    => logger.trace(s"Received $a from $coordRef"); a
                }
                future.failed.foreach {
                  case e: Exception => logger.warn(s"Error asking $coordRef message $msg", e)
                }
                Task.fromFuture(future)
              }

  /**
   * A variant of the above where one can pass in PartitionScanMethods and a function to convert to a message
   */
  def scatterGather[A](shardMap: ShardMapper,
                       partMethods: Seq[PartitionScanMethod],
                       parallelism: Int)(msgFunc: PartitionScanMethod => Any)
                      (implicit t: Timeout, ec: ExecutionContext): Observable[A] = {
    val coordsAndMsgs = partMethods.map { method =>
      (shardMap.coordForShard(method.shard), msgFunc(method))
    }
    scatterGather[A](coordsAndMsgs, parallelism)
  }

  def streamToFatQueryResponse(queryContext: QueryContext,
                               resp: Observable[StreamQueryResponse]): Task[QueryResponse] = {
    val rvs = new ArrayBuffer[RangeVector]()
    var stats: Option[QueryStats] = None
    var warnings: Option[QueryWarnings] = None
    var ex: Option[Throwable] = None
    var mayBePartial = false
    var partialResultReason: Option[String] = None
    var rs: Option[ResultSchema] = None

    resp.takeWhileInclusive(!_.isLast).map {
      case h: StreamQueryResultHeader =>
        rs = Some(h.resultSchema)
      case e: StreamQueryError =>
        stats = Some(e.queryStats)
        ex = Some(e.t)
      case r: StreamQueryResult =>
        rvs ++= r.result
      case f: StreamQueryResultFooter =>
        stats = Some(f.queryStats)
        mayBePartial = f.mayBePartial
        partialResultReason = f.partialResultReason
        warnings = Some(f.warnings)
    }.completedL.map { _ =>
      if (rs.isEmpty)
          QueryError(queryContext.queryId, stats.get, new IllegalStateException("Result didnt carry a header"))
      else if (ex.isDefined) QueryError(queryContext.queryId, stats.get, ex.get)
      else QueryResult(queryContext.queryId, rs.get, rvs,
                       stats.get, warnings.get, mayBePartial, partialResultReason)
    }
  }
}
