package filodb.coordinator.queryengine

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
import filodb.coordinator.client.QueryCommands
import filodb.core.{ErrorResponse, Types}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Dataset
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.store._

final case class ChildQueryError(source: ActorRef, err: QueryCommands.QueryError) extends
    Exception(s"From [$source] - ${err.toString}", err.t)

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
   * Convert column name strings into columnIDs.  NOTE: column names should not include row key columns
   * as those are automatically prepended.
   */
  def getColumnIDs(dataset: Dataset, colStrs: Seq[String]): Seq[Types.ColumnId] Or ErrorResponse =
    dataset.colIDs(colStrs: _*).badMap(missing => UndefinedColumns(missing.toSet))
           .map { ids =>   // avoid duplication if first ids are already row keys
             if (ids.take(dataset.rowKeyIDs.length) == dataset.rowKeyIDs) { ids }
             else { dataset.rowKeyIDs ++ ids }
           }

  /**
   * Validate and translate a DataQuery from LogicalPlan into a ChunkScanMethod used in physical plan
   */
  def validateDataQuery(dataset: Dataset, dataQuery: DataQuery): ChunkScanMethod Or ErrorResponse = {
    Try(dataQuery match {
      case AllPartitionData                => AllChunkScan
      case KeyRangeQuery(startKey, endKey) => RowKeyChunkScan(dataset, startKey, endKey)
      case MostRecentTime(lastMillis) if dataset.timestampColumn.isDefined =>
        val timeNow = System.currentTimeMillis
        RowKeyChunkScan(dataset, Seq(timeNow - lastMillis), Seq(timeNow))
      case MostRecentSample if dataset.timestampColumn.isDefined => LastSampleChunkScan
    }).toOr.badMap {
      case m: MatchError if dataset.timestampColumn.isEmpty =>
        BadQuery(s"Not a time series schema - cannot filter using $dataQuery: ${m.getMessage}")
      case m: MatchError => BadQuery(s"Could not parse $dataQuery: ${m.getMessage}")
      case e: Exception => BadArgument(e.getMessage)
    }
  }

  /**
   * Validates a PartitionQuery, returning a set of PartitionScanMethods with shard numbers.
   * @param dataset the Dataset to query
   * @param shardMap a ShardMapper containing the routing from shards to nodes/coordinators
   * @param partQuery the PartitionQuery to validate
   * @param options the QueryOptions
   */
  def validatePartQuery(dataset: Dataset, shardMap: ShardMapper,
                        partQuery: PartitionQuery,
                        options: QueryOptions): Seq[PartitionScanMethod] Or ErrorResponse =
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
        val shards = options.shardOverrides.getOrElse {
          val shardCols = dataset.options.shardKeyColumns
          if (shardCols.length > 0) {
            shardHashFromFilters(filters, shardCols) match {
              case Some(shardHash) => shardMap.queryShards(shardHash, options.shardKeySpread)
              case None            => throw new IllegalArgumentException(s"Must specify filters for $shardCols")
            }
          } else {
            shardMap.assignedShards
          }
        }
        logger.debug(s"Translated filters $filters into shards $shards using spread ${options.shardKeySpread}")
        shards.map { s => FilteredPartitionScan(ShardSplit(s), filters) }
    }).toOr.badMap {
      case m: MatchError => BadQuery(s"Could not parse $partQuery: ${m.getMessage}")
      case e: Exception => BadArgument(e.getMessage)
    }

  private def shardHashFromFilters(filters: Seq[ColumnFilter], shardColumns: Seq[String]): Option[Int] = {
    val shardColValues = shardColumns.map { shardCol =>
      // So to compute the shard hash we need shardCol == value filter (exact equals) for each shardColumn
      filters.find(f => f.column == shardCol) match {
        case Some(ColumnFilter(_, Filter.Equals(filtVal: String))) => filtVal
        case Some(ColumnFilter(_, filter)) =>
          logger.debug(s"Found filter for shard column $shardCol but $filter cannot be used for shard key routing")
          return None
        case _ =>
          logger.debug(s"Could not find filter for shard key column $shardCol, shard key hashing disabled")
          return None
      }
    }
    logger.debug(s"For shardColumns $shardColumns, extracted filter values $shardColValues successfully")
    Some(RecordBuilder.shardKeyHash(shardColumns, shardColValues))
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
              .mapAsync(parallelism) { case (coordRef, msg) =>
                val future: Future[A] = (coordRef ? msg).map {
                  case e: QueryError      => throw ChildQueryError(coordRef, e)
                  case err: ErrorResponse => throw ChildErrorResponse(coordRef, err)
                  case a: A @unchecked    => logger.trace(s"Received $a from $coordRef"); a
                }
                future.onFailure {
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
}