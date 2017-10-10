package filodb.coordinator

import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.existentials
import scala.util.Try

import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout
import monix.eval.Task
import monix.reactive.Observable
import org.scalactic._
import org.velvia.filo.BinaryVector

import filodb.core._
import filodb.core.memstore.MemStore
import filodb.core.metadata.{Column, RichProjection, WrongNumberArguments, BadArgument => BadArg}
import filodb.core.query.{AggregationFunction, CombinerFunction, NoTimestampColumn}
import filodb.core.store._

object QueryActor {
  private val nextId = new AtomicLong()
  def nextQueryId: Long = nextId.getAndIncrement

  // Internal command for query on each individual node, directed at one shard only
  final case class SingleShardQuery(query: QueryCommands.QueryArgs,
                                    dataset: DatasetRef,
                                    version: Int,
                                    partMethod: PartitionScanMethod,
                                    chunkScan: ChunkScanMethod)

  def props(memStore: MemStore, projection: RichProjection): Props =
    Props(classOf[QueryActor], memStore, projection)
}

/**
 * Translates external query API calls into internal ColumnStore calls.
 *
 * The actual reading of data structures and aggregation is performed asynchronously by Observables,
 * so it is probably fine for there to be just one QueryActor per dataset.
 */
final class QueryActor(memStore: MemStore,
                       projection: RichProjection) extends BaseActor {
  import OptionSugar._
  import TrySugar._

  import QueryActor._
  import QueryCommands._

  implicit val scheduler = monix.execution.Scheduler(context.dispatcher)
  var shardMap = ShardMapper.default

  def validateColumns(colStrs: Seq[String]): Seq[Column] Or ErrorResponse =
    RichProjection.getColumnsFromNames(projection.columns, colStrs)
                  .badMap {
                    case RichProjection.MissingColumnNames(missing, _) =>
                      NodeClusterActor.UndefinedColumns(missing.toSet)
                    case x: Any =>
                      DatasetCommands.DatasetError(x.toString)
                  }

  // Returns a list of PartitionScanMethods, one per shard
  def validatePartQuery(partQuery: PartitionQuery,
                        options: QueryOptions): Seq[PartitionScanMethod] Or ErrorResponse =
    Try(partQuery match {
      case SinglePartitionQuery(keyParts) =>
        val partKey = projection.partKey(keyParts: _*)
        val shard = shardMap.partitionToShardNode(partKey.hashCode).shard
        Seq(SinglePartitionScan(partKey, shard))

      case MultiPartitionQuery(keys) =>
        val partKeys = keys.map { k => projection.partKey(k: _*) }
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
        // TODO: filter shards by ones that are active?  reroute to other DC? etc.
        // TODO: monitor ratio of queries using shardKeyHash to queries that go to all shards
        val shards = options.shardKeyHash.map { hash =>
                        shardMap.queryShards(hash, options.shardKeySpread)
                     }.getOrElse(shardMap.assignedShards)
        shards.map { s => FilteredPartitionScan(ShardSplit(s), filters) }
    }).toOr.badMap {
      case m: MatchError => BadQuery(s"Could not parse $partQuery: " + m.getMessage)
      case e: Exception => BadArgument(e.getMessage)
    }

  def validateDataQuery(dataQuery: DataQuery): ChunkScanMethod Or ErrorResponse = {
    Try(dataQuery match {
      case AllPartitionData                => AllChunkScan
      case KeyRangeQuery(startKey, endKey) => RowKeyChunkScan(projection, startKey, endKey)
      case MostRecentTime(lastMillis) if projection.isTimeSeries =>
        val timeNow = System.currentTimeMillis
        RowKeyChunkScan(projection, Seq(timeNow - lastMillis), Seq(timeNow))
      case MostRecentSample if projection.isTimeSeries => LastSampleChunkScan
    }).toOr.badMap {
      case m: MatchError if !projection.isTimeSeries =>
        BadQuery(s"Not a time series schema - cannot filter using $dataQuery: " + m.getMessage)
      case m: MatchError => BadQuery(s"Could not parse $dataQuery: " + m.getMessage)
      case e: Exception => BadArgument(e.getMessage)
    }
  }

  def validateFunction(funcName: String): AggregationFunction Or ErrorResponse =
    AggregationFunction.withNameInsensitiveOption(funcName)
                       .toOr(BadQuery(s"No such aggregation function $funcName"))

  def validateCombiner(combinerName: String): CombinerFunction Or ErrorResponse =
    CombinerFunction.withNameInsensitiveOption(combinerName)
                    .toOr(BadQuery(s"No such combiner function $combinerName"))

  def handleRawQuery(q: RawQuery): Unit = {
    val RawQuery(dataset, version, colStrs, partQuery, dataQuery) = q
    val originator = sender()
    (for { colSeq      <- validateColumns(colStrs)
           partMethods <- validatePartQuery(partQuery, QueryOptions())
           chunkMethod <- validateDataQuery(dataQuery) }
    yield {
      val queryId = nextQueryId
      originator ! QueryInfo(queryId, dataset, colSeq.map(_.toString))
      // TODO: this is totally broken if there is more than one partMethod
      memStore.readChunks(projection, colSeq, version, partMethods.head, chunkMethod)
        .foreach { reader =>
          val bufs = reader.vectors.map {
            case b: BinaryVector[_] => b.toFiloBuffer
          }
          originator ! QueryRawChunks(queryId, reader.info.id, bufs)
        }
        // NOTE: for some reason Monix's doOnSuccess... has the wrong timing
        .map { Unit => originator ! QueryEndRaw(queryId) }
        .recover { case err: Exception => originator ! QueryError(queryId, err) }
    }).recover {
      case resp: ErrorResponse => originator ! resp
    }
  }

  // validate high level query params, then send out lower level aggregate queries to shards/coordinators
  // gather them and form an overall response
  def validateAndGatherAggregates(q: AggregateQuery): Unit = {
    val originator = sender()
    (for { aggFunc    <- validateFunction(q.query.functionName)
           combinerFunc <- validateCombiner(q.query.combinerName)
           chunkMethod <- validateDataQuery(q.query.dataQuery)
           aggregator <- aggFunc.validate(q.query.column, projection.timestampColumn.map(_.name),
                                          chunkMethod, q.query.args, projection)
           combiner   <- combinerFunc.validate(aggregator, q.query.combinerArgs)
           partMethods <- validatePartQuery(q.partitionQuery, q.queryOptions) }
    yield {
      val queryId = QueryActor.nextQueryId
      implicit val askTimeout = Timeout(q.queryOptions.queryTimeoutSecs.seconds)
      logger.debug(s"Sending out aggregates $partMethods and combining using $combiner...")
      val results = Observable.fromIterable(partMethods)
                      .mapAsync(q.queryOptions.parallelism) { partMethod =>
                        val coord = shardMap.coordForShard(partMethod.shard)
                        val query = SingleShardQuery(q.query, q.dataset, q.version, partMethod, chunkMethod)
                        val future: Future[combiner.C] = (coord ? query).map {
                          case a: combiner.C @unchecked => a
                          case err: ErrorResponse => throw new RuntimeException(err.toString)
                        }
                        Task.fromFuture(future)
                      }
      val combined = if (partMethods.length > 1) results.reduce(combiner.combine) else results
      combined.headL.runAsync
              .map { agg => originator ! AggregateResponse(queryId, agg.clazz, agg.result) }
              .recover { case err: Exception =>
                logger.error(s"Error during combining: $err", err)
                originator ! QueryError(queryId, err) }
    }).recover {
      case resp: ErrorResponse => originator ! resp
      case WrongNumberArguments(given, expected) => originator ! WrongNumberOfArgs(given, expected)
      case BadArg(reason) => originator ! BadArgument(reason)
      case NoTimestampColumn =>
        originator ! BadQuery(s"Cannot use time-based functions on dataset ${projection.datasetRef}")
      case other: Any     => originator ! BadQuery(other.toString)
    }
  }

  // lower level handling of per-shard aggregate
  def singleShardQuery(q: SingleShardQuery): Unit = {
    val originator = sender()
    (for { aggFunc    <- validateFunction(q.query.functionName)
           combinerFunc <- validateCombiner(q.query.combinerName)
           qSpec = QuerySpec(q.query.column, aggFunc, q.query.args, combinerFunc, q.query.combinerArgs)
           aggregateTask <- memStore.aggregate(projection, q.version, qSpec, q.partMethod, q.chunkScan) }
    yield {
      aggregateTask.runAsync
        .map { agg => originator ! agg }
        .recover { case err: Exception => originator ! QueryError(-1, err) }
    }).recover {
      case resp: ErrorResponse => originator ! resp
      case WrongNumberArguments(given, expected) => originator ! WrongNumberOfArgs(given, expected)
      case BadArg(reason) => originator ! BadArgument(reason)
      case other: Any     => originator ! BadQuery(other.toString)
    }
  }

  def receive: Receive = {
    case q: RawQuery       => handleRawQuery(q)
    case q: AggregateQuery => validateAndGatherAggregates(q)
    case q: SingleShardQuery => singleShardQuery(q)
    case GetIndexNames(ref, limit) =>
      sender() ! memStore.indexNames(ref).take(limit).map(_._1).toBuffer
    case GetIndexValues(ref, index, limit) =>
      // For now, just return values from the first shard
      memStore.activeShards(ref).headOption.foreach { shard =>
        sender() ! memStore.indexValues(ref, shard, index).take(limit).map(_.toString).toBuffer
      }

    case CurrentShardSnapshot(ds, mapper) =>
      logger.info(s"Got initial ShardSnapshot $mapper")
      shardMap = mapper

    case e: ShardEvent =>
      shardMap.updateFromEvent(e)
      logger.debug(s"Received ShardEvent $e, updated to $shardMap")
  }
}