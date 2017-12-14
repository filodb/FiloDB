package filodb.coordinator

import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

import akka.actor.{ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import kamon.Kamon
import kamon.trace.TraceContext
import monix.eval.Task
import monix.reactive.Observable
import org.scalactic._

import filodb.core._
import filodb.core.memstore.MemStore
import filodb.core.metadata.{BadArgument => BadArg, Dataset, WrongNumberArguments}
import filodb.core.query.{AggregationFunction, CombinerFunction, NoTimestampColumn}
import filodb.core.store._
import filodb.memory.format.BinaryVector

object QueryActor {
  private val nextId = new AtomicLong()
  def nextQueryId: Long = nextId.getAndIncrement

  // Internal command for query on each individual node, directed at one shard only
  final case class SingleShardQuery(query: QueryCommands.QueryArgs,
                                    dataset: DatasetRef,
                                    partMethod: PartitionScanMethod,
                                    chunkScan: ChunkScanMethod)

  def props(memStore: MemStore, dataset: Dataset): Props =
    Props(classOf[QueryActor], memStore, dataset)
}

/**
 * Translates external query API calls into internal ColumnStore calls.
 *
 * The actual reading of data structures and aggregation is performed asynchronously by Observables,
 * so it is probably fine for there to be just one QueryActor per dataset.
 */
final class QueryActor(memStore: MemStore,
                       dataset: Dataset) extends BaseActor {
  import OptionSugar._
  import TrySugar._

  import QueryActor._
  import QueryCommands._

  implicit val scheduler = monix.execution.Scheduler(context.dispatcher)
  var shardMap = ShardMapper.default
  val kamonTags = Map("dataset" -> dataset.ref.toString, "shard" -> "multiple")

  def getColumnIDs(colStrs: Seq[String]): Seq[Types.ColumnId] Or ErrorResponse =
    dataset.colIDs(colStrs: _*).badMap(missing => UndefinedColumns(missing.toSet))

  // Returns a list of PartitionScanMethods, one per shard
  def validatePartQuery(partQuery: PartitionQuery,
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
      case KeyRangeQuery(startKey, endKey) => RowKeyChunkScan(dataset, startKey, endKey)
      case MostRecentTime(lastMillis) if dataset.timestampColumn.isDefined =>
        val timeNow = System.currentTimeMillis
        RowKeyChunkScan(dataset, Seq(timeNow - lastMillis), Seq(timeNow))
      case MostRecentSample if dataset.timestampColumn.isDefined => LastSampleChunkScan
    }).toOr.badMap {
      case m: MatchError if dataset.timestampColumn.isEmpty =>
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
    val trace = Kamon.tracer.newContext("raw-query-latency", None, kamonTags)
    val RawQuery(_, colStrs, partQuery, dataQuery) = q
    val originator = sender()
    (for { colIDs      <- getColumnIDs(colStrs)
           partMethods <- validatePartQuery(partQuery, QueryOptions())
           chunkMethod <- validateDataQuery(dataQuery) }
    yield {
      val queryId = nextQueryId
      originator ! QueryInfo(queryId, dataset.ref, colStrs)
      // TODO: this is totally broken if there is more than one partMethod
      memStore.readChunks(dataset, colIDs, partMethods.head, chunkMethod)
        .foreach { reader =>
          val bufs = reader.vectors.map {
            case b: BinaryVector[_] => b.toFiloBuffer
          }
          originator ! QueryRawChunks(queryId, reader.info.id, bufs)
        }
        // NOTE: for some reason Monix's doOnSuccess... has the wrong timing
        .map { Unit => respond(originator, QueryEndRaw(queryId), trace) }
        .recover { case err: Exception => respond(originator, QueryError(queryId, err), trace) }
    }).recover {
      case resp: ErrorResponse => respond(originator, resp, trace)
    }
  }

  // validate high level query params, then send out lower level aggregate queries to shards/coordinators
  // gather them and form an overall response
  def validateAndGatherAggregates(q: AggregateQuery): Unit = {
    val trace = Kamon.tracer.newContext("aggregate-query-latency", None, kamonTags)
    val originator = sender()
    (for { aggFunc    <- validateFunction(q.query.functionName)
           combinerFunc <- validateCombiner(q.query.combinerName)
           chunkMethod <- validateDataQuery(q.query.dataQuery)
           aggregator <- aggFunc.validate(q.query.column, dataset.timestampColumn.map(_.name),
                                          chunkMethod, q.query.args, dataset)
           combiner   <- combinerFunc.validate(aggregator, q.query.combinerArgs)
           partMethods <- validatePartQuery(q.partitionQuery, q.queryOptions) }
    yield {
      val queryId = QueryActor.nextQueryId
      implicit val askTimeout = Timeout(q.queryOptions.queryTimeoutSecs.seconds)
      logger.debug(s"Sending out aggregates $partMethods and combining using $combiner...")
      val results = Observable.fromIterable(partMethods)
                      .mapAsync(q.queryOptions.parallelism) { partMethod =>
                        val coord = shardMap.coordForShard(partMethod.shard)
                        val query = SingleShardQuery(q.query, q.dataset, partMethod, chunkMethod)
                        val future: Future[combiner.C] = (coord ? query).map {
                          case a: combiner.C @unchecked => a
                          case err: ErrorResponse => throw new RuntimeException(err.toString)
                        }
                        Task.fromFuture(future)
                      }
      val combined = if (partMethods.length > 1) results.reduce(combiner.combine) else results
      combined.headL.runAsync
              .map { agg => respond(originator, AggregateResponse(queryId, agg.clazz, agg.result), trace) }
              .recover { case err: Exception =>
                logger.error(s"Error during combining: $err", err)
                respond(originator, QueryError(queryId, err), trace) }
    }).recover {
      case resp: ErrorResponse => respond(originator, resp, trace)
      case WrongNumberArguments(given, expected) => respond(originator, WrongNumberOfArgs(given, expected), trace)
      case BadArg(reason) => respond(originator, BadArgument(reason), trace)
      case NoTimestampColumn =>
        respond(originator, BadQuery(s"Cannot use time-based functions on dataset ${dataset.ref}"), trace)
      case other: Any     => respond(originator, BadQuery(other.toString), trace)
    }
  }

  private def respond(sender: ActorRef, response: Any, trace: TraceContext) = {
    sender ! response
    trace.finish()
  }

  // lower level handling of per-shard aggregate
  def singleShardQuery(q: SingleShardQuery): Unit = {
    // TODO currently each raw/aggregate query translates to multiple single shard queries

    val trace = Kamon.tracer.newContext("single-shard-query-latency", None,
      Map("dataset" -> dataset.ref.toString, "shard" -> q.partMethod.shard.toString))
    val originator = sender()
    (for { aggFunc    <- validateFunction(q.query.functionName)
           combinerFunc <- validateCombiner(q.query.combinerName)
           qSpec = QuerySpec(q.query.column, aggFunc, q.query.args, combinerFunc, q.query.combinerArgs)
           aggregateTask <- memStore.aggregate(dataset, qSpec, q.partMethod, q.chunkScan) }
    yield {
      aggregateTask.runAsync
        .map { agg => respond(originator, agg, trace) }
        .recover { case err: Exception => respond(originator, QueryError(-1, err), trace) }
    }).recover {
      case resp: ErrorResponse => respond(originator, resp, trace)
      case WrongNumberArguments(given, expected) => respond(originator, WrongNumberOfArgs(given, expected), trace)
      case BadArg(reason) => respond(originator, BadArgument(reason), trace)
      case other: Any     => respond(originator, BadQuery(other.toString), trace)
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