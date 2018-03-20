package filodb.coordinator

import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration._
import scala.util.control.NonFatal

import akka.actor.{ActorRef, Props}
import akka.util.Timeout
import kamon.Kamon
import monix.eval.Task
import org.scalactic._

import filodb.core._
import filodb.core.binaryrecord.{BinaryRecord, RecordSchema}
import filodb.core.memstore.MemStore
import filodb.core.metadata.{BadArgument => BadArg, Column, Dataset, WrongNumberArguments}
import filodb.core.query._
import filodb.core.store._
import filodb.memory.MemFactory
import filodb.memory.format.{Classes, SeqRowReader}
import filodb.memory.format.vectors.{DoubleVector, IntBinaryVector}

object QueryActor {
  private val nextId = new AtomicLong()
  def nextQueryId: Long = nextId.getAndIncrement

  // Internal command for query on each individual node, directed at one shard only
  final case class SingleShardQuery(query: client.QueryCommands.QueryArgs,
                                    dataset: DatasetRef,
                                    partMethod: PartitionScanMethod,
                                    chunkScan: ChunkScanMethod)

  def props(memStore: MemStore, dataset: Dataset): Props =
    Props(new QueryActor(memStore, dataset))
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

  import QueryActor._
  import client.LogicalPlan
  import client.LogicalPlan._
  import client.QueryCommands._
  import queryengine.Engine
  import queryengine.Utils._
  import Column.ColumnType._

  implicit val scheduler = monix.execution.Scheduler(context.dispatcher)
  var shardMap = ShardMapper.default
  val config = context.system.settings.config

  def validateFunction(funcName: String): AggregationFunction Or ErrorResponse =
    AggregationFunction.withNameInsensitiveOption(funcName)
                       .toOr(BadQuery(s"No such aggregation function $funcName"))

  def validateCombiner(combinerName: String): CombinerFunction Or ErrorResponse =
    CombinerFunction.withNameInsensitiveOption(combinerName)
                    .toOr(BadQuery(s"No such combiner function $combinerName"))

  // validate high level query params, then send out lower level aggregate queries to shards/coordinators
  // gather them and form an overall response
  // NOTE: this is deprecated, it is only kept around for the old aggregate-combine query pipeline
  // TODO: mvoe this into the new Logical/Physical plan machinery soon
  def validateAndGatherAggregates(args: QueryArgs,
                                  partQuery: PartitionQuery,
                                  options: QueryOptions): Unit = {
    val originator = sender()
    (for { aggFunc    <- validateFunction(args.functionName)
           combinerFunc <- validateCombiner(args.combinerName)
           chunkMethod <- validateDataQuery(dataset, args.dataQuery)
           aggregator <- aggFunc.validate(args.column, dataset.timestampColumn.map(_.name),
                                          chunkMethod, args.args, dataset)
           combiner   <- combinerFunc.validate(aggregator, args.combinerArgs)
           partMethods <- validatePartQuery(dataset, shardMap, partQuery, options) }
    yield {
      val queryId = QueryActor.nextQueryId
      implicit val askTimeout = Timeout(options.queryTimeoutSecs.seconds)
      logger.debug(s"Sending out aggregates $partMethods and combining using $combiner...")
      val results = scatterGather[combiner.C](shardMap, partMethods, options.parallelism) { method =>
                      SingleShardQuery(args, dataset.ref, method, chunkMethod)
                    }
      val combined = if (partMethods.length > 1) results.reduce(combiner.combine) else results
      combined.headL.runAsync
              .map { agg => originator ! aggToResult(queryId, agg) }
              .recover { case err: Exception =>
                logger.error(s"Error during combining: $err", err)
                originator ! QueryError(queryId, err) }
    }).recover {
      case resp: ErrorResponse => originator ! resp
      case WrongNumberArguments(given, expected) => originator ! WrongNumberOfArgs(given, expected)
      case BadArg(reason) => originator ! BadArgument(reason)
      case NoTimestampColumn =>
        originator ! BadQuery(s"Cannot use time-based functions on dataset ${dataset.ref}")
      case other: Any     => originator ! BadQuery(other.toString)
    }
  }

  val HistClass = classOf[HistogramBucket]

  // TEMPORARY: TODO: remove when old combine-aggregate pipeline is removed
  private def aggToResult(queryId: Long, agg: Aggregate[_]): QueryResult = {
    val result = agg.clazz match {
      case Classes.Double if agg.result.size == 1 =>
        val bRec = BinaryRecord(RecordSchema(DoubleColumn), SeqRowReader(Seq(agg.result(0))))
        val resultCols = Seq(ColumnInfo("result", DoubleColumn))
        TupleResult(ResultSchema(resultCols, 0), Tuple(None, bRec))
      // TODO: IF the result has multiple values
      case Classes.Double =>
        val doubles = DoubleVector(MemFactory.onHeapFactory, agg.result.toSeq.asInstanceOf[Seq[Double]])
        val partVector = PartitionVector(ChunkSetReader.fromVectors(Array(doubles)))
        val resultCols = Seq(ColumnInfo("result", DoubleColumn))
        VectorResult(ResultSchema(resultCols, 0), partVector)
      case Classes.Int    =>
        val bRec = BinaryRecord(RecordSchema(IntColumn), SeqRowReader(Seq(agg.result(0))))
        val resultCols = Seq(ColumnInfo("result", IntColumn))
        TupleResult(ResultSchema(resultCols, 0), Tuple(None, bRec))
        // TODO ok get the result here
      case HistClass      =>
        val buckets = agg.result.toSeq.asInstanceOf[Seq[HistogramBucket]]
        val counts = IntBinaryVector(MemFactory.onHeapFactory, buckets.map(_.count))  // call optimize()
        val bucketMax = DoubleVector(MemFactory.onHeapFactory, buckets.map(_.max))
        val partVector = PartitionVector(ChunkSetReader.fromVectors(Array(counts, bucketMax)))
        val resultCols = Seq(ColumnInfo("counts", IntColumn), ColumnInfo("bucketMax", DoubleColumn))
        VectorResult(ResultSchema(resultCols, 0), partVector)
    }
    QueryResult(queryId, result)
  }

  /**
   * Materializes the given Task by running it, firing a QueryResult to the originator, or if the task
   * results in an Exception, then responding with a QueryError.
   */
  private def respond(originator: ActorRef, task: Task[Result]): Unit = {
    val queryId = nextQueryId
    task.runAsync
      .map { res =>
        logger.debug(s"Result obtained from plan execution: $res")
        originator ! QueryResult(queryId, res)
      }.recover { case NonFatal(err) => originator ! QueryError(queryId, err) }
  }

  // lower level handling of per-shard aggregate
  def singleShardQuery(q: SingleShardQuery): Unit = {
    // TODO currently each raw/aggregate query translates to multiple single shard queries

    val originator = sender()
    (for { aggFunc    <- validateFunction(q.query.functionName)
           combinerFunc <- validateCombiner(q.query.combinerName)
           qSpec = QuerySpec(q.query.column, aggFunc, q.query.args, combinerFunc, q.query.combinerArgs)
           aggregateTask <- memStore.aggregate(dataset, qSpec, q.partMethod, q.chunkScan) }
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

  // Validate and convert "raw" logical plans into physical plan
  def validateRawQuery(partQuery: PartitionQuery,
                       dataQuery: DataQuery,
                       columns: Seq[String],
                       options: QueryOptions): Unit = {
    val originator = sender()
    (for { colIDs      <- getColumnIDs(dataset, columns)
           chunkMethod <- validateDataQuery(dataset, dataQuery)
           partMethods <- validatePartQuery(dataset, shardMap, partQuery, options) }
    yield {
      // Use distributeConcat to scatter gather Vectors or Tuples from each shard
      implicit val askTimeout = Timeout(options.queryTimeoutSecs.seconds)
      val execPlan = dataQuery match {
        case MostRecentSample =>
          Engine.DistributeConcat(partMethods, shardMap, options.parallelism, options.itemLimit) { method =>
            ExecPlan.streamLastTuplePlan(dataset, colIDs, method)
          }
        case _ =>
          Engine.DistributeConcat(partMethods, shardMap, options.parallelism, options.itemLimit) { method =>
            new ExecPlan.LocalVectorReader(colIDs, method, chunkMethod)
          }
      }
      logger.debug(s"Translated raw query for $partQuery, $dataQuery [$columns] into plan:\n$execPlan")

      // In the future, the step below will be common to all queries and can be moved out
      // For now kick start physical plan execution by sending a message to myself
      // NOTE: forward originator so we can respond directly to it
      self.tell(ExecPlanQuery(dataset.ref, execPlan, options.itemLimit), originator)
    }).recover {
      case resp: ErrorResponse => originator ! resp
    }
  }

  def execPhysicalPlan(physQuery: ExecPlanQuery, originator: ActorRef): Unit =
    respond(originator, Engine.execute(physQuery.execPlan, dataset, memStore, physQuery.limit))

  // This is only temporary, before the QueryEngine and optimizer is really flushed out.
  // Parse the query LogicalPlan and carry out actions
  // In the future, the optimizer will translate these plans into physical plans.  Validation done below would
  // need to be done by the Optimizer/Planner too.
  def parseQueryPlan(q: LogicalPlanQuery, originator: ActorRef): Unit = {
    logger.debug(s"Parsing query $q")
    q.plan match {
      case PartitionsInstant(partQuery, cols) =>
        // TODO: extract the last value of every vector only. OR, report a time range for the single value aggregate
        validateRawQuery(partQuery, MostRecentSample, cols, q.queryOptions)
      case PartitionsRange(partQuery, dataQuery, cols) =>
        validateRawQuery(partQuery, dataQuery, cols, q.queryOptions)

      // Right now everything else fits the combiner/aggregator pattern below
      case ReducePartitions(combFunc, combArgs,
             ReduceEach(aggFunc, aggArgs,
               PartitionsRange(partQuery, dataQuery, cols))) =>
        if (cols.length != 1) { originator ! BadQuery(s"Only one column should be specified, but got $cols") }
        else {
          val args = QueryArgs(aggFunc, cols(0), aggArgs, dataQuery, combFunc, combArgs)
          validateAndGatherAggregates(args, partQuery, q.queryOptions)
        }

      // Translate something with only ReduceEach and no ReducePartitions. This is just temporary
      case ReduceEach(aggFunc, aggArgs,
             PartitionsRange(partQuery, dataQuery, cols)) =>
        if (cols.length != 1) { originator ! BadQuery(s"Only one column should be specified, but got $cols") }
        else {
          val args = QueryArgs(aggFunc, cols(0), aggArgs, dataQuery)
          validateAndGatherAggregates(args, partQuery, q.queryOptions)
        }

      case other: LogicalPlan =>
        originator ! BadQuery(s"Unsupported logical plan $other")
    }
  }

  def receive: Receive = {
    case q: LogicalPlanQuery       => Kamon.currentSpan().tag("LogicalPlanQuery", q.toString)
                                      parseQueryPlan(q, sender())
    case q: ExecPlanQuery          => Kamon.currentSpan().tag("ExecPlanQuery", q.toString)
                                      execPhysicalPlan(q, sender())
    case q: SingleShardQuery       => Kamon.currentSpan().tag("SingleShardQuery", q.toString)
                                      singleShardQuery(q)
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