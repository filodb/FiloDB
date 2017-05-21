package filodb.coordinator

import akka.actor.Props
import java.util.concurrent.atomic.AtomicLong
import org.scalactic._
import org.velvia.filo.BinaryVector
import scala.language.existentials
import scala.util.Try

import filodb.core._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.memstore.MemStore
import filodb.core.metadata.{Column, RichProjection, BadArgument => BadArg, WrongNumberArguments}
import filodb.core.query.{Aggregate, AggregationFunction}
import filodb.core.store._

object QueryActor {
  private val nextId = new AtomicLong()
  def nextQueryId: Long = nextId.getAndIncrement

  def props(colStore: BaseColumnStore, projection: RichProjection): Props =
    Props(classOf[QueryActor], colStore, projection)
}

/**
 * Translates external query API calls into internal ColumnStore calls.
 *
 * The actual reading of data structures and aggregation is performed asynchronously by Observables,
 * so it is probably fine for there to be just one QueryActor per dataset.
 */
class QueryActor(colStore: BaseColumnStore,
                 projection: RichProjection) extends BaseActor {
  import QueryCommands._
  import TrySugar._
  import OptionSugar._
  import Column.ColumnType._

  implicit val scheduler = monix.execution.Scheduler(context.dispatcher)
  var shardMap = ShardMapper.empty

  private val isTimeSeries = projection.rowKeyColumns.head.columnType match {
    case LongColumn           => true
    case TimestampColumn      => true
    case x: Column.ColumnType => false
  }

  private val memStore: Option[MemStore] = colStore match {
    case m: MemStore => Some(m)
    case o: Any      => None
  }

  def validateColumns(colStrs: Seq[String]): Seq[Column] Or ErrorResponse =
    RichProjection.getColumnsFromNames(projection.columns, colStrs)
                  .badMap {
                    case RichProjection.MissingColumnNames(missing, _) =>
                      NodeClusterActor.UndefinedColumns(missing.toSet)
                    case x: Any =>
                      DatasetCommands.DatasetError(x.toString)
                  }

  def validatePartQuery(partQuery: PartitionQuery): PartitionScanMethod Or ErrorResponse =
    Try(partQuery match {
      case SinglePartitionQuery(keyParts) => SinglePartitionScan(projection.partKey(keyParts:_*))
      case MultiPartitionQuery(keys) =>
        val partKeys = keys.map { k => projection.partKey(k :_*) }
        MultiPartitionScan(partKeys)
      case FilteredPartitionQuery(filters) =>
        // TODO: in the future, parse filters to determine which splits to query
        // Depending on distribution strategy -> or just spread to all nodes
        // For now, just get the single split and scan everything
        val split = colStore.getScanSplits(projection.datasetRef, 1).head
        FilteredPartitionScan(split, filters)
    }).toOr.badMap {
      case m: MatchError => BadQuery(s"Could not parse $partQuery: " + m.getMessage)
      case e: Exception => BadArgument(e.getMessage)
    }

  def rowKeyScan(startKey: Seq[Any], endKey: Seq[Any]): RowKeyChunkScan =
    RowKeyChunkScan(BinaryRecord(projection, startKey), BinaryRecord(projection, endKey))

  def validateDataQuery(dataQuery: DataQuery): ChunkScanMethod Or ErrorResponse = {
    Try(dataQuery match {
      case AllPartitionData =>                AllChunkScan
      case KeyRangeQuery(startKey, endKey) => rowKeyScan(startKey, endKey)
      case MostRecentTime(lastMillis) =>
        require(isTimeSeries, s"Cannot use this query on a non-timeseries schema")
        val timeNow = System.currentTimeMillis
        rowKeyScan(Seq(timeNow - lastMillis), Seq(timeNow))
    }).toOr.badMap {
      case m: MatchError => BadQuery(s"Could not parse $dataQuery: " + m.getMessage)
      case e: Exception => BadArgument(e.getMessage)
    }
  }

  def validateFunction(funcName: String): AggregationFunction Or ErrorResponse =
    AggregationFunction.withNameInsensitiveOption(funcName)
                       .toOr(BadQuery(s"No such aggregation function $funcName"))

  def aggregateOrUserDataQuery(aggregate: Aggregate[_], dataQueryOpt: Option[DataQuery]):
    ChunkScanMethod Or ErrorResponse = aggregate.chunkScan(projection) match {
    // See if aggregate has a chunkScanMethod
    case Some(scanMethod) => Good(scanMethod)
    // If not, get dataQueryOpt and run it through validateDataQuery
    case None =>
      validateDataQuery(dataQueryOpt.getOrElse(AllPartitionData))
  }

  def handleRawQuery(q: RawQuery): Unit = {
    val RawQuery(dataset, version, colStrs, partQuery, dataQuery) = q
    val originator = sender
    (for { colSeq      <- validateColumns(colStrs)
           partMethod  <- validatePartQuery(partQuery)
           chunkMethod <- validateDataQuery(dataQuery) }
    yield {
      val queryId = QueryActor.nextQueryId
      originator ! QueryInfo(queryId, dataset, colSeq.map(_.toString))
      colStore.readChunks(projection, colSeq, version, partMethod, chunkMethod)
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

  def handleAggQuery(q: AggregateQuery): Unit = {
    val originator = sender
    (for { aggFunc    <- validateFunction(q.query.functionName)
           partMethod <- validatePartQuery(q.partitionQuery)
           aggAndIndices <- aggFunc.validate(q.query.args, projection)
           (aggregator, indices) = aggAndIndices
           chunkMethod <- aggregateOrUserDataQuery(aggregator, q.dataQuery) }
    yield {
      val queryId = QueryActor.nextQueryId
      val colSeq = indices.map(projection.columns)
      colStore.readChunks(projection, colSeq, q.version, partMethod, chunkMethod)
        .foldLeftL(aggregator)(_ add _)
        .runAsync
        .map { agg => originator ! AggregateResponse(queryId, agg.clazz, agg.result) }
        .recover { case err: Exception => originator ! QueryError(queryId, err) }
    }).recover {
      case resp: ErrorResponse => originator ! resp
      case WrongNumberArguments(given, expected) => originator ! WrongNumberOfArgs(given, expected)
      case BadArg(reason) => originator ! BadArgument(reason)
    }
  }

  def receive: Receive = {
    case q: RawQuery       => handleRawQuery(q)
    case q: AggregateQuery => handleAggQuery(q)
    case GetIndexNames(ref, limit) if memStore.isDefined =>
      sender ! memStore.get.indexNames(ref).take(limit).toBuffer
    case GetIndexValues(ref, index, limit) if memStore.isDefined =>
      sender ! memStore.get.indexValues(ref, index).take(limit).map(_.toString).toBuffer
    case NodeClusterActor.ShardMapUpdate(ref, newMap) =>
      shardMap = newMap
  }
}