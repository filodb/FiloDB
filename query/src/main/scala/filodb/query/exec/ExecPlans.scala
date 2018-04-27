package filodb.query.exec

import scala.concurrent.duration.FiniteDuration

import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.{DatasetRef, Types}
import filodb.core.metadata.Dataset
import filodb.core.query.{ColumnFilter, RangeVector, ResultSchema}
import filodb.core.store.{AllChunkScan, ChunkSource, FilteredPartitionScan, RowKeyChunkScan, ShardSplit}
import filodb.query._

/**
  * ExecPlan to select raw data from partitions that the given filter resolves to,
  * in the given shard, for the given row key range
  */
final case class SelectRawPartitionsExec(id: String,
                                         dispatcher: PlanDispatcher,
                                         dataset: DatasetRef,
                                         shard: Int,
                                         filters: Seq[ColumnFilter],
                                         rangeSelector: RangeSelector,
                                         columns: Seq[String]) extends LeafExecPlan {

  protected def schemaOfDoExecute(dataset: Dataset): ResultSchema = {
    val colIds = getColumnIDs(dataset, columns)
    ResultSchema(dataset.infosFromIDs(colIds),
      colIds.zip(dataset.rowKeyIDs).takeWhile { case (a, b) => a == b }.length)
  }

  protected def doExecute(source: ChunkSource,
                          dataset: Dataset,
                          queryConfig: QueryConfig)
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Observable[RangeVector] = {
    val colIds = getColumnIDs(dataset, columns)
    require(colIds.indexOfSlice(dataset.rowKeyIDs) == 0)

    val chunkMethod = rangeSelector match {
      case IntervalSelector(from, to) => RowKeyChunkScan(from, to)
      case AllChunksSelector => AllChunkScan
      case WriteBufferSelector => ???
      case EncodedChunksSelector => ???
    }
    val partMethod = FilteredPartitionScan(ShardSplit(shard), filters)
    source.rangeVectors(dataset, colIds, partMethod, dataset.rowKeyOrdering, chunkMethod)
    // TODO limit the number of chunks returned
  }

  /**
    * Convert column name strings into columnIDs.  NOTE: column names should not include row key columns
    * as those are automatically prepended.
    */
  private def getColumnIDs(dataset: Dataset, cols: Seq[String]): Seq[Types.ColumnId] = {
    val ids = dataset.colIDs(cols: _*)
                     .recover(missing => throw new BadQueryException(s"Undefined columns $missing"))
                     .get
    // avoid duplication if first ids are already row keys
    if (ids.take(dataset.rowKeyIDs.length) == dataset.rowKeyIDs) { ids }
    else { dataset.rowKeyIDs ++ ids }
  }

  protected def args: String = s"shard=$shard, rangeSelector=$rangeSelector, filters=$filters"
}

/**
  * Reduce combined aggregates from children. Can be applied in a
  * hierarchical manner multiple times to arrive at result.
  */
final case class ReduceAggregateExec(id: String,
                                     dispatcher: PlanDispatcher,
                                     childAggregates: Seq[ExecPlan],
                                     aggrOp: AggregationOperator,
                                     aggrParams: Seq[Any]) extends NonLeafExecPlan {
  def children: Seq[ExecPlan] = childAggregates

  protected def schemaOfCompose(dataset: Dataset): ResultSchema = ???

  protected def args: String = s"aggrOp=$aggrOp, aggrParams=$aggrParams"

  protected def compose(childResponses: Observable[QueryResponse],
                        queryConfig: QueryConfig): Observable[RangeVector] = ???
}

/**
  * Binary join operator between results of lhs and rhs plan
  */
final case class BinaryJoinExec(id: String,
                                dispatcher: PlanDispatcher,
                                lhs: Seq[ExecPlan],
                                rhs: Seq[ExecPlan],
                                binaryOp: BinaryOperator,
                                on: Seq[String],
                                ignoring: Seq[String]) extends NonLeafExecPlan {
  def children: Seq[ExecPlan] = lhs ++ rhs

  protected def schemaOfCompose(dataset: Dataset): ResultSchema = ???

  protected def args: String = s"binaryOp=$binaryOp, on=$on, ignoring=$ignoring"

  protected def compose(childResponses: Observable[QueryResponse],
                        queryConfig: QueryConfig): Observable[RangeVector] = ???
}

/**
  * Simply concatenate results from child ExecPlan objects
  */
final case class DistConcatExec(id: String,
                                dispatcher: PlanDispatcher,
                                children: Seq[ExecPlan]) extends NonLeafExecPlan {
  require(!children.isEmpty)

  protected def args: String = ""

  protected def schemaOfCompose(dataset: Dataset): ResultSchema = children.head.schema(dataset)

  protected def compose(childResponses: Observable[QueryResponse],
                        queryConfig: QueryConfig): Observable[RangeVector] = {
    childResponses.flatMap {
      case qr: QueryResult => Observable.fromIterable(qr.result)
      case qe: QueryError => throw qe.t
    }
  }
}
