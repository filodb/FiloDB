package filodb.query.exec

import scala.concurrent.duration.FiniteDuration

import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.{DatasetRef, Types}
import filodb.core.metadata.Dataset
import filodb.core.query.{ColumnFilter, RangeVector, ResultSchema}
import filodb.core.store._
import filodb.query.QueryConfig


/**
  * ExecPlan to select raw data from partitions that the given filter resolves to,
  * in the given shard, for the given row key range
  */
final case class SelectRawPartitionsExec(id: String,
                                         submitTime: Long,
                                         limit: Int,
                                         dispatcher: PlanDispatcher,
                                         dataset: DatasetRef,
                                         shard: Int,
                                         filters: Seq[ColumnFilter],
                                         rowKeyRange: RowKeyRange,
                                         colIds: Seq[Types.ColumnId]) extends LeafExecPlan {
  require(colIds.nonEmpty)

  protected def schemaOfDoExecute(dataset: Dataset): ResultSchema =
    ResultSchema(dataset.infosFromIDs(colIds),
      colIds.zip(dataset.rowKeyIDs).takeWhile { case (a, b) => a == b }.length)

  protected def doExecute(source: ChunkSource,
                          dataset: Dataset,
                          queryConfig: QueryConfig)
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Observable[RangeVector] = {
    require(colIds.indexOfSlice(dataset.rowKeyIDs) == 0)

    val chunkMethod = rowKeyRange match {
      case RowKeyInterval(from, to) => RowKeyChunkScan(from, to)
      case AllChunks                => AllChunkScan
      case WriteBuffers             => WriteBufferChunkScan
      case InMemoryChunks           => InMemoryChunkScan
      case EncodedChunks            => ???
    }
    val partMethod = FilteredPartitionScan(ShardSplit(shard), filters)
    source.rangeVectors(dataset, colIds, partMethod, chunkMethod)
  }

  protected def args: String = s"shard=$shard, rowKeyRange=$rowKeyRange, filters=$filters, colIDs=$colIds"
}

