package filodb.query.exec

import scala.concurrent.duration.FiniteDuration

import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.{DatasetRef, Types}
import filodb.core.memstore.TimeSeriesShard
import filodb.core.metadata.{Column, Dataset}
import filodb.core.query._
import filodb.core.store._
import filodb.query.QueryConfig

object SelectChunkInfosExec {
  import Column.ColumnType._

  val ChunkInfosSchema = ResultSchema(
    Seq(
      ColumnInfo("id", LongColumn),
      ColumnInfo("numRows", IntColumn),
      ColumnInfo("startTime", LongColumn),
      ColumnInfo("endTime", LongColumn),
      ColumnInfo("numBytes", IntColumn),
      ColumnInfo("readerKlazz", StringColumn)
    ), 0
  )
}

/**
  * ExecPlan to select raw ChunkInfos and chunk stats from partitions that the given filter resolves to,
  * in the given shard, for the given row key range, for one particular column
  * ID (Long), NumRows (Int), startTime (Long), endTime (Long), numBytes(I) of chunk, readerclass of chunk
  */
final case class SelectChunkInfosExec(id: String,
                                      submitTime: Long,
                                      limit: Int,
                                      dispatcher: PlanDispatcher,
                                      dataset: DatasetRef,
                                      shard: Int,
                                      filters: Seq[ColumnFilter],
                                      rowKeyRange: RowKeyRange,
                                      column: Types.ColumnId) extends LeafExecPlan {
  import SelectChunkInfosExec._

  protected def schemaOfDoExecute(dataset: Dataset): ResultSchema = ChunkInfosSchema

  protected def doExecute(source: ChunkSource,
                          dataset: Dataset,
                          queryConfig: QueryConfig)
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Observable[RangeVector] = {
    val dataColumn = dataset.dataColumns(column)
    val chunkMethod = rowKeyRange match {
      case RowKeyInterval(from, to) => RowKeyChunkScan(from, to)
      case AllChunks                => AllChunkScan
      case WriteBuffers             => WriteBufferChunkScan
      case InMemoryChunks           => InMemoryChunkScan
      case EncodedChunks            => ???
    }
    val partMethod = FilteredPartitionScan(ShardSplit(shard), filters)
    val partCols = dataset.infosFromIDs(dataset.partitionColumns.map(_.id))
    val numGroups = source.groupsInDataset(dataset)
    source.scanPartitions(dataset, Seq(column), partMethod, chunkMethod)
          .filter(_.hasChunks(chunkMethod))
          .map { partition =>
            source.stats.incrReadPartitions(1)
            val subgroup = TimeSeriesShard.partKeyGroup(dataset.partKeySchema, partition.partKeyBase,
                                                        partition.partKeyOffset, numGroups)
            val key = new PartitionRangeVectorKey(partition.partKeyBase, partition.partKeyOffset,
                                                  dataset.partKeySchema, partCols, shard, subgroup)
            ChunkInfoRangeVector(key, partition, chunkMethod, dataColumn)
          }
  }

  protected def args: String = s"shard=$shard, rowKeyRange=$rowKeyRange, filters=$filters, col=$column"
}

