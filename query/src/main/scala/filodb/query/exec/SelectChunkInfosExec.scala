package filodb.query.exec

import scala.concurrent.duration.FiniteDuration

import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.{DatasetRef, Types}
import filodb.core.memstore.TimeSeriesShard
import filodb.core.metadata.{Column, Schema}
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
                                      dataSchema: Schema,
                                      filters: Seq[ColumnFilter],
                                      chunkMethod: ChunkScanMethod,
                                      column: Types.ColumnId) extends LeafExecPlan {
  import SelectChunkInfosExec._

  protected def schemaOfDoExecute(): ResultSchema = ChunkInfosSchema

  protected def doExecute(source: ChunkSource,
                          queryConfig: QueryConfig)
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Observable[RangeVector] = {
    val dataColumn = dataSchema.data.columns(column)
    val partMethod = FilteredPartitionScan(ShardSplit(shard), filters)
    val partCols = dataSchema.partitionInfos
    val numGroups = source.groupsInDataset(dataset)
    source.scanPartitions(dataset, Seq(column), partMethod, chunkMethod)
          .filter(_.hasChunks(chunkMethod))
          .map { partition =>
            source.stats.incrReadPartitions(1)
            val subgroup = TimeSeriesShard.partKeyGroup(dataSchema.partKeySchema, partition.partKeyBase,
                                                        partition.partKeyOffset, numGroups)
            val key = new PartitionRangeVectorKey(partition.partKeyBase, partition.partKeyOffset,
                                                  dataSchema.partKeySchema, partCols, shard, subgroup, partition.partID)
            ChunkInfoRangeVector(key, partition, chunkMethod, dataColumn)
          }
  }

  protected def args: String = s"shard=$shard, chunkMethod=$chunkMethod, filters=$filters, col=$column"
}

