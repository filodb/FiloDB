package filodb.query.exec

import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler

import filodb.core.DatasetRef
import filodb.core.memstore.TimeSeriesShard
import filodb.core.metadata.Column
import filodb.core.query._
import filodb.core.store._

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
final case class SelectChunkInfosExec(queryContext: QueryContext,
                                      dispatcher: PlanDispatcher,
                                      dataset: DatasetRef,
                                      shard: Int,
                                      filters: Seq[ColumnFilter],
                                      chunkMethod: ChunkScanMethod,
                                      schema: Option[String] = None,
                                      colName: Option[String] = None) extends LeafExecPlan {
  import SelectChunkInfosExec._

  def doExecute(source: ChunkSource,
                querySession: QuerySession)
               (implicit sched: Scheduler): ExecResult = {
    val partMethod = FilteredPartitionScan(ShardSplit(shard), filters)
    val lookupRes = source.lookupPartitions(dataset, partMethod, chunkMethod, querySession)

    val schemas = source.schemas(dataset).get
    val dataSchema = schema.map { s => schemas.schemas(s) }
                           .getOrElse(schemas(lookupRes.firstSchemaId.get))
    val colID = colName.map(n => dataSchema.colIDs(n).get.head).getOrElse(dataSchema.data.valueColumn)
    val dataColumn = dataSchema.data.columns(colID)
    val partCols = dataSchema.partitionInfos
    val numGroups = source.groupsInDataset(dataset)
    Kamon.currentSpan().mark("creating-scanpartitions")
    val rvs = source.scanPartitions(dataset, lookupRes, Seq.empty, querySession)
          .filter(_.hasChunks(chunkMethod))
          .map { partition =>
            source.stats.incrReadPartitions(1)
            val subgroup = TimeSeriesShard.partKeyGroup(dataSchema.partKeySchema, partition.partKeyBase,
                                                        partition.partKeyOffset, numGroups)
            val key = new PartitionRangeVectorKey(partition.partKeyBase, partition.partKeyOffset,
                                                  dataSchema.partKeySchema, partCols, shard,
                                                  subgroup, partition.partID, dataSchema.name)
            ChunkInfoRangeVector(key, partition, chunkMethod, dataColumn)
          }
    ExecResult(rvs, Task.eval(ChunkInfosSchema))
  }

  protected def args: String = s"shard=$shard, chunkMethod=$chunkMethod, filters=$filters, col=$colName"
}

