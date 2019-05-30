package filodb.query.exec

import scala.concurrent.duration.FiniteDuration

import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.{DatasetRef, Types}
import filodb.core.metadata.{Column, Dataset}
import filodb.core.query.{ColumnFilter, RangeVector, ResultSchema}
import filodb.core.store._
import filodb.query.QueryConfig

object SelectRawPartitionsExec {
  import Column.ColumnType._

  // Returns Some(colID) the ID of a "max" column if one of given colIDs is a histogram.
  def histMaxColumn(dataset: Dataset, colIDs: Seq[Types.ColumnId]): Option[Int] = {
    colIDs.find { id => dataset.dataColumns(id).columnType == HistogramColumn }
          .flatMap { histColID =>
            dataset.dataColumns.find { c => c.name == "max" && c.columnType == DoubleColumn }
          }.map(_.id)
  }
}

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
                                         chunkMethod: ChunkScanMethod,
                                         colIds: Seq[Types.ColumnId]) extends LeafExecPlan {
  import SelectRawPartitionsExec._

  require(colIds.nonEmpty)

  protected[filodb] def schemaOfDoExecute(dataset: Dataset): ResultSchema = {
    val numRowKeyCols = colIds.zip(dataset.rowKeyIDs).takeWhile { case (a, b) => a == b }.length

    // Add the max column to the schema together with Histograms for max computation -- just in case it's needed
    // But make sure the max column isn't already included
    histMaxColumn(dataset, colIds).filter { mId => !(colIds contains mId) }
                                  .map { maxColId =>
      ResultSchema(dataset.infosFromIDs(colIds :+ maxColId), numRowKeyCols, colIDs = (colIds :+ maxColId))
    }.getOrElse {
      ResultSchema(dataset.infosFromIDs(colIds), numRowKeyCols, colIDs = colIds)
    }
  }

  protected def doExecute(source: ChunkSource,
                          dataset: Dataset,
                          queryConfig: QueryConfig)
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Observable[RangeVector] = {
    require(colIds.indexOfSlice(dataset.rowKeyIDs) == 0)

    val partMethod = FilteredPartitionScan(ShardSplit(shard), filters)
    source.rangeVectors(dataset, colIds, partMethod, chunkMethod)
  }

  protected def args: String = s"shard=$shard, chunkMethod=$chunkMethod, filters=$filters, colIDs=$colIds"
}

