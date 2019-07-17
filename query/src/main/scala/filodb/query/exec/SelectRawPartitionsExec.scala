package filodb.query.exec

import scala.concurrent.duration.FiniteDuration

import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.{DatasetRef, Types}
import filodb.core.metadata.{Column, Dataset}
import filodb.core.query.{ColumnFilter, RangeVector, ResultSchema}
import filodb.core.store._
import filodb.query.{Query, QueryConfig}

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

  protected[filodb] def schemaOfDoExecute(dataset: Dataset): ResultSchema = {
    val selectedColIds = selectColIds(dataset)
    val numRowKeyCols = selectedColIds.zip(dataset.rowKeyIDs).takeWhile { case (a, b) => a == b }.length

    // Add the max column to the schema together with Histograms for max computation -- just in case it's needed
    // But make sure the max column isn't already included
    histMaxColumn(dataset, selectedColIds).filter { mId => !(colIds contains mId) }
                                  .map { maxColId =>
      ResultSchema(dataset.infosFromIDs(selectedColIds :+ maxColId),
        numRowKeyCols, colIDs = (selectedColIds :+ maxColId))
    }.getOrElse {
      ResultSchema(dataset.infosFromIDs(selectedColIds), numRowKeyCols, colIDs = selectedColIds)
    }
  }

  private def selectColIds(dataset: Dataset) = {
    dataset.rowKeyIDs ++ {
      if (colIds.nonEmpty) {
        // query is selecting specific columns
        colIds
      } else if (!dataset.hasDownsampledData) {
        // needs to select raw data
        colIds ++ dataset.colIDs(dataset.options.valueColumn).get
      } else {
        // need to select column based on range function
        val colNames = rangeVectorTransformers.find(_.isInstanceOf[PeriodicSamplesMapper]).map { p =>
          PeriodicSamplesMapper.downsampleColsFromRangeFunction(p.asInstanceOf[PeriodicSamplesMapper].functionId)
        }.getOrElse(Seq("avg"))
        colIds ++ dataset.colIDs(colNames: _*).get
      }
    }
  }

  protected def doExecute(source: ChunkSource,
                          dataset: Dataset,
                          queryConfig: QueryConfig)
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Observable[RangeVector] = {
    require(dataset.rowKeyIDs.forall(rk => !colIds.contains(rk)),
      "User selected columns should not include timestamp (row-key); it will be auto-prepended")

    val partMethod = FilteredPartitionScan(ShardSplit(shard), filters)
    val selectCols = selectColIds(dataset)
    Query.qLogger.debug(s"queryId=$id on dataset=${dataset.ref} shard=$shard" +
      s" is configured to use column=$selectCols to serve downsampled results")
    source.rangeVectors(dataset, selectCols, partMethod, chunkMethod)
  }

  protected def args: String = s"shard=$shard, chunkMethod=$chunkMethod, filters=$filters, colIDs=$colIds"
}

