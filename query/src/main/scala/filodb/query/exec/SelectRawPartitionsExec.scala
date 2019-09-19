package filodb.query.exec

import scala.concurrent.duration.FiniteDuration

import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.{DatasetRef, Types}
import filodb.core.metadata.{Column, Schema, Schemas}
import filodb.core.query.{ColumnFilter, RangeVector, ResultSchema}
import filodb.core.store._
import filodb.query.{Query, QueryConfig, RangeFunctionId}
import filodb.query.exec.rangefn.RangeFunction
import filodb.query.Query.qLogger

object SelectRawPartitionsExec extends  {
  import Column.ColumnType._

  // Returns Some(colID) the ID of a "max" column if one of given colIDs is a histogram.
  def histMaxColumn(schema: Schema, colIDs: Seq[Types.ColumnId]): Option[Int] = {
    colIDs.find { id => schema.data.columns(id).columnType == HistogramColumn }
          .flatMap { histColID =>
            schema.data.columns.find(c => c.name == "max"  && c.columnType == DoubleColumn).map(_.id)
          }
  }

  def findFirstRangeFunction(transformers: Seq[RangeVectorTransformer]): Option[RangeFunctionId] =
    transformers.collect { case p: PeriodicSamplesMapper => p.functionId }.headOption.flatten

  def replaceRangeFunction(transformers: Seq[RangeVectorTransformer],
                           oldFunc: Option[RangeFunctionId],
                           newFunc: Option[RangeFunctionId]): Seq[RangeVectorTransformer] =
    transformers.map {
      case p: PeriodicSamplesMapper if p.functionId == oldFunc => p.copy(functionId = newFunc)
      case other: RangeVectorTransformer => other
    }

  // Optimize transformers, replacing range functions for downsampled gauge queries if needed
  def optimizeForDownsample(schema: Schema, transformers: Seq[RangeVectorTransformer]): Seq[RangeVectorTransformer] = {
    if (schema == Schemas.dsGauge) {
      val origFunc = findFirstRangeFunction(transformers)
      val newFunc = RangeFunction.downsampleRangeFunction(origFunc)
      qLogger.debug(s"Replacing range function $origFunc with $newFunc...")
      replaceRangeFunction(transformers, origFunc, newFunc)
    } else {
      transformers.toBuffer   // Produce an immutable copy, so the original one is not mutated by accident
    }
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
                                         datasetRef: DatasetRef,
                                         shard: Int,
                                         dataSchema: Schema,
                                         filters: Seq[ColumnFilter],
                                         chunkMethod: ChunkScanMethod,
                                         colIds: Seq[Types.ColumnId]) extends LeafExecPlan {
  import SelectRawPartitionsExec._

  def dataset: DatasetRef = datasetRef

  protected[filodb] def schemaOfDoExecute(): ResultSchema = {
    require(!colIds.contains(0),
      "User selected columns should not include timestamp (row-key); it will be auto-prepended")

    val selectedColIds = selectColIds()
    val numRowKeyCols = 1 // hardcoded since a future PR will indeed fix this to 1 timestamp column

    // Add the max column to the schema together with Histograms for max computation -- just in case it's needed
    // But make sure the max column isn't already included
    histMaxColumn(dataSchema, selectedColIds).filter { mId => !(colIds contains mId) }
                                  .map { maxColId =>
      ResultSchema(dataSchema.infosFromIDs(selectedColIds :+ maxColId),
        numRowKeyCols, colIDs = (selectedColIds :+ maxColId))
    }.getOrElse {
      ResultSchema(dataSchema.infosFromIDs(selectedColIds), numRowKeyCols, colIDs = selectedColIds)
    }
  }

  private def selectColIds() = {
    Schemas.rowKeyIDs ++ {
      if (colIds.nonEmpty) {
        // query is selecting specific columns
        colIds
      } else if (!(dataSchema == Schemas.dsGauge)) {
        // needs to select raw data
        colIds ++ dataSchema.colIDs(dataSchema.data.valueColName).get
      } else {
        // need to select column based on range function for gauge schema
        val colNames = rangeVectorTransformers.find(_.isInstanceOf[PeriodicSamplesMapper]).map { p =>
          RangeFunction.downsampleColsFromRangeFunction(dataSchema, p.asInstanceOf[PeriodicSamplesMapper].functionId)
        }.getOrElse(Seq(dataSchema.data.valueColName))
        colIds ++ dataSchema.colIDs(colNames: _*).get
      }
    }
  }

  protected def doExecute(source: ChunkSource,
                          queryConfig: QueryConfig)
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Observable[RangeVector] = {
    require(!colIds.contains(0),
      "User selected columns should not include timestamp (row-key); it will be auto-prepended")

    // Optimize/adjust transformers for downsampling queries.
    replaceTransformers(optimizeForDownsample(dataSchema, rangeVectorTransformers))

    val partMethod = FilteredPartitionScan(ShardSplit(shard), filters)
    val selectCols = selectColIds()
    Query.qLogger.debug(s"queryId=$id on dataset=$datasetRef shard=$shard schema=${dataSchema.name}" +
      s" is configured to use column=$selectCols to serve downsampled results")
    source.rangeVectors(datasetRef, dataSchema, selectCols, partMethod, chunkMethod)
  }

  protected def args: String = s"dataset=$dataset, shard=$shard, schema=${dataSchema.name}, " +
                               s"chunkMethod=$chunkMethod, filters=$filters, colIDs=$colIds"
}

