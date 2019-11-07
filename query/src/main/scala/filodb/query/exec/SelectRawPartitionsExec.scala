package filodb.query.exec

import scala.concurrent.duration.FiniteDuration

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.{DatasetRef, Types}
import filodb.core.memstore.PartLookupResult
import filodb.core.metadata.{Column, Schema, Schemas}
import filodb.core.query.ResultSchema
import filodb.core.store._
import filodb.query.{Query, QueryConfig}
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

  def findFirstRangeFunction(transformers: Seq[RangeVectorTransformer]): Option[InternalRangeFunction] =
    transformers.collect { case p: PeriodicSamplesMapper => p.functionId }.headOption.flatten

  def replaceRangeFunction(transformers: Seq[RangeVectorTransformer],
                           oldFunc: Option[InternalRangeFunction],
                           newFunc: Option[InternalRangeFunction]): Seq[RangeVectorTransformer] =
    transformers.map {
      case p: PeriodicSamplesMapper if p.functionId == oldFunc => p.copy(functionId = newFunc)
      case other: RangeVectorTransformer => other
    }

  // Transform transformers, replacing range functions for downsampled gauge queries if needed
  def newXFormersForDownsample(schema: Schema,
                               transformers: Seq[RangeVectorTransformer]): Seq[RangeVectorTransformer] = {
    if (schema == Schemas.dsGauge) {
      val origFunc = findFirstRangeFunction(transformers)
      val newFunc = RangeFunction.downsampleRangeFunction(origFunc)
      qLogger.debug(s"Replacing range function $origFunc with $newFunc...")
      replaceRangeFunction(transformers, origFunc, newFunc)
    } else {
      transformers.toBuffer   // Produce an immutable copy, so the original one is not mutated by accident
    }
  }

  def newXFormersForHistMax(schema: Schema, transformers: Seq[RangeVectorTransformer]): Seq[RangeVectorTransformer] = {
    histMaxColumn(schema, schema.data.columns.map(_.id)).map { maxColID =>
      // Histogram with max column present.  Check for range functions and change if needed
      val origFunc = findFirstRangeFunction(transformers)
      val newFunc = RangeFunction.histMaxRangeFunction(origFunc)
      qLogger.debug(s"Replacing range function for histogram max $origFunc with $newFunc...")
      replaceRangeFunction(transformers, origFunc, newFunc)
    }.getOrElse(transformers)
  }

  // Given optional column names, find the column IDs, adjusting for the schema
  def getColumnIDs(dataSchema: Schema,
                   colNames: Seq[String],
                   transformers: Seq[RangeVectorTransformer]): Seq[Types.ColumnId] = {
    Schemas.rowKeyIDs ++ {
      if (colNames.nonEmpty) {
        // query is selecting specific columns
        dataSchema.colIDs(colNames: _*).get
      } else if (!(dataSchema == Schemas.dsGauge)) {
        // needs to select raw data
        dataSchema.colIDs(dataSchema.data.valueColName).get
      } else {
        // need to select column based on range function for gauge schema
        val names = findFirstRangeFunction(transformers).map { func =>
          RangeFunction.downsampleColsFromRangeFunction(dataSchema, Some(func))
        }.getOrElse(Seq(dataSchema.data.valueColName))

        dataSchema.colIDs(names: _*).get
      }
    }
  }

  // Automatically add column ID for max column if it exists and we picked histogram col already
  // But make sure the max column isn't already included
  def addIDsForHistMax(dataSchema: Schema, colIDs: Seq[Types.ColumnId]): Seq[Types.ColumnId] =
    histMaxColumn(dataSchema, colIDs).filter { mId => !(colIDs contains mId) }
      .map { maxColID =>
        colIDs :+ maxColID
      }.getOrElse(colIDs)
}

/**
  * ExecPlan to select raw data from partitions.  Specific to a particular schema.
  * This plan should be created with transformers and column IDs exactly as needed to query data given the
  * schema. It does not perform any magic to transform column IDs or range functions further.
  * This plan and its transformers should know exactly what kind of data they are dealing with.
  * Note that it needs to be a separate ExecPlan as its transformers might have been modified by the parent ExecPlan.
  *
  * @param dataSchema normally Some(schema), but may be None if schema cannot be discovered.
  * @param filterSchemas if true, filter the partitions for the given schema.
  *                      if false, the given schema is expected for all partitions, error will be thrown otherwise.
  * @param colIds the exact column IDs that are needed for querying the raw data
  */
final case class SelectRawPartitionsExec(id: String,
                                         submitTime: Long,
                                         limit: Int,
                                         dispatcher: PlanDispatcher,
                                         datasetRef: DatasetRef,
                                         dataSchema: Option[Schema],
                                         lookupRes: Option[PartLookupResult],
                                         filterSchemas: Boolean,
                                         colIds: Seq[Types.ColumnId]) extends LeafExecPlan {
  def dataset: DatasetRef = datasetRef

  private def schemaOfDoExecute(): ResultSchema = {
    // If the data schema cannot be found, then return empty ResultSchema
    dataSchema.map { sch =>
      val numRowKeyCols = 1
      ResultSchema(sch.infosFromIDs(colIds), numRowKeyCols, colIDs = colIds)
    }.getOrElse(ResultSchema.empty)
  }

  def doExecute(source: ChunkSource,
                queryConfig: QueryConfig)
               (implicit sched: Scheduler,
                timeout: FiniteDuration): ExecResult = {
    Query.qLogger.debug(s"queryId=$id on dataset=$datasetRef shard=${lookupRes.map(_.shard).getOrElse("")}" +
      s"schema=${dataSchema.map(_.name)} is configured to use columnIDs=$colIds")
    val rvs = dataSchema.map { sch =>
      source.rangeVectors(datasetRef, lookupRes.get, colIds, sch, filterSchemas)
    }.getOrElse(Observable.empty)
    ExecResult(rvs, Task.eval(schemaOfDoExecute()))
  }

  protected def args: String = s"dataset=$dataset, shard=${lookupRes.map(_.shard).getOrElse(-1)}, " +
                               s"schema=${dataSchema.map(_.name)}, filterSchemas=$filterSchemas, " +
                               s"chunkMethod=${lookupRes.map(_.chunkMethod).getOrElse("")}, colIDs=$colIds"
}
