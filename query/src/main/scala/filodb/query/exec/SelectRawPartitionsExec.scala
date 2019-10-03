package filodb.query.exec

import scala.concurrent.duration.FiniteDuration

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.{DatasetRef, Types}
import filodb.core.memstore.PartLookupResult
import filodb.core.metadata.{Column, Schema, Schemas}
import filodb.core.query.{ColumnFilter, ResultSchema}
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

  def optimizeForHistMax(schema: Schema, transformers: Seq[RangeVectorTransformer]): Seq[RangeVectorTransformer] = {
    histMaxColumn(schema, schema.data.columns.map(_.id)).map { maxColID =>
      // Histogram with max column present.  Check for range functions and change if needed
      val origFunc = findFirstRangeFunction(transformers)
      val newFunc = RangeFunction.histMaxRangeFunction(origFunc)
      qLogger.debug(s"Replacing range function for histogram max $origFunc with $newFunc...")
      replaceRangeFunction(transformers, origFunc, newFunc)
    }.getOrElse(transformers)
  }

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
  * ExecPlan to select raw data from partitions that the given filter resolves to,
  * in the given shard, for the given row key range.  Specific to a particular schema.
  * This plan and its transformers should know exactly what kind of data they are dealing with.
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

/**
  * ExecPlan to select raw data from partitions that the given filter resolves to,
  * in the given shard, for the given row key range.  Schema-agnostic - discovers the schema and
  * creates an inner SelectRawPartitionsExec with the right column IDs and other details depending on schema.
  * @param schema an optional schema to filter partitions using
  */
final case class MultiSchemaPartitionsExec(id: String,
                                           submitTime: Long,
                                           limit: Int,
                                           dispatcher: PlanDispatcher,
                                           dataset: DatasetRef,
                                           shard: Int,
                                           filters: Seq[ColumnFilter],
                                           chunkMethod: ChunkScanMethod,
                                           schema: Option[String] = None,
                                           colName: Option[String] = None) extends LeafExecPlan {
  import SelectRawPartitionsExec._

  override def allTransformers: Seq[RangeVectorTransformer] = finalPlan.rangeVectorTransformers

  var finalPlan: ExecPlan = _

  private def finalizePlan(source: ChunkSource): ExecPlan = {
    val partMethod = FilteredPartitionScan(ShardSplit(shard), filters)
    val lookupRes = source.lookupPartitions(dataset, partMethod, chunkMethod)

    // Find the schema if one wasn't supplied
    val schemas = source.schemas(dataset).get
    val dataSchema = schema.map { s => schemas.schemas(s) }
                           .getOrElse(schemas(
                             lookupRes.firstSchemaId.getOrElse(schemas.schemas.values.head.schemaHash)))

    // If we cannot find a schema, or none is provided, we cannot move ahead with specific SRPE planning
    schema.map { s => schemas.schemas(s) }
          .orElse(lookupRes.firstSchemaId.map(schemas.apply))
          .map { sch =>
            val newxformers1 = optimizeForDownsample(sch, rangeVectorTransformers)
            val newxformers = optimizeForHistMax(sch, newxformers1)
            val colIDs1 = getColumnIDs(sch, colName.toSeq, rangeVectorTransformers)
            val colIDs  = addIDsForHistMax(sch, colIDs1)

            val newPlan = SelectRawPartitionsExec(id, submitTime, limit, dispatcher, dataset,
                                                  Some(sch), Some(lookupRes),
                                                  schema.isDefined, colIDs)
            qLogger.debug(s"Discovered schema ${sch.name} and created inner plan $newPlan")
            newxformers.foreach { xf => newPlan.addRangeVectorTransformer(xf) }
            newPlan
          }.getOrElse {
            qLogger.debug(s"No time series found for filters $filters... employing empty plan")
            SelectRawPartitionsExec(id, submitTime, limit, dispatcher, dataset,
                                    None, Some(lookupRes),
                                    schema.isDefined, Nil)
          }
  }

  def doExecute(source: ChunkSource,
                queryConfig: QueryConfig)
               (implicit sched: Scheduler,
                timeout: FiniteDuration): ExecResult = {
     finalPlan = finalizePlan(source)
     finalPlan.doExecute(source, queryConfig)(sched, timeout)
   }

  protected def args: String = s"dataset=$dataset, shard=$shard, " +
                               s"chunkMethod=$chunkMethod, filters=$filters, colName=$colName"
}

