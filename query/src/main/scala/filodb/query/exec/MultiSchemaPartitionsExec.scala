package filodb.query.exec

import scala.concurrent.duration.FiniteDuration

import monix.execution.Scheduler

import filodb.core.DatasetRef
import filodb.core.metadata.Schemas
import filodb.core.query.ColumnFilter
import filodb.core.store._
import filodb.query.Query.qLogger
import filodb.query.QueryConfig

final case class UnknownSchemaQueryErr(id: Int) extends
  Exception(s"Unknown schema ID $id during query.  This likely means a schema config change happened and " +
            "the partitionkeys tables were not truncated.")

/**
  * ExecPlan to select raw data from partitions that the given filter resolves to,
  * in the given shard, for the given row key range.  Schema-agnostic - discovers the schema and
  * creates an inner SelectRawPartitionsExec with the right column IDs and other details depending on schema.
  * The inner SelectRawPartitionsExec may have modified transformers based on the discovered schema -
  *   ex. some RangeFunctions are transformed for downsample gauge schemas.
  *
  * @param schema an optional schema to filter partitions using.  If not supplied, schema is discovered.
  * @param colName optional column name to select for querying.  If not supplied, the default valueColumn from
  *               data schema definition is used.  For downsampled gauges, column is automatically chosen.
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

  private def finalizePlan(source: ChunkSource, span: kamon.trace.Span): ExecPlan = {
    span.mark("filtered-partition-scan")
    val partMethod = FilteredPartitionScan(ShardSplit(shard), filters)
    span.mark("lookup-partitions")
    val lookupRes = source.lookupPartitions(dataset, partMethod, chunkMethod)

    // Find the schema if one wasn't supplied
    val schemas = source.schemas(dataset).get
    span.mark("finalize-plan")
    // If we cannot find a schema, or none is provided, we cannot move ahead with specific SRPE planning
    schema.map { s => schemas.schemas(s) }
          .orElse(lookupRes.firstSchemaId.map(schemas.apply))
          .map { sch =>
            // There should not be any unknown schemas at all, as they are filtered out during ingestion and
            // in bootstrapPartKeys().  This might happen after schema changes if old partkeys are not truncated.
            if (sch == Schemas.UnknownSchema) throw UnknownSchemaQueryErr(lookupRes.firstSchemaId.getOrElse(-1))

            // Modify transformers as needed for histogram w/ max, downsample, other schemas
            val newxformers1 = newXFormersForDownsample(sch, rangeVectorTransformers)
            val newxformers = newXFormersForHistMax(sch, newxformers1)

            // Get exact column IDs needed, including max column as needed for histogram calculations.
            // This code is responsible for putting exact IDs needed by any range functions.
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
                queryConfig: QueryConfig,
                parentSpan: kamon.trace.Span)
               (implicit sched: Scheduler,
                timeout: FiniteDuration): ExecResult = {
    finalPlan = finalizePlan(source, parentSpan)
    finalPlan.doExecute(source, queryConfig, parentSpan)(sched, timeout)
   }

  protected def args: String = s"dataset=$dataset, shard=$shard, " +
                               s"chunkMethod=$chunkMethod, filters=$filters, colName=$colName"

  // Print inner node's details for debugging
  override def curNodeText(level: Int): String = {
    val innerText = Option(finalPlan).map(e => s"Inner: + ${e.curNodeText(level + 1)}\n").getOrElse("")
    s"${super.curNodeText(level)} $innerText"
  }

  override protected def printRangeVectorTransformersForLevel(level: Int = 0) = {
     Option(finalPlan).getOrElse(this).rangeVectorTransformers.reverse.zipWithIndex.map { case (t, i) =>
       s"${"-" * (level + i)}T~${t.getClass.getSimpleName}(${t.args})" +
         printFunctionArgument(t, level + i + 1).mkString("\n")
    }
  }
}

