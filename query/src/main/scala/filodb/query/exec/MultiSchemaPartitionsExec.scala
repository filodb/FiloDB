package filodb.query.exec

import kamon.Kamon
import monix.execution.Scheduler

import filodb.core.{DatasetRef, QueryTimeoutException}
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, QueryContext, QuerySession, ServiceUnavailableException}
import filodb.core.query.Filter.Equals
import filodb.core.store._
import filodb.query.Query.qLogger

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
final case class MultiSchemaPartitionsExec(queryContext: QueryContext,
                                           dispatcher: PlanDispatcher,
                                           dataset: DatasetRef,
                                           shard: Int,
                                           filters: Seq[ColumnFilter],
                                           chunkMethod: ChunkScanMethod,
                                           metricColumn: String,
                                           schema: Option[String] = None,
                                           colName: Option[String] = None,
                                           limit: Option[Int] = None) extends LeafExecPlan {
  import SelectRawPartitionsExec._

  override def allTransformers: Seq[RangeVectorTransformer] = finalPlan.rangeVectorTransformers

  @transient // dont serialize the SelectRawPartitionsExec plan created for plan execution
  var finalPlan: SelectRawPartitionsExec = _

  // Remove _columnName from metricColumn and generate PartLookupResult
  private def removeColumnAndGenerateLookupResult(filters: Seq[ColumnFilter], metricName: String, columnName: String,
                                                  source: ChunkSource,
                                                  querySession: QuerySession) = {
    // Assume metric name has only equal filter
    val filterWithoutColumn = filters.filterNot(_.column == metricColumn) :+
      ColumnFilter(metricColumn, Equals(metricName.stripSuffix(s"_$columnName")))

    val partMethod = FilteredPartitionScan(ShardSplit(shard), filterWithoutColumn)
    val lookupRes = source.lookupPartitions(dataset, partMethod, chunkMethod, querySession)
    (lookupRes, Some(columnName))
  }

  // scalastyle:off method.length
  private def finalizePlan(source: ChunkSource,
                           querySession: QuerySession): SelectRawPartitionsExec = {
    val partMethod = FilteredPartitionScan(ShardSplit(shard), filters)
    Kamon.currentSpan().mark("filtered-partition-scan")
    var lookupRes = source.lookupPartitions(dataset, partMethod, chunkMethod, querySession)
    val metricName = filters.find(_.column == metricColumn).map(_.filter.valuesStrings.head.toString)
    var newColName = colName

  /*
   * Remove _sum & _count suffix. _bucket & le are removed in SingleClusterPlanner
   * Metric name can have _sum & _count as suffix. So we remove the suffix only when partition lookup does not
   * return any results
   */
   if (lookupRes.firstSchemaId.isEmpty && querySession.queryConfig.translatePromToFilodbHistogram && colName.isEmpty) {

     if (metricName.isDefined) {
       val res = if (metricName.get.endsWith("_sum"))
                  removeColumnAndGenerateLookupResult(filters, metricName.get, "sum", source, querySession)
                 else if (metricName.get.endsWith("_count"))
                  removeColumnAndGenerateLookupResult(filters, metricName.get, "count", source,
                    querySession)
                 else (lookupRes, newColName)

       lookupRes = res._1
       newColName = res._2
     }
   }

    Kamon.currentSpan().mark("lookup-partitions-done")

    val queryTimeElapsed = System.currentTimeMillis() - queryContext.submitTime
    if (queryTimeElapsed >= queryContext.plannerParams.queryTimeoutMillis)
      throw QueryTimeoutException(queryTimeElapsed, this.getClass.getName)

    // Find the schema if one wasn't supplied
    val schemas = source.schemas(dataset).get
    // If we cannot find a schema, or none is provided, we cannot move ahead with specific SRPE planning
    schema.map { s => schemas.schemas(s) }
          .orElse(lookupRes.firstSchemaId.map(schemas.apply))
          .map { sch =>
            // There should not be any unknown schemas at all, as they are filtered out during ingestion and
            // in bootstrapPartKeys().  This might happen after schema changes if old partkeys are not truncated.
            if (sch == Schemas.UnknownSchema) throw UnknownSchemaQueryErr(lookupRes.firstSchemaId.getOrElse(-1))

            // Get exact column IDs needed, including max column as needed for histogram calculations.
            // This code is responsible for putting exact IDs needed by any range functions.
            val colIDs1 = getColumnIDs(sch, colName.toSeq, rangeVectorTransformers)
            val colIDs  = addIDsForHistMax(sch, colIDs1)

            // Modify transformers as needed for histogram w/ max, downsample, other schemas
            val newxformers1 = newXFormersForDownsample(sch, rangeVectorTransformers)
            val newxformers = newXFormersForHistMax(sch, colIDs, newxformers1)

            val newPlan = SelectRawPartitionsExec(queryContext, dispatcher, dataset,
                                                  Some(sch), Some(lookupRes),
                                                  schema.isDefined, colIDs, limit)
            qLogger.debug(s"Discovered schema ${sch.name} and created inner plan $newPlan")
            newxformers.foreach { xf => newPlan.addRangeVectorTransformer(xf) }
            newPlan
          }.getOrElse {
            qLogger.debug(s"No time series found for filters $filters... employing empty plan")
            SelectRawPartitionsExec(queryContext, dispatcher, dataset,
                                    None, Some(lookupRes),
                                    schema.isDefined, Nil, None)
          }
  }
  // scalastyle:on method.length

  def doExecute(source: ChunkSource,
                querySession: QuerySession)
               (implicit sched: Scheduler): ExecResult = {
    if (!source.isReadyForQuery(dataset, shard)) {
      if (!queryContext.plannerParams.allowPartialResults) {
        throw new ServiceUnavailableException(s"Unable to answer query since shard $shard is still bootstrapping")
      }
      querySession.resultCouldBePartial = true
      querySession.partialResultsReason = Some("Result may be partial since some shards are still bootstrapping")
    }
    finalPlan = finalizePlan(source, querySession)
    finalPlan.doExecute(source, querySession)(sched)
  }

  protected def args: String = s"dataset=$dataset, shard=$shard, " +
                               s"chunkMethod=$chunkMethod, filters=$filters, colName=$colName, schema=$schema"

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

