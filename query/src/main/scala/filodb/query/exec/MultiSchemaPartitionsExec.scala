package filodb.query.exec

import kamon.Kamon
import monix.execution.Scheduler

import filodb.core.{DatasetRef, GlobalConfig}
import filodb.core.memstore.PartLookupResult
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, QueryConfig, QueryContext, QuerySession, QueryWarnings}
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
                                           colName: Option[String] = None) extends LeafExecPlan {
  import SelectRawPartitionsExec._

  override def allTransformers: Seq[RangeVectorTransformer] = finalPlan.rangeVectorTransformers.toSeq

  @transient // dont serialize the SelectRawPartitionsExec plan created for plan execution
  var finalPlan: SelectRawPartitionsExec = _

  // Remove _columnName suffix from metricName and generate PartLookupResult
  private def removeSuffixAndGenerateLookupResult(filters: Seq[ColumnFilter], metricName: String, columnName: String,
                                                  source: ChunkSource,
                                                  datasetRef: DatasetRef,
                                                  originalLookupResult: PartLookupResult,
                                                  querySession: QuerySession) = {
    // Assume metric name has only equal filter
    val filterWithoutColumn = filters.filterNot(_.column == metricColumn) :+
      ColumnFilter(metricColumn, Equals(metricName.stripSuffix(s"_$columnName")))

    val partMethod = FilteredPartitionScan(ShardSplit(shard), filterWithoutColumn)
    // clear stats since previous call to lookupPartitions set the stat with metric name that has suffix
    querySession.queryStats.clear()
    val lookupRes = source.lookupPartitions(dataset, partMethod, chunkMethod, querySession)
    // Accept only if lookupResult schema has a data column with name as the requested columnName.
    // This is true generally for histogram queries where we rewrite the query to remove _sum, _count suffix.
    // Check is needed since summaries are not columnized, and we don't want the rewrite to happen for summaries.
    // If column does not exist, return original lookup result and don't change the query
    val schemas = source.schemas(datasetRef).get
    if (lookupRes.firstSchemaId.exists(s => schemas.apply(s).data.columns.exists(_.name == columnName)))
      (lookupRes, Some(columnName))
    else
      (originalLookupResult, None)
  }

  /**
   * @param maxMinTenantValue tenant value string such as workspace, app etc
   * @return true if the config is defined AND maxMinTenantValue is not in the list of disabled workspaces.
   *         false otherwise
   */
  def isMaxMinColumnsEnabled(maxMinTenantValue: Option[String]) : Boolean = {
    maxMinTenantValue.isDefined match {
      // we are making sure that the config is defined to avoid any accidental "turn on" of the feature when not desired
      case true => (GlobalConfig.workspacesDisabledForMaxMin.isDefined) &&
        (!GlobalConfig.workspacesDisabledForMaxMin.get.contains(maxMinTenantValue.get))
      case false => false
    }
  }

  // scalastyle:off method.length
  private def finalizePlan(source: ChunkSource,
                           querySession: QuerySession): SelectRawPartitionsExec = {
    val partMethod = FilteredPartitionScan(ShardSplit(shard), filters)
    Kamon.currentSpan().mark("filtered-partition-scan")
    var lookupRes = source.lookupPartitions(dataset, partMethod, chunkMethod, querySession)
    val metricName = filters.find(_.column == metricColumn).map(_.filter.valuesStrings.head.toString)
    val maxMinTenantFilter = filters
      .find(x => x.column == GlobalConfig.maxMinTenantColumnFilter && x.filter.isInstanceOf[Equals])
      .map(_.filter.valuesStrings.head.toString)
    var newColName = colName

    /*
     * As part of Histogram query compatibility with Prometheus format histograms, we
     * remove _sum, _count suffix from metric name here. _bucket & le are already
     * removed in SingleClusterPlanner. We remove the suffix only when partition lookup does not return any results
     */
    if (lookupRes.firstSchemaId.isEmpty && querySession.queryConfig.translatePromToFilodbHistogram &&
        colName.isEmpty && metricName.isDefined) {
      val res = if (metricName.get.endsWith("_sum"))
        removeSuffixAndGenerateLookupResult(filters, metricName.get, "sum", source, dataset, lookupRes, querySession)
      else if (metricName.get.endsWith("_count"))
        removeSuffixAndGenerateLookupResult(filters, metricName.get, "count", source, dataset, lookupRes, querySession)
      else (lookupRes, newColName)

      lookupRes = res._1
      newColName = res._2
    }

    Kamon.currentSpan().mark("lookup-partitions-done")

    queryContext.checkQueryTimeout(this.getClass.getName)

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
            val colIDs1 = getColumnIDs(sch, newColName.toSeq, rangeVectorTransformers.toSeq)

            val colIDs = if (sch.data.columns.exists(_.name == "min") &&
                             sch.data.columns.exists(_.name == "max") &&
                             isMaxMinColumnsEnabled(maxMinTenantFilter))  addIDsForHistMaxMin(sch, colIDs1)
                         else colIDs1

            // Modify transformers as needed for histogram w/ max, downsample, other schemas
            val newxformers1 = newXFormersForDownsample(sch, rangeVectorTransformers.toSeq).toSeq
            val newxformers = isMaxMinColumnsEnabled(maxMinTenantFilter) match {
              case true => newXFormersForHistMaxMin(sch, colIDs, newxformers1)
              case _ => newxformers1
            }

            val newPlan = SelectRawPartitionsExec(queryContext, dispatcher, dataset,
                                                  Some(sch), Some(lookupRes),
                                                  schema.isDefined, colIDs, planId)
            qLogger.debug(s"Discovered schema ${sch.name} and created inner plan $newPlan")
            newxformers.foreach { xf => newPlan.addRangeVectorTransformer(xf) }
            newPlan
          }.getOrElse {
            qLogger.debug(s"No time series found for filters $filters... employing empty plan")
            SelectRawPartitionsExec(queryContext, dispatcher, dataset,
                                    None, Some(lookupRes),
                                    schema.isDefined, Nil, planId)
          }
  }
  // scalastyle:on method.length

  def doExecute(source: ChunkSource,
                querySession: QuerySession)
               (implicit sched: Scheduler): ExecResult = {
    source.checkReadyForQuery(dataset, shard, querySession)
    source.acquireSharedLock(dataset, shard, querySession)
    finalPlan = finalizePlan(source, querySession)
    finalPlan.doExecute(source, querySession)(sched)
  }

  protected def args: String = s"dataset=$dataset, shard=$shard, " +
                               s"chunkMethod=$chunkMethod, filters=$filters, colName=$colName, schema=$schema"

  // Print inner node's details for debugging
  override def curNodeText(level: Int): String = {
    val innerText = Option(finalPlan).map(e => s"Inner: + ${e.curNodeText(level + 1)}\n").getOrElse("")
    s"${super.curNodeText(level)} $innerText".trim
  }

  override protected def printRangeVectorTransformersForLevel(level: Int = 0) = {
     Option(finalPlan).getOrElse(this).rangeVectorTransformers.reverse.zipWithIndex.map { case (t, i) =>
       s"${"-" * (level + i)}T~${t.getClass.getSimpleName}(${t.args})" +
         printFunctionArgument(t, level + i + 1).mkString("\n")
    }
  }

  override def checkResultBytes(resultSize: Long, queryConfig: QueryConfig, queryWarnings: QueryWarnings): Unit = {
    super.checkResultBytes(resultSize, queryConfig, queryWarnings)
    finalPlan.lookupRes.foreach(plr =>
      if (plr.dataBytesScannedCtr.get() > queryContext.plannerParams.warnLimits.rawScannedBytes) {
        queryWarnings.updateRawScannedBytes(plr.dataBytesScannedCtr.get())
        val msg =
          s"Query scanned ${plr.dataBytesScannedCtr.get()} bytes, which exceeds a max warn limit of " +
            s"${queryContext.plannerParams.warnLimits.rawScannedBytes} bytes allowed to be scanned per shard. "
        qLogger.info(queryContext.getQueryLogLine(msg))
      }
    )
  }
}

