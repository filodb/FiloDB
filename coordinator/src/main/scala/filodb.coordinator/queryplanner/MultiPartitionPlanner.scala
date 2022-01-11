package filodb.coordinator.queryplanner

import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.queryplanner.LogicalPlanUtils._
import filodb.core.metadata.{Dataset, DatasetOptions, Schemas}
import filodb.core.query.{PromQlQueryParams, QueryConfig, QueryContext}
import filodb.query._
import filodb.query.LogicalPlan._
import filodb.query.exec._

case class PartitionAssignment(partitionName: String, endPoint: String, timeRange: TimeRange)

trait PartitionLocationProvider {

  def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment]
  def getAuthorizedPartitions(timeRange: TimeRange): List[PartitionAssignment]
}

/**
 * The MultiPartitionPlanner  is responsible for planning queries that span one or more deployment partitions.
 * Data for each shard-key (ws/ns) combination can be ingested into a different partition. The partitionLocationProvider
 * param provides the locality mapping of fully specified ws/ns to a single partition. Note that this planner DOES NOT
 * handle regex in ws/ns labels in the queries as that is handled by ShardKeyRegexPlanner. Planners  are
 * Hierarchical and ShardKeyRegexPlanner wraps MultiPartitionPlanner, thus all regex on namespace (_ns_) in the
 * queries are already replaced with equals when the materialize of this class is invoked.
 *
 * @param partitionLocationProvider The implementation is responsible to get the partition assignments based on the
 *                                  shard keys and an additional time dimension to handle assignments of partitions
 * @param localPartitionPlanner     The planner instance to use of the data is available locally
 * @param localPartitionName        Unique name for the local partition
 * @param dataset                   The dataset instance, see Dataset documentation for more details
 * @param queryConfig               Configuration for the query planner
 * @param remoteExecHttpClient      If the partition is not local, a remote call is made to the correct partition to
 *                                  query and retrieve the data.
 */
class MultiPartitionPlanner(partitionLocationProvider: PartitionLocationProvider,
                            localPartitionPlanner: QueryPlanner,
                            localPartitionName: String,
                            val dataset: Dataset,
                            val queryConfig: QueryConfig,
                            remoteExecHttpClient: RemoteExecHttpClient = RemoteHttpClient.defaultClient)
  extends QueryPlanner with StrictLogging with PlannerHelper {

  override val schemas: Schemas = Schemas(dataset.schema)
  override val dsOptions: DatasetOptions = schemas.part.options

  import net.ceedubs.ficus.Ficus._

  val remoteHttpTimeoutMs: Long =
    queryConfig.routingConfig.config.as[Option[Long]]("remote.http.timeout").getOrElse(60000)

  val datasetMetricColumn: String = dataset.options.metricColumn


  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
      // Pseudo code for the materialize
      //
      // def materialize(lp) {
      //   walk(lp)
      // }
      //
      // def walk(lp) {
      //   if lp.isLocalPlan() {
      //     localPlanner.materialize(lp)
      //   } else if partitions(lp).size == 1 && partitions(lp.partitions).head.name != "localPartition" {
      //     materializeRemoteExecPlan(lp)
      //   } else {
      //     case BinaryJoin:
      //       lhs, rhs = walk(lp.lhs), walk(lp.rhs)
      //       BinaryJoinExec(lhs, rhs, inProcess)
      //     case x: _:
      //       // X represents appropriate handler based on type of x
      //       plannerHelper.materializeX(x)
      //    }
      // }
    val tsdbQueryParams = qContext.origQueryParams

    if(
      !tsdbQueryParams.isInstanceOf[PromQlQueryParams] || // We don't know the promql issued (unusual)
      (tsdbQueryParams.isInstanceOf[PromQlQueryParams] && !qContext.plannerParams.processMultiPartition)
    ) { // Query was part of routing
      localPartitionPlanner.materialize(logicalPlan, qContext)
    } else logicalPlan match {
//      case tlsq: TopLevelSubquery             => super.materializeTopLevelSubquery(qContext, tlsq).plans.head
//      case sqww: SubqueryWithWindowing        => super.materializeSubqueryWithWindowing(qContext, sqww).plans.head
      case mqp: MetadataQueryPlan             => materializeMetadataQueryPlan(mqp, qContext).plans.head
      case lp: TsCardinalities                => materializeTsCardinalities(lp, qContext).plans.head
      case _                                  => walkLogicalPlanTree(logicalPlan, qContext).plans.head
    }
  }

  override def walkLogicalPlanTree(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {
    // Should avoid this asInstanceOf, far many places where we do this now.
    val params = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    // MultiPartitionPlanner has capability to stitch across time partitions
    // To see if we do have  across-time partitions
    // (or just multiple partitions because of a binary join) we call getPartitions() function
    // In case of TopLevelSubquery normal start-end of query it not enough as they might access the data beyond
    // query start-end, hence, we "enhance" update params with the start-end that the queries will indeed cover.
    // The notion of lookback that getPartitions() understands, is not applicable to TopLevelSubquery, hence, special
    // treatment here. The parameters produced, however, are used exclusively to see if we have multiple
    // partitions.
//    val updatedQueryContext = logicalPlan match {
//      case tlsq: TopLevelSubquery => topLevelSubqueryContext(tlsq, qContext)
//      case _ => qContext
//    }
//    val paramToCheckPartitions = updatedQueryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
//    val partitions = getPartitions(logicalPlan, paramToCheckPartitions)
    val paramToCheckPartitions = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val partitions = getPartitions(logicalPlan, paramToCheckPartitions)

    if (isSinglePartition(partitions)) {
        val (partitionName, startMs, endMs) =
          ( partitions.headOption.map(_.partitionName).getOrElse(localPartitionName),
            params.startSecs * 1000L,
            params.endSecs * 1000L)

        // If the plan is on a single partition, then depending on partition name we either delegate to local or
        // remote planner
        val execPlan = if (partitionName.equals(localPartitionName)) {
            localPartitionPlanner.materialize(logicalPlan, qContext)
        } else {
          // Single partition but remote, send the entire plan remotely
          val remotePartitionEndpoint = partitions.head.endPoint
          val httpEndpoint = remotePartitionEndpoint + params.remoteQueryPath.getOrElse("")
          val remoteContext = if (logicalPlan.isInstanceOf[PeriodicSeriesPlan]) {
            val psp : PeriodicSeriesPlan = logicalPlan.asInstanceOf[PeriodicSeriesPlan]
            val startSecs = psp.startMs / 1000
            val stepSecs = psp.stepMs / 1000
            val endSecs = psp.endMs / 1000
            generateRemoteExecParamsWithStep(qContext, startSecs, stepSecs, endSecs)
          } else {
            generateRemoteExecParams(qContext, startMs, endMs)
          }
          PromQlRemoteExec(
            httpEndpoint, remoteHttpTimeoutMs, remoteContext, inProcessPlanDispatcher, dataset.ref, remoteExecHttpClient
          )
        }
        PlanResult(Seq(execPlan))
    } else walkMultiPartitionPlan(logicalPlan, qContext)
  }


  /**
   * Invoked when the plan tree spans multiple plans
   *
   * @param logicalPlan The multi partition LogicalPlan tree
   * @param qContext the QueryContext object
   * @return
   */
  private def walkMultiPartitionPlan(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {
    logicalPlan match {
      case lp: BinaryJoin                 => materializeMultiPartitionBinaryJoin(lp, qContext)
      case _: MetadataQueryPlan           => throw new IllegalArgumentException("MetadataQueryPlan unexpected here")
      case lp: ApplyInstantFunction       => super.materializeApplyInstantFunction(qContext, lp)
      case lp: ApplyInstantFunctionRaw    => super.materializeApplyInstantFunctionRaw(qContext, lp)
      case lp: Aggregate                  => super.materializeAggregate(qContext, lp)
      case lp: ScalarVectorBinaryOperation=> super.materializeScalarVectorBinOp(qContext, lp)
      case lp: ApplyMiscellaneousFunction => super.materializeApplyMiscellaneousFunction(qContext, lp)
      case lp: ApplySortFunction          => super.materializeApplySortFunction(qContext, lp)
      case lp: ScalarVaryingDoublePlan    => super.materializeScalarPlan(qContext, lp)
      case _: ScalarTimeBasedPlan         => throw new IllegalArgumentException("ScalarTimeBasedPlan unexpected here")
      case lp: VectorPlan                 => super.materializeVectorPlan(qContext, lp)
      case _: ScalarFixedDoublePlan       => throw new IllegalArgumentException("ScalarFixedDoublePlan unexpected here")
      case lp: ApplyAbsentFunction        => super.materializeAbsentFunction(qContext, lp)
      case lp: ScalarBinaryOperation      => super.materializeScalarBinaryOperation(qContext, lp)
      case lp: ApplyLimitFunction         => super.materializeLimitFunction(qContext, lp)
      case lp: TsCardinalities            => materializeTsCardinalities(lp, qContext)
      case lp: SubqueryWithWindowing       => super.materializeSubqueryWithWindowing(qContext, lp)
      case lp: TopLevelSubquery            => super.materializeTopLevelSubquery(qContext, lp)
      case _: PeriodicSeries |
           _: PeriodicSeriesWithWindowing |
           _: RawChunkMeta |
           _: RawSeries                   => materializePeriodicAndRawSeries(logicalPlan, qContext)
    }
  }

  private def getRoutingKeys(logicalPlan: LogicalPlan) = {
    val columnFilterGroup = LogicalPlan.getColumnFilterGroup(logicalPlan)
    val routingKeys = dataset.options.nonMetricShardColumns
      .map(x => (x, LogicalPlan.getColumnValues(columnFilterGroup, x)))
    if (routingKeys.flatMap(_._2).isEmpty) Seq.empty else routingKeys
  }

  private def generateRemoteExecParams(queryContext: QueryContext, startMs: Long, endMs: Long) = {
    val queryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryContext.copy(origQueryParams = queryParams.copy(startSecs = startMs/1000, endSecs = endMs / 1000),
      plannerParams = queryContext.plannerParams.copy(processMultiPartition = false))
  }

  private def generateRemoteExecParamsWithStep(
    queryContext: QueryContext, startSecs: Long, stepSecs: Long, endSecs: Long
  ) = {
    val queryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryContext.copy(
      origQueryParams = queryParams.copy(startSecs = startSecs, stepSecs = stepSecs, endSecs = endSecs),
      plannerParams = queryContext.plannerParams.copy(processMultiPartition = false)
    )
  }

  /**
   * Gets the partition Assignment for the given plan
   */
  private def getPartitions(logicalPlan: LogicalPlan,
                            queryParams: PromQlQueryParams,
                            infiniteTimeRange: Boolean = false) : Seq[PartitionAssignment] = {

    //1.  Get a Seq of all Leaf node filters
    val leafFilters = LogicalPlan.getColumnFilterGroup(logicalPlan)
    val nonMetricColumnSet = dataset.options.nonMetricShardColumns.toSet
    //2. Filter from each leaf node filters to keep only nonShardKeyColumns and convert them to key value map
    val routingKeyMap = leafFilters.map(cf => {
      cf.filter(col => nonMetricColumnSet.contains(col.column)).map(
        x => (x.column, x.filter.valuesStrings.head.toString)).toMap
    })

    // 3. Determine the query time range
    val queryTimeRange = if (infiniteTimeRange) {
      TimeRange(0, Long.MaxValue)
    } else {
      // 3a. Get the start and end time is ms based on the lookback, offset and the user provided start and end time
      val (maxOffsetMs, minOffsetMs) = LogicalPlanUtils.getOffsetMillis(logicalPlan)
        .foldLeft((Long.MinValue, Long.MaxValue)) {
          case ((accMax, accMin), currValue) => (accMax.max(currValue), accMin.min(currValue))
        }

      val periodicSeriesTimeWithOffset = TimeRange((queryParams.startSecs * 1000) - maxOffsetMs,
        (queryParams.endSecs * 1000) - minOffsetMs)
      val lookBackMs = getLookBackMillis(logicalPlan).max

      //3b Get the Query time range based on user provided range, offsets in previous steps and lookback
      TimeRange(periodicSeriesTimeWithOffset.startMs - lookBackMs,
        periodicSeriesTimeWithOffset.endMs)
    }

    //4. Based on the map in 2 and time range in 5, get the partitions to query
    routingKeyMap.flatMap(metricMap =>
      partitionLocationProvider.getPartitions(metricMap, queryTimeRange))
  }

  /**
   * Checks if all the PartitionAssignments belong to same partition
   */
  private def isSinglePartition(partitions: Seq[PartitionAssignment]) : Boolean = {
    if (partitions.isEmpty)
      true
    else {
      val partName = partitions.head.partitionName
      partitions.forall(_.partitionName.equals(partName))
    }
  }


  /**
   *
   * @param logicalPlan Logical plan
   * @param queryParams PromQlQueryParams having query details
   * @return Returns PartitionAssignment, lookback, offset and routing keys
   */
  private def resolvePartitionsAndRoutingKeys(logicalPlan: LogicalPlan, queryParams: PromQlQueryParams) = {

    val routingKeys = getRoutingKeys(logicalPlan)

    val offsetMs = LogicalPlanUtils.getOffsetMillis(logicalPlan)
    // To cover entire time range for queries like sum(foo offset 2d) - sum(foo)
    val periodicSeriesTimeWithOffset = TimeRange((queryParams.startSecs * 1000) - offsetMs.max,
      (queryParams.endSecs * 1000) - offsetMs.min)
    val lookBackMs = getLookBackMillis(logicalPlan).max

    // Time at which raw data would be retrieved which is used to get partition assignments.
    // It should have time with offset and lookback as we need raw data at time including offset and lookback.
    val queryTimeRange = TimeRange(periodicSeriesTimeWithOffset.startMs - lookBackMs,
      periodicSeriesTimeWithOffset.endMs)

    val partitions = if (routingKeys.isEmpty) List.empty
    else {
      val routingKeyMap = routingKeys.map(x => (x._1, x._2.head)).toMap
      partitionLocationProvider.getPartitions(routingKeyMap, queryTimeRange).
        sortBy(_.timeRange.startMs)
    }
    if (partitions.isEmpty && routingKeys.nonEmpty)
      logger.warn(s"No partitions found for routing keys: $routingKeys")

    (partitions, lookBackMs, offsetMs, routingKeys)
  }

  /**
    * Materialize all queries except Binary Join and Metadata
    */
  def materializePeriodicAndRawSeries(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val (partitions, lookBackMs, offsetMs, routingKeys) = resolvePartitionsAndRoutingKeys(logicalPlan, queryParams)
    val execPlan = if (partitions.isEmpty || routingKeys.forall(_._2.isEmpty))
      localPartitionPlanner.materialize(logicalPlan, qContext)
    else {
      val stepMs = queryParams.stepSecs * 1000
      val isInstantQuery: Boolean = if (queryParams.startSecs == queryParams.endSecs) true else false
      var prevPartitionStart = queryParams.startSecs * 1000
      val execPlans = partitions.zipWithIndex.map { case (p, i) =>
        // First partition should start from query start time, no need to calculate time according to step for instant
        // queries
        val startMs =
          if (i == 0 || isInstantQuery) {
            queryParams.startSecs * 1000
          } else {
            // The logic below does not work for partitions split across time as we encounter a hole
            // in the produced result. The size of the hole is lookBackMs + stepMs
            val numStepsInPrevPartition = (p.timeRange.startMs - prevPartitionStart + lookBackMs) / stepMs
            val lastPartitionInstant = prevPartitionStart + numStepsInPrevPartition * stepMs
            val start = lastPartitionInstant + stepMs
            // If query duration is less than or equal to lookback start will be greater than query end time
            if (start > (queryParams.endSecs * 1000)) queryParams.endSecs * 1000 else start
          }
        prevPartitionStart = startMs
        // we assume endMs should be equal partition endMs but if the query's end is smaller than partition endMs,
        // why would we want to stretch the query??
        val endMs = if (isInstantQuery) queryParams.endSecs * 1000 else p.timeRange.endMs + offsetMs.min
        logger.debug(s"partitionInfo=$p; updated startMs=$startMs, endMs=$endMs")
        if (p.partitionName.equals(localPartitionName)) {
          val lpWithUpdatedTime = copyLogicalPlanWithUpdatedTimeRange(logicalPlan, TimeRange(startMs, endMs))
          localPartitionPlanner.materialize(lpWithUpdatedTime, qContext)
        } else {
          val httpEndpoint = p.endPoint + queryParams.remoteQueryPath.getOrElse("")
          PromQlRemoteExec(httpEndpoint, remoteHttpTimeoutMs, generateRemoteExecParams(qContext, startMs, endMs),
            inProcessPlanDispatcher, dataset.ref, remoteExecHttpClient)
        }
      }
      if (execPlans.size == 1) execPlans.head
      else {
        // TODO: Do we pass in QueryContext in LogicalPlan's helper rvRangeForPlan?
        StitchRvsExec(qContext, inProcessPlanDispatcher, rvRangeFromPlan(logicalPlan),
          execPlans.sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec]))
      }
      // ^^ Stitch RemoteExec plan results with local using InProcessPlanDispatcher
      // Sort to move RemoteExec in end as it does not have schema
    }
    PlanResult(execPlan:: Nil)
  }

  private def materializeMultiPartitionBinaryJoin(logicalPlan: BinaryJoin, qContext: QueryContext): PlanResult = {
    val lhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.lhs)))
    val rhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.rhs)))

    val lhsExec = this.materialize(logicalPlan.lhs, lhsQueryContext)
    val rhsExec = this.materialize(logicalPlan.rhs, rhsQueryContext)

    val execPlan = if (logicalPlan.operator.isInstanceOf[SetOperator])
      SetOperatorExec(qContext, InProcessPlanDispatcher(queryConfig), Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
        LogicalPlanUtils.renameLabels(logicalPlan.on, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn), datasetMetricColumn,
        rvRangeFromPlan(logicalPlan))
    else
      BinaryJoinExec(qContext, inProcessPlanDispatcher, Seq(lhsExec), Seq(rhsExec), logicalPlan.operator,
        logicalPlan.cardinality, LogicalPlanUtils.renameLabels(logicalPlan.on, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.include, datasetMetricColumn), datasetMetricColumn,
        rvRangeFromPlan(logicalPlan))
    PlanResult(execPlan :: Nil)
  }

  private def copy(lp: MetadataQueryPlan, startMs: Long, endMs: Long): MetadataQueryPlan = lp match {
    case sk: SeriesKeysByFilters       => sk.copy(startMs = startMs, endMs = endMs)
    case lv: LabelValues               => lv.copy(startMs = startMs, endMs = endMs)
    case ln: LabelNames                => ln.copy(startMs = startMs, endMs = endMs)
    case lc: LabelCardinality          => lc.copy(startMs = startMs, endMs = endMs)
  }

  def materializeMetadataQueryPlan(lp: MetadataQueryPlan, qContext: QueryContext): PlanResult = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]


    // LabelCardinality is a special case, here the partitions to send this query to is not  the authorized partition
    // but the actual one where data resides, similar to how non metadata plans work, however, getting label cardinality
    // is a metadata operation and shares common components with other metadata endpoints.
    val partitions = lp match {
      case lc: LabelCardinality       => getPartitions(lc, qContext.origQueryParams.asInstanceOf[PromQlQueryParams])
      case _                          => partitionLocationProvider.getAuthorizedPartitions(
        TimeRange(queryParams.startSecs * 1000, queryParams.endSecs * 1000))
    }

    val execPlan = if (partitions.isEmpty) {
      logger.warn(s"No partitions found for ${queryParams.startSecs}, ${queryParams.endSecs}")
      localPartitionPlanner.materialize(lp, qContext)
    }
    else {
      val execPlans = partitions.map { p =>
        logger.debug(s"partitionInfo=$p; queryParams=$queryParams")
        if (p.partitionName.equals(localPartitionName))
          localPartitionPlanner.materialize(
            copy(lp, startMs = p.timeRange.startMs, endMs = p.timeRange.endMs), qContext)
        else {
          val params: Map[String, String] = lp match {
            case _: SeriesKeysByFilters |
                 _: LabelNames |
                 _: LabelCardinality    => Map("match[]" -> LogicalPlanParser.metatadataMatchToQuery(lp))
            case lv: LabelValues        => PlannerUtil.getLabelValuesUrlParams(lv, queryParams)
          }
          createMetadataRemoteExec(qContext, p, params)
        }
      }
      if (execPlans.size == 1) execPlans.head
      else lp match {
        case _: SeriesKeysByFilters => PartKeysDistConcatExec(qContext, inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[MetadataRemoteExec]))
        case _: LabelValues => LabelValuesDistConcatExec(qContext, inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[MetadataRemoteExec]))
        case _: LabelNames => LabelNamesDistConcatExec(qContext, inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[MetadataRemoteExec]))
        case _: LabelCardinality => LabelCardinalityReduceExec(qContext, inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[MetadataRemoteExec]))
      }
    }
    PlanResult(execPlan::Nil)
  }

  def materializeTsCardinalities(lp: TsCardinalities, qContext: QueryContext): PlanResult = {

    import TsCardinalities._

    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val partitions = if (lp.shardKeyPrefix.size >= 2) {
      // At least a ws/ns pair is required to select specific partitions.
      getPartitions(lp, queryParams, infiniteTimeRange = true)
    } else {
      logger.info(s"(ws, ns) pair not provided in prefix=${lp.shardKeyPrefix};" +
                  s"dispatching to all authorized partitions")
      partitionLocationProvider.getAuthorizedPartitions(TimeRange(0, Long.MaxValue))
    }
    val execPlan = if (partitions.isEmpty) {
      logger.warn(s"no partitions found for $lp; defaulting to local planner")
      localPartitionPlanner.materialize(lp, qContext)
    } else {
      val execPlans = partitions.map { p =>
        logger.debug(s"partition=$p; plan=$lp")
        if (p.partitionName.equals(localPartitionName))
          localPartitionPlanner.materialize(lp, qContext)
        else {
          val params = Map(
            "match[]" -> ("{" + SHARD_KEY_LABELS.zip(lp.shardKeyPrefix)
                           .map{ case (label, value) => s"""$label="$value""""}
                           .mkString(",") + "}"),
            "numGroupByFields" -> lp.numGroupByFields.toString)
          createMetadataRemoteExec(qContext, p, params)
        }
      }
      if (execPlans.size == 1) {
        execPlans.head
      } else {
        TsCardReduceExec(qContext, inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[MetadataRemoteExec]))
      }
    }
    PlanResult(execPlan::Nil)
  }

  private def createMetadataRemoteExec(qContext: QueryContext, partitionAssignment: PartitionAssignment,
                                       urlParams: Map[String, String]) = {
    val finalQueryContext = generateRemoteExecParams(
      qContext, partitionAssignment.timeRange.startMs, partitionAssignment.timeRange.endMs)
    val httpEndpoint = partitionAssignment.endPoint + finalQueryContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      remoteQueryPath.getOrElse("")
    MetadataRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
      urlParams, finalQueryContext, inProcessPlanDispatcher, dataset.ref, remoteExecHttpClient)
  }
}
