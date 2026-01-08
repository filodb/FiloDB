package filodb.coordinator.queryplanner

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import akka.actor.ActorRef
import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.{ActorPlanDispatcher, GrpcPlanDispatcher, RemoteActorPlanDispatcher, ShardMapper}
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.coordinator.queryplanner.SingleClusterPlanner.findTargetSchema
import filodb.core.{SpreadProvider, StaticTargetSchemaProvider, TargetSchemaChange, TargetSchemaProvider}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.{Dataset, DatasetOptions, Schemas}
import filodb.core.metrics.FilodbMetrics
import filodb.core.query.{Filter, _}
import filodb.core.query.Filter.{Equals, EqualsRegex}
import filodb.prometheus.ast.Vectors.{PromMetricLabel, TypeLabel}
import filodb.prometheus.ast.WindowConstants
import filodb.query.{exec, _}
import filodb.query.InstantFunctionId.HistogramBucket
import filodb.query.LogicalPlan._
import filodb.query.exec.{LocalPartitionDistConcatExec, _}
import filodb.query.exec.InternalRangeFunction.Last

// scalastyle:off file.size.limit

object SingleClusterPlanner {
  private val mdNoShardKeyFilterRequests = FilodbMetrics.counter("queryengine-metadata-no-shardkey-requests")

  // Find the TargetSchema that is applicable i.e effective for the current query window
  private def findTargetSchema(targetSchemaChanges: Seq[TargetSchemaChange],
                               startMs: Long, endMs: Long): Option[TargetSchemaChange] = {
    val tsIndex = targetSchemaChanges.lastIndexWhere(t => t.time <= startMs)
    if(tsIndex > -1)
      Some(targetSchemaChanges(tsIndex))
    else
      None
  }
}

/**
  * Responsible for query planning within single FiloDB cluster
  * @param schemas          schema instance, used to extract partKey schema
  * @param spreadProvider  used to get spread
  * @param shardMapperFunc used to get shard locality
  * @param timeSplitEnabled split based on longer time range
  * @param minTimeRangeForSplitMs if time range is longer than this, zplan will be split into multiple plans
  * @param splitSizeMs time range for each split, if plan needed to be split
  */

class SingleClusterPlanner(val dataset: Dataset,
                           val schemas: Schemas,
                           shardMapperFunc: => ShardMapper,
                           earliestRetainedTimestampFn: => Long,
                           val queryConfig: QueryConfig,
                           clusterName: String,
                           spreadProvider: SpreadProvider = StaticSpreadProvider(),
                           _targetSchemaProvider: TargetSchemaProvider = StaticTargetSchemaProvider(),
                           timeSplitEnabled: Boolean = false,
                           minTimeRangeForSplitMs: => Long = 1.day.toMillis,
                           splitSizeMs: => Long = 1.day.toMillis)
                           extends QueryPlanner with StrictLogging with DefaultPlanner {
  override val dsOptions: DatasetOptions = schemas.part.options
  private val shardColumns = dsOptions.shardKeyColumns.sorted
  private val dsRef = dataset.ref

  private val shardPushdownCache: Option[Cache[(LogicalPlan, Option[Seq[Int]]), Option[Set[Int]]]] =
      if (queryConfig.cachingConfig.singleClusterPlannerCachingEnabled) {
        Some(
          Caffeine.newBuilder()
          .maximumSize(queryConfig.cachingConfig.singleClusterPlannerCachingSize)
          .recordStats()
          .build()
        )
      } else {
        None
      }


  private val tSchemaChangingCache: Option[Cache[(Seq[ColumnFilter], Long, Long), Some[Boolean]]] =
    if (queryConfig.cachingConfig.singleClusterPlannerCachingEnabled) {
      Some(
        Caffeine.newBuilder()
          .maximumSize(queryConfig.cachingConfig.singleClusterPlannerCachingSize)
          .recordStats()
          .build()
      )
    } else {
      None
    }

  private[queryplanner] def invalidateCaches(): Unit = {
    shardPushdownCache.foreach(_.invalidateAll())
    tSchemaChangingCache.foreach(_.invalidateAll())
  }

  // failed failover counter captures failovers which are not possible because at least one shard
  // is down both on the primary and DR clusters, the query will get executed only when the
  // partial results are acceptable otherwise an exception is thrown
  val shardUnavailableFailoverCounter = FilodbMetrics.counter(HighAvailabilityPlanner.FailoverCounterName,
                                                Map("cluster" -> clusterName, "type" -> "shardUnavailable"))

  val numPlansMaterialized = FilodbMetrics.counter("single-cluster-plans-materialized",
                                                Map("cluster" -> clusterName, "dataset" -> dataset.ref.dataset))

  private def targetSchemaProvider(qContext: QueryContext): TargetSchemaProvider = {
   qContext.plannerParams.targetSchemaProviderOverride.getOrElse(_targetSchemaProvider)
  }

  private def isTargetSchemaChangingInner(shardKeyFilters: Seq[ColumnFilter],
                                  startMs: Long, endMs: Long,
                                  qContext: QueryContext): Boolean = {
    val keyToValues = shardKeyFilters.map { filter =>
      val values = filter match {
        case ColumnFilter(col, regex: EqualsRegex) if QueryUtils.containsPipeOnlyRegex(regex.value.toString) =>
          QueryUtils.splitAtUnescapedPipes(regex.value.toString).distinct
        case ColumnFilter(col, equals: Equals) =>
          Seq(equals.value.toString)
      }
      (filter.column, values)
    }.toMap

    QueryUtils.makeAllKeyValueCombos(keyToValues).exists { shardKeys =>
      // Replace any EqualsRegex shard-key filters with Equals.
      val equalsFilters = shardKeys.map(entry => ColumnFilter(entry._1, Equals(entry._2))).toSeq
      val newFilters = LogicalPlanUtils.upsertFilters(shardKeyFilters, equalsFilters)
      val targetSchemaChanges = targetSchemaProvider(qContext).targetSchemaFunc(newFilters)
      targetSchemaChanges.nonEmpty && targetSchemaChanges.exists(c => c.time >= startMs && c.time <= endMs)
    }
  }


  /**
   * Returns true iff a target-schema:
   *   (1) matches any shard-key matched by the argument filters, and
   *   (2) changes between the argument timestamps.
   */
  def isTargetSchemaChanging(shardKeyFilters: Seq[ColumnFilter],
                             startMs: Long, endMs: Long,
                             qContext: QueryContext): Boolean = {
    tSchemaChangingCache match {
      case Some(cache) =>
        cache.get((shardKeyFilters, startMs, endMs), _ => {
          Some(isTargetSchemaChangingInner(shardKeyFilters, startMs, endMs, qContext))
        }).getOrElse(true)
      case None =>
        isTargetSchemaChangingInner(shardKeyFilters, startMs, endMs, qContext)
    }
  }

  /**
   * Returns true iff a target-schema should be used to identify query shards.
   * A target-schema should be used iff all of:
   *   (1) A target-schema is defined for the argument filters.
   *   (2) The target-schema does not change between startMs and endMs.
   *   (3) All required target-schema columns are present in the argument filters.
   *
   * @param filters      Query Column Filters
   * @param targetSchema TargetSchema
   * @return useTargetSchema - use target-schema to calculate query shards
   */
  def useTargetSchemaForShards(filters: Seq[ColumnFilter],
                               startMs: Long, endMs: Long,
                               qContext: QueryContext): Boolean = {
    val targetSchemaChanges = targetSchemaProvider(qContext).targetSchemaFunc(filters)
    val targetSchemaOpt = findTargetSchema(targetSchemaChanges, startMs, endMs)
    if (targetSchemaOpt.isEmpty) {
      return false
    }

    val shardKeyFilters = {
      val filterOpts = dataset.options.nonMetricShardColumns.map(col => filters.find(_.column == col))
      assert(filterOpts.forall(_.isDefined), "expected all shard-key filters present but found: " + filters)
      filterOpts.map(_.get)
    }
    val tsChangeExists = isTargetSchemaChanging(shardKeyFilters, startMs, endMs, qContext)
    val allTSColsPresent = targetSchemaOpt.get.schema
      .forall(tschemaCol => filters.exists(cf =>
        cf.column == tschemaCol && cf.filter.isInstanceOf[Equals]))

    !tsChangeExists && allTSColsPresent
  }

  import SingleClusterPlanner._

  private def dispatcherForShard(shard: Int, forceInProcess: Boolean, queryContext: QueryContext): PlanDispatcher = {
    if (forceInProcess) {
      return inProcessPlanDispatcher
    }
    queryContext.plannerParams.failoverMode match {
      case LegacyFailoverMode => legacyDispatcherForShard(shard, queryContext)
      case ShardLevelFailoverMode => shardLevelFailoverDispatcherForShard(shard, queryContext)
    }
  }

  private def legacyDispatcherForShard(shard: Int, queryContext: QueryContext): PlanDispatcher = {
    val targetActor = shardMapperFunc.coordForShard(shard)
    if (targetActor == ActorRef.noSender) {
      logger.debug(s"ShardMapper: $shardMapperFunc")
      if (queryContext.plannerParams.allowPartialResults)
        logger.debug(s"Shard: $shard is not available however query is proceeding as partial results is enabled")
      else
        throw new RuntimeException(s"Shard: $shard is not available")
    }
    ActorPlanDispatcher(targetActor, clusterName)
  }



  private def shardLevelFailoverDispatcherForShard(shard: Int, queryContext: QueryContext): PlanDispatcher = {
    val pp = queryContext.plannerParams
    val localShardMapper =
      pp.localShardMapper.getOrElse(throw new RuntimeException("Local shard map unavailable"))
    val buddyShardMapper = pp.buddyShardMapper.get
    // the reason we check shard mapper from the query context as opposed to
    // the regular shard mapper of the SingleClusterPlanner is because the former allows us to
    // inject failures to test the code as well as verify shard level failover in production by
    // providing parameter downShards, for example downShards=tsdb1/us-east-1a/raw/6,7,8
    if (!localShardMapper.allShardsActive && buddyShardMapper.allShardsActive) {
      // all remote case
      getRemoteDispatcherForShard(shard, queryContext)
    } else if (localShardMapper.shards(shard).active) {
      val targetActor = shardMapperFunc.coordForShard(shard)
      // this means that the shard went down in the split second after high availability took
      // a snapshot of the shard state
      if (targetActor == ActorRef.noSender) {
        getRemoteDispatcherForShard(shard, queryContext)
      } else {
        ActorPlanDispatcher(targetActor, clusterName)
      }
    } else {
      getRemoteDispatcherForShard(shard, queryContext)
    }
  }

  private def getRemoteDispatcherForShard(shard: Int, queryContext: QueryContext): PlanDispatcher = {
    val remoteShardMapper =
      queryContext.plannerParams.buddyShardMapper.getOrElse(throw new RuntimeException("Remote shard map unavailable"))
    val shardInfo = remoteShardMapper.shards(shard)
    if (!shardInfo.active) {
      if (queryContext.plannerParams.allowPartialResults)
        logger.debug(s"Shard: $shard is not available however query is proceeding as partial results is enabled")
      else {
        shardUnavailableFailoverCounter.increment()
        throw new filodb.core.query.ServiceUnavailableException(
          s"Remote Buddy Shard: $shard is not available"
        )
      }
    }
    val dispatcher = RemoteActorPlanDispatcher(shardInfo.address, clusterName)
    dispatcher
  }

  private def makeBuddyExecPlanIfNeeded(qContext: QueryContext, ep: ExecPlan): ExecPlan = {
    // if we use shard level failover mode and the dispatcher that we have
    // received from dispatcherForShard call is a dispatcher for a remote buddy shard
    // we need to wrap the execution plan in GenericRemoteExec
    // GenericRemoteExec exists only because we cannot directly query the shards
    // from a coordinator. GenericRemoteExec will pass it to a machine that can resolve
    // ActorRef of a remote shard and dispatch to the appropriate actor.
    val pp = qContext.plannerParams
    if (
      pp.failoverMode == ShardLevelFailoverMode &&
      (!pp.buddyShardMapper.get.allShardsActive) && // not shipping the entire plan remotely
      ep.dispatcher.isInstanceOf[RemoteActorPlanDispatcher]
    ) {
      GenericRemoteExec(
        GrpcPlanDispatcher(pp.buddyGrpcEndpoint.get, pp.buddyGrpcTimeoutMs.get),
        ep
      )
    } else ep
  }

  /**
   * Change start time of logical plan according to earliestRetainedTimestampFn
   */
  private def updateStartTime(logicalPlan: LogicalPlan): Option[LogicalPlan] = {
    val periodicSeriesPlan = LogicalPlanUtils.getPeriodicSeriesPlan(logicalPlan)
    if (periodicSeriesPlan.isEmpty) Some(logicalPlan)
    else {
      //For binary join LHS & RHS should have same times
      val boundParams = periodicSeriesPlan.get.head match {
        case p: PeriodicSeries => (p.startMs, p.stepMs, WindowConstants.staleDataLookbackMillis,
          p.offsetMs.getOrElse(0L), p.atMs.getOrElse(0), p.endMs)
        case w: PeriodicSeriesWithWindowing => (w.startMs, w.stepMs, w.window, w.offsetMs.getOrElse(0L),
          w.atMs.getOrElse(0L), w.endMs)
        case _  => throw new UnsupportedOperationException(s"Invalid plan: ${periodicSeriesPlan.get.head}")
      }

      val newStartMs = boundToStartTimeToEarliestRetained(boundParams._1, boundParams._2, boundParams._3,
        boundParams._4)
      if (newStartMs <= boundParams._6) { // if there is an overlap between query and retention ranges
        if (newStartMs != boundParams._1)
          Some(LogicalPlanUtils.copyLogicalPlanWithUpdatedSeconds(
            logicalPlan, newStartMs / 1000, boundParams._6 / 1000))
        else Some(logicalPlan)
      } else { // query is outside retention period, simply return empty result
        None
      }
    }
  }

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    val plannerParams = qContext.plannerParams
    val updatedPlan = updateStartTime(logicalPlan)
    if (updatedPlan.isEmpty) EmptyResultExec(qContext, dsRef, inProcessPlanDispatcher)
    else {
     val logicalPlan = updatedPlan.get
      val timeSplitConfig = if (plannerParams.timeSplitEnabled)
        (plannerParams.timeSplitEnabled, plannerParams.minTimeRangeForSplitMs, plannerParams.splitSizeMs)
      else (timeSplitEnabled, minTimeRangeForSplitMs, splitSizeMs)

      if (shardMapperFunc.numShards <= 0) throw new IllegalStateException("No shards available")
      val logicalPlans = if (updatedPlan.get.isInstanceOf[PeriodicSeriesPlan])
        LogicalPlanUtils.splitPlans(updatedPlan.get, qContext, timeSplitConfig._1,
          timeSplitConfig._2, timeSplitConfig._3)
      else
        Seq(logicalPlan)
      val materialized = logicalPlans match {
        case Seq(one) => materializeTimeSplitPlan(one, qContext, false)
        case many =>
          val materializedPlans = many.map(materializeTimeSplitPlan(_, qContext, false))
          val targetActor = PlannerUtil.pickDispatcher(materializedPlans)

          // create SplitLocalPartitionDistConcatExec that will execute child execplanss sequentially and stitches
          // results back with StitchRvsMapper transformer.
          val stitchPlan = SplitLocalPartitionDistConcatExec(qContext, targetActor, materializedPlans,
            rvRangeFromPlan(logicalPlan))
          stitchPlan
      }
      logger.debug(s"Materialized logical plan for dataset=$dsRef :" +
        s" $logicalPlan to \n${materialized.printTree()}")
      numPlansMaterialized.increment()
      materialized
    }
  }

  private def materializeTimeSplitPlan(logicalPlan: LogicalPlan,
                                       qContext: QueryContext,
                                       forceInProcess: Boolean): ExecPlan = {
    val materialized = walkLogicalPlanTree(logicalPlan, qContext, forceInProcess)
    match {
      case PlanResult(Seq(justOne), stitch) =>
        if (stitch) justOne.addRangeVectorTransformer(StitchRvsMapper(rvRangeFromPlan(logicalPlan)))
        justOne
      case PlanResult(many, stitch) =>
        val targetActor = PlannerUtil.pickDispatcher(many)
        many.head match {
          case _: LabelValuesExec => LabelValuesDistConcatExec(qContext, targetActor, many)
          case _: LabelNamesExec => LabelNamesDistConcatExec(qContext, targetActor, many)
          case _: TsCardExec => TsCardReduceExec(qContext, targetActor, many)
          case _: LabelCardinalityExec =>
            val reduceExec = LabelCardinalityReduceExec(qContext, targetActor, many)
            // Presenter here is added separately which use the bytes representing the sketch to get an estimate
            // of cardinality. The presenter is kept separate from DistConcatExec to enable multi partition queries
            // later if needed. The DistConcatExec's from multiple partitions can still return the map of label names
            // and bytes and they can be merged to create a new sketch. Only the top level exec needs to then add the
            // presenter to display the final mapping of label name and the count based on the sketch bytes.
            if (!qContext.plannerParams.skipAggregatePresent) {
              reduceExec.addRangeVectorTransformer(new LabelCardinalityPresenter())
            }
            reduceExec
          case _: PartKeysExec => PartKeysDistConcatExec(qContext, targetActor, many)
          case _: ExecPlan =>
            val topPlan = LocalPartitionDistConcatExec(qContext, targetActor, many)
            if (stitch) topPlan.addRangeVectorTransformer(StitchRvsMapper(rvRangeFromPlan(logicalPlan)))
            topPlan
        }
    }
    logger.debug(s"Materialized logical plan for dataset=$dsRef :" +
      s" $logicalPlan to \n${materialized.printTree()}")
    materialized
  }

  /**
   * Returns the set of shards that contain query data.
   * @param shardPairs all shard-key (column,value) pairs to be queried.
   */
  def shardsFromValues(shardPairs: Seq[(String, String)],
                       filters: Seq[ColumnFilter],
                       qContext: QueryContext,
                       startMs: Long,
                       endMs: Long): Seq[Int] = {

    val spreadProvToUse = qContext.plannerParams.spreadOverride.getOrElse(spreadProvider)

    val metric = shardPairs.find(_._1 == dsOptions.metricColumn)
      .map(_._2)
      .getOrElse(throw new BadQueryException(s"Could not find metric value"))

    val shardValues = shardPairs.filterNot(_._1 == dsOptions.metricColumn).map(_._2)

    logger.debug(s"For shardColumns $shardColumns, extracted metric $metric and shard values $shardValues")
    val targetSchemaChange = targetSchemaProvider(qContext).targetSchemaFunc(filters)
    val targetSchema = {
      if (targetSchemaChange.nonEmpty) {
        findTargetSchema(targetSchemaChange, startMs, endMs).map(tsc => tsc.schema).getOrElse(Seq.empty)
      } else Seq.empty
    }
    val shardHash = RecordBuilder.shardKeyHash(shardValues, dsOptions.metricColumn, metric, targetSchema)
    if(useTargetSchemaForShards(filters, startMs, endMs, qContext)) {
      val nonShardKeyLabelPairs = filters.filter(f => !shardColumns.contains(f.column)
        && f.filter.isInstanceOf[Filter.Equals])
        .map(cf => cf.column ->
          cf.filter.asInstanceOf[Filter.Equals].value.toString).toMap
      val partitionHash = RecordBuilder.partitionKeyHash(nonShardKeyLabelPairs, shardPairs.toMap, targetSchema,
        dsOptions.metricColumn, metric)
      // since target-schema filter is provided in the query, ingestionShard can be used to find the single shard
      // that can answer the query.
      Seq(shardMapperFunc.ingestionShard(shardHash, partitionHash, spreadProvToUse.spreadFunc(filters).last.spread))
    } else {
      shardMapperFunc.queryShards(shardHash, spreadProvToUse.spreadFunc(filters).last.spread)
    }
  }

  // scalastyle:off method.length
  def shardsFromFilters(filters: Seq[ColumnFilter],
                        qContext: QueryContext,
                        startMs: Long,
                        endMs: Long): Seq[Int] = {

    require(shardColumns.nonEmpty || qContext.plannerParams.shardOverrides.nonEmpty,
      s"Dataset $dsRef does not have shard columns defined, and shard overrides were not mentioned")

    qContext.plannerParams.shardOverrides.getOrElse {
      val shardColToValues: Seq[(String, Seq[String])] = shardColumns.map { shardCol =>
        // To compute the shard hash, filters must match all shard columns either by equality or EqualsRegex,
        //   where any match by EqualsRegex can use at most the '|' regex character.
        val values = filters.find(f => f.column == shardCol) match {
          case Some(ColumnFilter(_, Filter.Equals(filtVal: String))) =>
            Seq(filtVal)
          case Some(ColumnFilter(_, Filter.EqualsRegex(filtVal: String)))
            if QueryUtils.containsPipeOnlyRegex(filtVal) => QueryUtils.splitAtUnescapedPipes(filtVal).distinct
          case Some(ColumnFilter(_, filter)) =>
            throw new BadQueryException(s"Found filter for shard column $shardCol but " +
              s"$filter cannot be used for shard key routing")
          case _ =>
            throw new BadQueryException(s"Could not find filter for shard key column " +
              s"$shardCol, shard key hashing disabled")
        }
        val trimmedValues = values.map(value => RecordBuilder.trimShardColumn(dsOptions, shardCol, value))
        (shardCol, trimmedValues)
      }

      // Find the union of all shards for each shard-key.
      val shardKeys = QueryUtils.makeAllKeyValueCombos(shardColToValues.toMap)
      shardKeys.flatMap{ shardKey =>
        // Replace any EqualsRegex shard-key filters with Equals.
        val newFilters = filters.map{ filt =>
            shardKey.get(filt.column)
              .map(value => ColumnFilter(filt.column, Filter.Equals(value)))
              .getOrElse(filt)
          }
        shardsFromValues(shardKey.toSeq, newFilters, qContext, startMs, endMs)
      }.distinct
    }
  }
  // scalastyle:off method.length

  /**
    * Renames Prom AST __name__ metric name filters to one based on the actual metric column of the dataset,
    * if it is not the prometheus standard
    */
  def renameMetricFilter(filters: Seq[ColumnFilter]): Seq[ColumnFilter] =
    if (dsOptions.metricColumn != PromMetricLabel) {
      filters map {
        case ColumnFilter(PromMetricLabel, filt) => ColumnFilter(dsOptions.metricColumn, filt)
        case other: ColumnFilter                 => other
      }
    } else {
      filters
    }

  /**
    * Walk logical plan tree depth-first and generate execution plans starting from the bottom
    *
    * @return ExecPlans that answer the logical plan provided
    */
  // scalastyle:off cyclomatic.complexity
  override def walkLogicalPlanTree(logicalPlan: LogicalPlan,
                                   qContext: QueryContext,
                                   forceInProcess: Boolean): PlanResult = {
     logicalPlan match {
      case lp: RawSeries                   => materializeRawSeries(qContext, lp, forceInProcess)
      case lp: RawChunkMeta                => materializeRawChunkMeta(qContext, lp, forceInProcess)
      case lp: PeriodicSeries              => materializePeriodicSeries(qContext, lp, forceInProcess)
      case lp: PeriodicSeriesWithWindowing => materializePeriodicSeriesWithWindowing(qContext, lp, forceInProcess)
      case lp: ApplyInstantFunction        => materializeApplyInstantFunction(qContext, lp, forceInProcess)
      case lp: ApplyInstantFunctionRaw     => materializeApplyInstantFunctionRaw(qContext, lp, forceInProcess)
      case lp: Aggregate                   => materializeAggregate(qContext, lp, forceInProcess)
      case lp: BinaryJoin                  => materializeBinaryJoin(qContext, lp, forceInProcess)
      case lp: ScalarVectorBinaryOperation => materializeScalarVectorBinOp(qContext, lp, forceInProcess)
      case lp: LabelValues                 => materializeLabelValues(qContext, lp, forceInProcess)
      case lp: LabelNames                  => materializeLabelNames(qContext, lp, forceInProcess)
      case lp: TsCardinalities             => materializeTsCardinalities(qContext, lp, forceInProcess)
      case lp: SeriesKeysByFilters         => materializeSeriesKeysByFilters(qContext, lp, forceInProcess)
      case lp: ApplyMiscellaneousFunction  => materializeApplyMiscellaneousFunction(qContext, lp, forceInProcess)
      case lp: ApplySortFunction           => materializeApplySortFunction(qContext, lp, forceInProcess)
      case lp: ScalarVaryingDoublePlan     => materializeScalarPlan(qContext, lp, forceInProcess)
      case lp: ScalarTimeBasedPlan         => materializeScalarTimeBased(qContext, lp)
      case lp: VectorPlan                  => materializeVectorPlan(qContext, lp, forceInProcess)
      case lp: ScalarFixedDoublePlan       => materializeFixedScalar(qContext, lp)
      case lp: ApplyAbsentFunction         => materializeAbsentFunction(qContext, lp, forceInProcess)
      case lp: ApplyLimitFunction          => materializeLimitFunction(qContext, lp, forceInProcess)
      case lp: ScalarBinaryOperation       => materializeScalarBinaryOperation(qContext, lp, forceInProcess)
      case lp: SubqueryWithWindowing       => materializeSubqueryWithWindowing(qContext, lp, forceInProcess)
      case lp: TopLevelSubquery            => materializeTopLevelSubquery(qContext, lp, forceInProcess)
      case lp: LabelCardinality            => materializeLabelCardinality(qContext, lp, forceInProcess)
      //case _                               => throw new BadQueryException("Invalid logical plan")
    }
  }
  // scalastyle:on cyclomatic.complexity

  // scalastyle:off method.length
  /**
   * Returns the shards spanned by a LogicalPlan's data.
   * @return an occupied Option iff shards could be determined for all leaf-level plans.
   */
  def getShardSpanFromLp(lp: LogicalPlan,
                                 qContext: QueryContext): Option[Set[Int]] = {

    case class LeafInfo(lp: LogicalPlan,
                        startMs: Long,
                        endMs: Long,
                        renamedFilters: Seq[ColumnFilter])

    // construct a LeafInfo for each leaf...
    val leafInfos = new ArrayBuffer[LeafInfo]()
    for (leafPlan <- findLeafLogicalPlans(lp)) {
      val filters = getColumnFilterGroup(leafPlan).map(_.toSeq)
      assert(filters.size == 1, s"expected leaf plan to yield single filter group, but got ${filters.size}")
      leafPlan match {
        case rs: RawSeries =>
          // Get time params from the RangeSelector, and use them to identify a TargetSchemaChanges.
          val startEndMsPairOpt = rs.rangeSelector match {
            case is: IntervalSelector => Some((is.from, is.to))
            case _ => None
          }
          val (startMs, endMs): (Long, Long) = startEndMsPairOpt.getOrElse((0, Long.MaxValue))
          leafInfos.append(LeafInfo(leafPlan, startMs, endMs, renameMetricFilter(filters.head)))
        // Do nothing; not pulling data from any shards.
        case sc: ScalarPlan => { }
        // Note!! If an unrecognized plan type is encountered, this just pessimistically returns None.
        case _ => return None
      }
    }

    // if we can't extract shards from all filters, return an empty result
    if (!leafInfos.forall{ leaf =>
      canGetShardsFromFilters(leaf.renamedFilters, qContext)
    }) return None

    val shards = leafInfos.flatMap( leaf =>
      shardsFromFilters(leaf.renamedFilters, qContext, leaf.startMs, leaf.endMs)).toSet

    Some(shards)
  }
  // scalastyle:on method.length

  private def getPushdownShardsInner(qContext: QueryContext,
                                plan: LogicalPlan): Option[Set[Int]] = {
    val getRawPushdownShards = (rs: RawSeries) => {
      if (qContext.plannerParams.shardOverrides.isEmpty) {
        getShardSpanFromLp(rs, qContext).get
      } else qContext.plannerParams.shardOverrides.get.toSet
    }
    LogicalPlanUtils.getPushdownKeys(
      plan,
      targetSchemaProvider(qContext),
      dataset.options.nonMetricShardColumns,
      getRawPushdownShards,
      rs => LogicalPlan.getRawSeriesFilters(rs))
  }

  /**
   * Returns a set of shards iff a plan can be pushed-down to each.
   * See [[LogicalPlanUtils.getPushdownKeys]] for more info.
   */
  private def getPushdownShards(qContext: QueryContext,
                                plan: LogicalPlan): Option[Set[Int]] = {
    shardPushdownCache match {
      case Some(cache) =>
        cache.get((plan, qContext.plannerParams.shardOverrides), _ => {
          getPushdownShardsInner(qContext, plan)
        })
      case None =>
        getPushdownShardsInner(qContext, plan)
    }
  }

  /**
   * Materialize a BinaryJoin without the pushdown optimization.
   * @param forceDispatcher If occupied, forces this BinaryJoin to be materialized with the dispatcher.
   *                        Only this root plan has its dispatcher forced; children are unaffected.
   *                        ####### The dispatcher is applied regardless of forceInProcess #######
   */
  private def materializeBinaryJoinNoPushdown(qContext: QueryContext,
                                              lp: BinaryJoin,
                                              forceInProcess: Boolean,
                                              forceDispatcher: Option[PlanDispatcher]): PlanResult = {
    val lhs = walkLogicalPlanTree(lp.lhs, qContext, forceInProcess)
    val stitchedLhs = if (lhs.needsStitch) Seq(StitchRvsExec(qContext,
      PlannerUtil.pickDispatcher(lhs.plans), rvRangeFromPlan(lp), lhs.plans))
    else lhs.plans
    val rhs = walkLogicalPlanTree(lp.rhs, qContext, forceInProcess)
    val stitchedRhs = if (rhs.needsStitch) Seq(StitchRvsExec(qContext,
      PlannerUtil.pickDispatcher(rhs.plans), rvRangeFromPlan(lp), rhs.plans))
    else rhs.plans

    // TODO Currently we create separate exec plan node for stitching.
    // Ideally, we can go one step further and add capability to NonLeafNode plans to pre-process
    // and transform child results individually before composing child results together.
    // In theory, more efficient to use transformer than to have separate exec plan node to avoid IO.
    // In the interest of keeping it simple, deferring decorations to the ExecPlan. Add only if needed after measuring.

    val targetActor = forceDispatcher.getOrElse(PlannerUtil.pickDispatcher(stitchedLhs ++ stitchedRhs))
    val joined = if (lp.operator.isInstanceOf[SetOperator])
      Seq(exec.SetOperatorExec(qContext, targetActor, stitchedLhs, stitchedRhs, lp.operator,
        lp.on.map(LogicalPlanUtils.renameLabels(_, dsOptions.metricColumn)),
        LogicalPlanUtils.renameLabels(lp.ignoring, dsOptions.metricColumn), dsOptions.metricColumn,
        rvRangeFromPlan(lp)))
    else
      Seq(BinaryJoinExec(qContext, targetActor, stitchedLhs, stitchedRhs, lp.operator, lp.cardinality,
        lp.on.map(LogicalPlanUtils.renameLabels(_, dsOptions.metricColumn)),
        LogicalPlanUtils.renameLabels(lp.ignoring, dsOptions.metricColumn),
        LogicalPlanUtils.renameLabels(lp.include, dsOptions.metricColumn), dsOptions.metricColumn, rvRangeFromPlan(lp)))

    PlanResult(joined)
  }

  // scalastyle:off method.length
  /**
   * The pushdown optimization can be applied to both BinaryJoins and Aggregates; in both
   *   cases, the idea is roughly the same. Suppose we had the following BinaryJoin:
   *
   * BinaryJoin(lhs=[PeriodicSeries on shards 0,1], rhs=[PeriodicSeries on shards 0,1])
   *
   * This might be materialized as follows:
   *
   * E~BinaryJoinExec(binaryOp=ADD) on ActorPlanDispatcher
   * -T~PeriodicSamplesMapper()
   * --E~MultiSchemaPartitionsExec(shard=0) on ActorPlanDispatcher  // lhs
   * -T~PeriodicSamplesMapper()
   * --E~MultiSchemaPartitionsExec(shard=1) on ActorPlanDispatcher  // lhs
   * -T~PeriodicSamplesMapper()
   * --E~MultiSchemaPartitionsExec(shard=0) on ActorPlanDispatcher  // rhs
   * -T~PeriodicSamplesMapper()
   * --E~MultiSchemaPartitionsExec(shard=1) on ActorPlanDispatcher  // rhs
   *
   * Data is pulled from two shards, sent to the BinaryJoin actor, then that single actor
   *   needs to process all of this data.
   *
   * When (1) a target-schema is defined for all leaf-level filters, (2) all of these
   *   target schemas are identical, and (3) the join-key fully-specifies the
   *   target-schema labels, we can relieve much of this single-actor pressure.
   *   Lhs/rhs values will never be joined across shards, so the following ExecPlan
   *   would yield the same result as the above plan:
   *
   * E~LocalPartitionDistConcatExec()
   * -E~BinaryJoinExec(binaryOp=ADD) on ActorPlanDispatcher
   * --T~PeriodicSamplesMapper()
   * ---E~MultiSchemaPartitionsExec(shard=0) on InProcessPlanDispatcher
   * --T~PeriodicSamplesMapper()
   * ---E~MultiSchemaPartitionsExec(shard=0) on InProcessPlanDispatcher
   * -E~BinaryJoinExec(binaryOp=ADD) on ActorPlanDispatcher
   * --T~PeriodicSamplesMapper()
   * ---E~MultiSchemaPartitionsExec(shard=1) on InProcessPlanDispatcher
   * --T~PeriodicSamplesMapper()
   * ---E~MultiSchemaPartitionsExec(shard=1) on InProcessPlanDispatcher
   *
   * Now, data is joined locally and in smaller batches.
   *
   * This same idea can be applied to Aggregates, as well. A plan of the form:
   *
   * Aggregate(vectors=[PeriodicSeries on shards 0,1])
   *
   * might be materialized as:
   *
   * T~AggregatePresenter
   * -E~LocalPartitionReduceAggregateExec on ActorPlanDispatcher
   * --T~AggregateMapReduce
   * ---T~PeriodicSamplesMapper
   * ----E~MultiSchemaPartitionsExec(shard=0) on ActorPlanDispatcher
   * --T~AggregateMapReduce
   * ---T~PeriodicSamplesMapper
   * ----E~MultiSchemaPartitionsExec(shard=1) on ActorPlanDispatcher
   *
   * But, with the pushdown applied, this could instead be materialized as:
   *
   * E~LocalPartitionDistConcatExec on ActorPlanDispatcher
   * -T~AggregatePresenter
   * --E~LocalPartitionReduceAggregateExec on ActorPlanDispatcher
   * ---T~AggregateMapReduce
   * ----T~PeriodicSamplesMapper
   * -----E~MultiSchemaPartitionsExec(shard=0) on InProcessPlanDispatcher
   * -T~AggregatePresenter
   * --E~LocalPartitionReduceAggregateExec on ActorPlanDispatcher
   * ---T~AggregateMapReduce
   * ----T~PeriodicSamplesMapper
   * -----E~MultiSchemaPartitionsExec(shard=1) on InProcessPlanDispatcher
   *
   * For the sake of simplicity, we impose two additional prerequisites for this
   *   optimization on BinaryJoins:
   *     (1) the ExecPlans materialized by lhs/rhs must each draw data from only a single shard
   *     (2) the sets of shards from which lhs/rhs draw data must be identical
   *
   * @param shards The set of shards from which the argument plan draws its data.
   *                 If the argument plan is a BinaryJoin, lhs/rhs must individually draw
   *                 data from exactly the set of argument shards.
   */
  private def materializeWithPushdown(qContext: QueryContext,
                                      lp: LogicalPlan,
                                      shards: Set[Int],
                                      forceInProcess: Boolean): PlanResult = {
    // step through the shards, and materialize a plan for each
    val plans = shards.toSeq.flatMap{ shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess, qContext)
      val qContextWithShardOverride = {
        val shardOverridePlannerParams = qContext.plannerParams.copy(shardOverrides = Some(Seq(shard)))
        qContext.copy(plannerParams = shardOverridePlannerParams)
      }
      // force child plans to dispatch in-process (since they're all on the same shard)
      val eps = lp match {
        case bj: BinaryJoin =>
          materializeBinaryJoinNoPushdown(qContextWithShardOverride, bj,
            forceInProcess = true, forceDispatcher = Some(dispatcher)).plans
        case agg: Aggregate =>
          materializeAggregateNoPushdown(qContextWithShardOverride, agg,
            forceInProcess = true, forceDispatcher = Some(dispatcher)).plans
        case x => throw new IllegalArgumentException(s"unhandled type: ${x.getClass}")
      }
      val pp = qContext.plannerParams
      eps.map(ep => makeBuddyExecPlanIfNeeded(qContext, ep))
    }
    PlanResult(plans.toSeq)
  }
  // scalastyle:on method.length

  override def materializeBinaryJoin(qContext: QueryContext,
                                     lp: BinaryJoin,
                                     forceInProcess: Boolean): PlanResult = {

    optimizeOrVectorDouble(qContext, lp).getOrElse {

      // see materializeWithPushdown for details about the pushdown optimization.
      val pushdownShards = getPushdownShards(qContext, lp)
      if (pushdownShards.isDefined) {
        materializeWithPushdown(qContext, lp, pushdownShards.get, forceInProcess)
      } else {
        materializeBinaryJoinNoPushdown(qContext, lp, forceInProcess, None)
      }
    }
  }

  // scalastyle:off method.length
  override private[queryplanner] def materializePeriodicSeriesWithWindowing(qContext: QueryContext,
                                                     lp: PeriodicSeriesWithWindowing,
                                                     forceInProcess: Boolean): PlanResult = {

    val (nameFilter: Option[String], leFilter: Option[String], logicalPlanWithoutBucket: PeriodicSeriesWithWindowing) =
      if (queryConfig.translatePromToFilodbHistogram) {
       val result = removeBucket(Right(lp))
        (result._1, result._2, result._3.right.get)
    } else (None, None, lp)

    val series = walkLogicalPlanTree(logicalPlanWithoutBucket.series, qContext, forceInProcess)
    val rawSource = logicalPlanWithoutBucket.series.isRaw

    /* Last function is used to get the latest value in the window for absent_over_time
    If no data is present AbsentFunctionMapper will return range vector with value 1 */

    val execRangeFn = if (logicalPlanWithoutBucket.function == RangeFunctionId.AbsentOverTime) Last
                      else InternalRangeFunction.lpToInternalFunc(logicalPlanWithoutBucket.function)

    val realScanStartMs = lp.atMs.getOrElse(logicalPlanWithoutBucket.startMs)
    val realScanEndMs = lp.atMs.getOrElse(logicalPlanWithoutBucket.endMs)
    val realScanStepMs: Long = lp.atMs.map(_ => 0L).getOrElse(lp.stepMs)

    val paramsExec = materializeFunctionArgs(logicalPlanWithoutBucket.functionArgs, qContext)
    val window = if (execRangeFn == InternalRangeFunction.Timestamp) None else Some(logicalPlanWithoutBucket.window)
    series.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(realScanStartMs,
      realScanStepMs, realScanEndMs, window, Some(execRangeFn),
      logicalPlanWithoutBucket.stepMultipleNotationUsed,
      paramsExec, logicalPlanWithoutBucket.offsetMs, rawSource)))

    // Add the le filter transformer to select the required bucket
    (nameFilter, leFilter) match {
      case (Some(filter), Some (le)) if filter.endsWith("_bucket") => {
        // if le filter matches for Inf or +Inf, set the double value to PositiveInfinity
        val doubleVal = le match {
          case "+Inf" => Double.PositiveInfinity
          case "Inf" => Double.PositiveInfinity
          case _ => le.toDouble
        }
        val paramsExec = StaticFuncArgs(doubleVal, RangeParams(realScanStartMs / 1000,
          realScanStepMs / 1000, realScanEndMs / 1000))
        series.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(HistogramBucket,
          Seq(paramsExec))))
      }
      case _ => //NOP
    }

    val result = if (logicalPlanWithoutBucket.function == RangeFunctionId.AbsentOverTime) {
      val aggregate = Aggregate(AggregationOperator.Sum, logicalPlanWithoutBucket, Nil,
                                AggregateClause.byOpt(Seq("job")))
      // Add sum to aggregate all child responses
      // If all children have NaN value, sum will yield NaN and AbsentFunctionMapper will yield 1
      val aggregatePlanResult = PlanResult(Seq(addAggregator(aggregate, qContext.copy(plannerParams =
        qContext.plannerParams.copy(skipAggregatePresent = true)), series)))
      addAbsentFunctionMapper(aggregatePlanResult, logicalPlanWithoutBucket.columnFilters,
        RangeParams(realScanStartMs / 1000, realScanStepMs / 1000, realScanEndMs / 1000), qContext)

    } else series

    // repeat the same value for each step if '@' is specified
    if (lp.atMs.nonEmpty) {
      result.plans.foreach(p => p.addRangeVectorTransformer(RepeatTransformer(lp.startMs, lp.stepMs, lp.endMs,
        p.queryWithPlanName(qContext))))
    }
    result
  }
  // scalastyle:on method.length

  override private[queryplanner] def removeBucket(lp: Either[PeriodicSeries, PeriodicSeriesWithWindowing]) = {
    val rawSeries = lp match {
      case Right(value) => value.series
      case Left(value)  => value.rawSeries
    }

    rawSeries match {
      case rawSeriesLp: RawSeries =>

        val nameFilter = rawSeriesLp.filters.find(_.column.equals(PromMetricLabel)).
          map(_.filter.valuesStrings.head.toString)
        val leFilter = rawSeriesLp.filters.find(_.column == "le").map(_.filter.valuesStrings.head.toString)

        if (nameFilter.isEmpty) (nameFilter, leFilter, lp)
        else {
          // the convention for histogram bucket queries is to have the "_bucket" string in the suffix
          if (!nameFilter.get.endsWith("_bucket")) {
            (nameFilter, leFilter, lp)
          }
          else {
            val filtersWithoutBucket = rawSeriesLp.filters.filterNot(_.column.equals(PromMetricLabel)).
              filterNot(_.column == "le") :+ ColumnFilter(PromMetricLabel,
              Equals(PlannerUtil.replaceLastBucketOccurenceStringFromMetricName(nameFilter.get)))
            val newLp =
              if (lp.isLeft)
                Left(lp.left.get.copy(rawSeries = rawSeriesLp.copy(filters = filtersWithoutBucket)))
              else
                Right(lp.right.get.copy(series = rawSeriesLp.copy(filters = filtersWithoutBucket)))
            (nameFilter, leFilter, newLp)
          }
        }
      case _ => (None, None, lp)
    }
  }
  override private[queryplanner] def materializePeriodicSeries(qContext: QueryContext,
                                        lp: PeriodicSeries,
                                        forceInProcess: Boolean): PlanResult = {

   // Convert to FiloDB histogram by removing le label and bucket prefix
   // _sum and _count are removed in MultiSchemaPartitionsExec since we need to check whether there is a metric name
   // with _sum/_count as suffix
    val (nameFilter: Option[String], leFilter: Option[String], lpWithoutBucket: PeriodicSeries) =
    if (queryConfig.translatePromToFilodbHistogram) {
     val result = removeBucket(Left(lp))
      (result._1, result._2, result._3.left.get)

    } else (None, None, lp)

    val realScanStartMs = lp.atMs.getOrElse(lp.startMs)
    val realScanEndMs = lp.atMs.getOrElse(lp.endMs)
    val realScanStepMs: Long = lp.atMs.map(_ => 0L).getOrElse(lp.stepMs)

    val rawSeries = walkLogicalPlanTree(lpWithoutBucket.rawSeries, qContext, forceInProcess)
    rawSeries.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(realScanStartMs, realScanStepMs,
      realScanEndMs, None, None, stepMultipleNotationUsed = false, Nil, lp.offsetMs)))

    if (nameFilter.isDefined && nameFilter.head.endsWith("_bucket") && leFilter.isDefined) {
      val paramsExec = StaticFuncArgs(leFilter.head.toDouble, RangeParams(realScanStartMs/1000, realScanStepMs/1000,
        realScanEndMs/1000))
      rawSeries.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(HistogramBucket,
        Seq(paramsExec))))
    }
    // repeat the same value for each step if '@' is specified
    if (lp.atMs.nonEmpty) {
      rawSeries.plans.foreach(p => p.addRangeVectorTransformer(RepeatTransformer(lp.startMs, lp.stepMs, lp.endMs,
        p.queryWithPlanName(qContext))))
    }
    rawSeries
  }

  /**
    * Calculates the earliest startTime possible given the query's start/window/step/offset.
    * This is used to bound the startTime of queries so we dont create possibility of aggregating
    * partially expired data and return incomplete results.
    *
    * @return new startTime to be used for query in ms. If original startTime is within retention
    *         period, returns it as is.
    */
  private def boundToStartTimeToEarliestRetained(startMs: Long, stepMs: Long,
                                                 windowMs: Long, offsetMs: Long): Long = {
    // In case query is earlier than earliestRetainedTimestamp then we need to drop the first few instants
    // to prevent inaccurate results being served. Inaccuracy creeps in because data can be in memory for which
    // equivalent data may not be in cassandra. Aggregations cannot be guaranteed to be complete.
    // Also if startMs < 500000000000 (before 1985), it is usually a unit test case with synthetic data, so don't fiddle
    // around with those queries
    val earliestRetainedTimestamp = earliestRetainedTimestampFn
    if (startMs - windowMs - offsetMs < earliestRetainedTimestamp && startMs > 500000000000L) {
      // We calculate below number of steps/instants to drop. We drop instant if data required for that instant
      // doesnt fully fall into the retention period. Data required for that instant involves
      // going backwards from that instant up to windowMs + offsetMs milli-seconds.
      val numStepsBeforeRetention = (earliestRetainedTimestamp - startMs + windowMs + offsetMs) / stepMs
      val lastInstantBeforeRetention = startMs + numStepsBeforeRetention * stepMs
      lastInstantBeforeRetention + stepMs
    } else {
      startMs
    }
  }

  /**
    * If there is a _type_ filter, return it.
    */
  private def extractSchemaFilter(filters: Seq[ColumnFilter]): Option[String] = {
    var schemaOpt: Option[String] = None
    filters.foreach { case ColumnFilter(label, filt) =>
      val isTypeFilt = label == TypeLabel
      if (isTypeFilt) filt match {
        case Filter.Equals(schema) => schemaOpt = Some(schema.asInstanceOf[String])
        case x: Any                 => throw new IllegalArgumentException(s"Illegal filter $x on _type_")
      }
      isTypeFilt
    }
    schemaOpt
  }

  // scalastyle:off method.length
  private def materializeRawSeries(qContext: QueryContext,
                                   lp: RawSeries,
                                   forceInProcess: Boolean): PlanResult = {
    val spreadProvToUse = qContext.plannerParams.spreadOverride.getOrElse(spreadProvider)
    val offsetMillis: Long = lp.offsetMs.getOrElse(0)
    val colName = lp.columns.headOption
    val renamedFilters = renameMetricFilter(lp.filters)
    val schemaOpt = extractSchemaFilter(renamedFilters)
    val spreadChanges = spreadProvToUse.spreadFunc(renamedFilters)

    val rangeSelectorWithOffset = lp.rangeSelector match {
      case IntervalSelector(fromMs, toMs) => IntervalSelector(fromMs - offsetMillis - lp.lookbackMs.getOrElse(
                                             queryConfig.staleSampleAfterMs), toMs - offsetMillis)
      case _                              => lp.rangeSelector
    }
    val needsStitch = rangeSelectorWithOffset match {
      case IntervalSelector(from, to) => spreadChanges.exists(c => c.time >= from && c.time <= to)
      case _                          => false
    }

    val (startMs, endMs): (Long, Long) = rangeSelectorWithOffset match {
      case IntervalSelector(from, to) => (from, to)
      case _ => (0, Long.MaxValue)
    }

    val shardKeyFilters = LogicalPlan.getNonMetricShardKeyFilters(lp, dataset.options.nonMetricShardColumns)
    assert(shardKeyFilters.size == 1, "RawSeries with more than one shard-key group: " + lp)
    val targetSchemaChangesExist = isTargetSchemaChanging(shardKeyFilters.head, startMs, endMs, qContext)

    val execPlans = shardsFromFilters(renamedFilters, qContext, startMs, endMs).map { shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess, qContext)
      val ep = MultiSchemaPartitionsExec(
        qContext, dispatcher, dsRef, shard, renamedFilters,
        toChunkScanMethod(rangeSelectorWithOffset), dsOptions.metricColumn, schemaOpt, colName
      )
      // if we use shard level failover mode and the dispatcher that we have
      // received from dispatcherForShard call is a dispatcher for a remote buddy shard
      // we need to wrap it in GenericRemoteExec
      // GenericRemoteExec exists only because we cannot directly query the shards
      // from a coordinator. GenericRemoteExec will pass it to a machine that can resolve
      // ActorRef of a remote shard.
      makeBuddyExecPlanIfNeeded(qContext, ep)
    }

    // Stitch only if spread changes during the query-window.
    // When a target-schema changes during query window, data might be ingested in
    //   different shards after the change.
    PlanResult(execPlans, needsStitch || targetSchemaChangesExist)
  }
  // scalastyle:on method.length

  private def materializeLabelValues(qContext: QueryContext,
                                     lp: LabelValues,
                                     forceInProcess: Boolean): PlanResult = {
    // If the label is PromMetricLabel and is different than dataset's metric name,
    // replace it with dataset's metric name. (needed for prometheus plugins)
    val metricLabelIndex = lp.labelNames.indexOf(PromMetricLabel)
    val labelNames = if (metricLabelIndex > -1 && dsOptions.metricColumn != PromMetricLabel)
      lp.labelNames.updated(metricLabelIndex, dsOptions.metricColumn) else lp.labelNames

    val renamedFilters = renameMetricFilter(lp.filters)
    val shardsToHit = if (canGetShardsFromFilters(renamedFilters, qContext)) {
      shardsFromFilters(renamedFilters, qContext, lp.startMs, lp.endMs)
    } else {
      mdNoShardKeyFilterRequests.increment()
      shardMapperFunc.assignedShards
    }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess, qContext)
      val ep = exec.LabelValuesExec(
        qContext, dispatcher, dsRef, shard, renamedFilters, labelNames, lp.startMs, lp.endMs
      )
      makeBuddyExecPlanIfNeeded(qContext, ep)
    }
    PlanResult(metaExec)
  }

  // allow metadataQueries to get list of shards from shardKeyFilters only if all shardCols have Equals filter
  //   or EqualsRegex filter with only the pipe special character.
  private def canGetShardsFromFilters(renamedFilters: Seq[ColumnFilter],
                                      qContext: QueryContext): Boolean = {
    if (qContext.plannerParams.shardOverrides.isEmpty && shardColumns.nonEmpty) {
      shardColumns.toSet.subsetOf(renamedFilters.map(_.column).toSet) &&
        shardColumns.forall { shardCol =>
          // So to compute the shard hash we need shardCol == value filter (exact equals) for each shardColumn
          renamedFilters.find(f => f.column == shardCol) match {
            case Some(ColumnFilter(_, Filter.Equals(_: String))) => true
            case Some(ColumnFilter(_, Filter.EqualsRegex(value: String))) =>
              // Make sure no regex chars except the pipe, which can be used to concatenate values.
              QueryUtils.containsPipeOnlyRegex(value)
            case _ => false
          }
        }
    } else false
  }

  private def materializeLabelNames(qContext: QueryContext,
                                    lp: LabelNames,
                                    forceInProcess: Boolean): PlanResult = {
    val renamedFilters = renameMetricFilter(lp.filters)
    val shardsToHit = if (canGetShardsFromFilters(renamedFilters, qContext)) {
      shardsFromFilters(renamedFilters, qContext, lp.startMs, lp.endMs)
    } else {
      mdNoShardKeyFilterRequests.increment()
      shardMapperFunc.assignedShards
    }

    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess, qContext)
      val ep = exec.LabelNamesExec(qContext, dispatcher, dsRef, shard, renamedFilters, lp.startMs, lp.endMs)
      makeBuddyExecPlanIfNeeded(qContext, ep)
    }

    PlanResult(metaExec)
  }

  private def materializeLabelCardinality(qContext: QueryContext,
                                    lp: LabelCardinality,
                                          forceInProcess: Boolean): PlanResult = {
    val renamedFilters = renameMetricFilter(lp.filters)
    val shardsToHit = shardsFromFilters(renamedFilters, qContext, lp.startMs, lp.endMs)

    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess, qContext)
      val ep = exec.LabelCardinalityExec(qContext, dispatcher, dsRef, shard, renamedFilters, lp.startMs, lp.endMs)
      makeBuddyExecPlanIfNeeded(qContext, ep)
    }
    PlanResult(metaExec)
  }

  private def materializeTsCardinalities(qContext: QueryContext,
                                         lp: TsCardinalities,
                                         forceInProcess: Boolean): PlanResult = {
    // If no clusterName is passed in the logical plan, we use the passed clusterName in the SingleClusterPlanner
    // We are using the passed cluster name in logical plan for tenant metering apis
    val clusterNameToPass = lp.overrideClusterName match {
      case "" => clusterName
      case _ => lp.overrideClusterName
    }
    val metaExec = shardMapperFunc.assignedShards.map{ shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess, qContext)
      val ep = exec.TsCardExec(
        qContext, dispatcher, dsRef, shard, lp.shardKeyPrefix, lp.numGroupByFields, clusterNameToPass
      )
      makeBuddyExecPlanIfNeeded(qContext, ep)
    }
    PlanResult(metaExec)
  }

  private def materializeSeriesKeysByFilters(qContext: QueryContext,
                                             lp: SeriesKeysByFilters,
                                             forceInProcess: Boolean): PlanResult = {
    val renamedFilters = renameMetricFilter(lp.filters)

    val shardsToHit = if (canGetShardsFromFilters(renamedFilters, qContext)) {
      shardsFromFilters(renamedFilters, qContext, lp.startMs, lp.endMs)
    } else {
      mdNoShardKeyFilterRequests.increment()
      shardMapperFunc.assignedShards
    }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess, qContext)
      PartKeysExec(qContext, dispatcher, dsRef, shard, renamedFilters,
                   lp.fetchFirstLastSampleTimes, lp.startMs, lp.endMs)
    }
    PlanResult(metaExec)
  }

  private def materializeRawChunkMeta(qContext: QueryContext,
                                      lp: RawChunkMeta,
                                      forceInProcess: Boolean): PlanResult = {
    // Translate column name to ID and validate here
    val colName = if (lp.column.isEmpty) None else Some(lp.column)
    val renamedFilters = renameMetricFilter(lp.filters)
    val schemaOpt = extractSchemaFilter(renamedFilters)
    val metaExec = shardsFromFilters(renamedFilters, qContext, lp.startMs, lp.endMs).map { shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess, qContext)
      val ep = SelectChunkInfosExec(
        qContext, dispatcher, dsRef, shard, renamedFilters, toChunkScanMethod(lp.rangeSelector),
        schemaOpt, colName
      )
      makeBuddyExecPlanIfNeeded(qContext, ep)
    }
    PlanResult(metaExec)
  }

  /**
   * Materialize an Aggregate without the pushdown optimization.
   * @param forceDispatcher If occupied, forces this Aggregate to be materialized with the dispatcher.
   *                        Only this root plan has its dispatcher forced; children are unaffected.
   *                        ####### The dispatcher is applied regardless of forceInProcess #######
   */
  private def materializeAggregateNoPushdown(qContext: QueryContext,
                                   lp: Aggregate,
                                   forceInProcess: Boolean = false,
                                   forceDispatcher: Option[PlanDispatcher] = None): PlanResult = {
    val canPushdownInner = !LogicalPlanUtils.hasDescendantAggregateOrJoin(lp.vectors) ||
      getPushdownShards(qContext, lp.vectors).isDefined
    // Child plan should not skip Aggregate Present such as Topk in Sum(Topk)
    var toReduceLevel1 = walkLogicalPlanTree(lp.vectors,
      qContext.copy(plannerParams = qContext.plannerParams.copy(skipAggregatePresent = false)), forceInProcess)
    if (!canPushdownInner) {
      val dispatcher = PlannerUtil.pickDispatcher(toReduceLevel1.plans)
      val plan = if (toReduceLevel1.needsStitch) {
        StitchRvsExec(qContext, dispatcher, outputRvRange = None, children = toReduceLevel1.plans)
      } else if (toReduceLevel1.plans.size > 1) {
        LocalPartitionDistConcatExec(qContext, dispatcher, toReduceLevel1.plans)
      } else {
        toReduceLevel1.plans.head
      }
      toReduceLevel1 = PlanResult(Seq(plan))
    }
    val reducer = addAggregator(lp, qContext, toReduceLevel1, forceDispatcher)
    PlanResult(Seq(reducer)) // since we have aggregated, no stitching
  }

  override def materializeAggregate(qContext: QueryContext,
                                    lp: Aggregate,
                                    forceInProcess: Boolean): PlanResult = {
    // see materializeWithPushdown for details about the pushdown optimization.
    val pushdownShards = getPushdownShards(qContext, lp)
    if (pushdownShards.isDefined) {
      materializeWithPushdown(qContext, lp, pushdownShards.get, forceInProcess)
    } else {
      materializeAggregateNoPushdown(qContext, lp, forceInProcess)
    }
  }
}
