package filodb.coordinator.queryplanner

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon

import filodb.coordinator.{ActorPlanDispatcher, ShardMapper}
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.core.{SpreadProvider, StaticTargetSchemaProvider, TargetSchemaChange, TargetSchemaProvider}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.{Dataset, DatasetOptions, Schemas}
import filodb.core.query._
import filodb.core.query.Filter.Equals
import filodb.core.store.{AllChunkScan, ChunkScanMethod, InMemoryChunkScan, TimeRangeChunkScan, WriteBufferChunkScan}
import filodb.prometheus.ast.Vectors.{PromMetricLabel, TypeLabel}
import filodb.prometheus.ast.WindowConstants
import filodb.query.{exec, _}
import filodb.query.InstantFunctionId.HistogramBucket
import filodb.query.LogicalPlan._
import filodb.query.exec.{LocalPartitionDistConcatExec, _}
import filodb.query.exec.InternalRangeFunction.Last


// scalastyle:off file.size.limit

object SingleClusterPlanner {
  private val mdNoShardKeyFilterRequests = Kamon.counter("queryengine-metadata-no-shardkey-requests").withoutTags

  // Is TargetSchema changing during query window.
  private def isTargetSchemaChanging(targetSchemaChanges: Seq[TargetSchemaChange],
                                     startMs: Long, endMs: Long): Boolean =
    targetSchemaChanges.nonEmpty && targetSchemaChanges.exists(c => c.time >= startMs && c.time <= endMs)

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
                           shardKeyMatcher: Seq[ColumnFilter] => Seq[Seq[ColumnFilter]] =
                               PartitionLocationPlanner.equalsOnlyShardKeyMatcher,
                           timeSplitEnabled: Boolean = false,
                           minTimeRangeForSplitMs: => Long = 1.day.toMillis,
                           splitSizeMs: => Long = 1.day.toMillis)
                           extends QueryPlanner with StrictLogging with DefaultPlanner {
  override val dsOptions: DatasetOptions = schemas.part.options
  private val shardColumns = dsOptions.shardKeyColumns.sorted
  private val dsRef = dataset.ref

  val numPlansMaterialized = Kamon.counter("single-cluster-plans-materialized")
    .withTag("cluster", clusterName)
    .withTag("dataset", dataset.ref.dataset)

  private def targetSchemaProvider(qContext: QueryContext): TargetSchemaProvider = {
   qContext.plannerParams.targetSchemaProviderOverride.getOrElse(_targetSchemaProvider)
  }

  import SingleClusterPlanner._

  private def dispatcherForShard(shard: Int, forceInProcess: Boolean, queryContext: QueryContext): PlanDispatcher = {
    if (forceInProcess) {
      return inProcessPlanDispatcher
    }
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

  /**
   * If TargetSchema exists and all of the target-schema label filters are provided in the query, then return true.
   *
   * @param filters Query Column Filters
   * @param targetSchema TargetSchema
   * @return useTargetSchema - use target-schema to calculate query shards
   */
  private def useTargetSchemaForShards(filters: Seq[ColumnFilter],
                                       targetSchema: Option[TargetSchemaChange]): Boolean = {
    if (targetSchema.isEmpty || targetSchema.get.schema.isEmpty) {
      return false
    }
    // Make sure each target-schema column has a filter.
    targetSchema.get.schema
      .forall(tschemaCol => filters.exists(cf => {
        cf.column == tschemaCol &&
        (shardColumns.contains(tschemaCol) || cf.filter.isInstanceOf[Equals])
      }))
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
          p.offsetMs.getOrElse(0L), p.endMs)
        case w: PeriodicSeriesWithWindowing => (w.startMs, w.stepMs, w.window, w.offsetMs.getOrElse(0L), w.endMs)
        case _  => throw new UnsupportedOperationException(s"Invalid plan: ${periodicSeriesPlan.get.head}")
      }

      val newStartMs = boundToStartTimeToEarliestRetained(boundParams._1, boundParams._2, boundParams._3,
        boundParams._4)
      if (newStartMs <= boundParams._5) { // if there is an overlap between query and retention ranges
        if (newStartMs != boundParams._1)
          Some(LogicalPlanUtils.copyLogicalPlanWithUpdatedTimeRange(logicalPlan, TimeRange(newStartMs, boundParams._5)))
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

  def shardsFromValues(shardPairs: Seq[(String, String)],
                       filters: Seq[ColumnFilter],
                       qContext: QueryContext,
                       startMs: Long,
                       endMs: Long,
                       useTargetSchemaForShards: Seq[ColumnFilter] => Boolean = _ => false): Seq[Int] = {
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
    if(useTargetSchemaForShards(filters)) {
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
  def shardsFromFilters(rawFilters: Seq[ColumnFilter],
                        qContext: QueryContext,
                        startMs: Long,
                        endMs: Long,
                        useTargetSchemaForShards: Seq[ColumnFilter] => Boolean = _ => false): Seq[Int] = {

    require(shardColumns.nonEmpty || qContext.plannerParams.shardOverrides.nonEmpty,
      s"Dataset $dsRef does not have shard columns defined, and shard overrides were not mentioned")

    qContext.plannerParams.shardOverrides.getOrElse {
      val hasNonEqualsShardKeyFilter = rawFilters.exists { filter =>
        shardColumns.contains(filter.column) && !filter.filter.isInstanceOf[Equals]
      }
      val filterGroups = if (hasNonEqualsShardKeyFilter) {
        shardKeyMatcher(rawFilters).map(LogicalPlanUtils.upsertFilters(rawFilters, _))
      } else {
        Seq(rawFilters)
      }
      filterGroups.flatMap{ filters =>
        val shardValues = shardColumns.map { shardCol =>
          // To compute the shard hash, filters must match all shard columns by equality.
          val value = filters.find(f => f.column == shardCol) match {
            case Some(ColumnFilter(_, Filter.Equals(filtVal: String))) => filtVal
            case Some(ColumnFilter(_, filter)) =>
              throw new BadQueryException(s"Found filter for shard column $shardCol but " +
                s"$filter cannot be used for shard key routing")
            case _ =>
              throw new BadQueryException(s"Could not find filter for shard key column " +
                s"$shardCol, shard key hashing disabled")
          }
          RecordBuilder.trimShardColumn(dsOptions, shardCol, value)
        }
        val shardPairs = shardColumns.zip(shardValues)
        shardsFromValues(shardPairs, filters, qContext, startMs, endMs, useTargetSchemaForShards)
      }.distinct
    }
  }
  // scalastyle:off method.length

  private def toChunkScanMethod(rangeSelector: RangeSelector): ChunkScanMethod = {
    rangeSelector match {
      case IntervalSelector(from, to) => TimeRangeChunkScan(from, to)
      case AllChunksSelector          => AllChunkScan
      case WriteBufferSelector        => WriteBufferChunkScan
      case InMemoryChunksSelector     => InMemoryChunkScan
      case x @ _                      => throw new IllegalArgumentException(s"Unsupported range selector '$x' found")
    }
  }

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
    val tsp = targetSchemaProvider(qContext)
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

    val shards = leafInfos.flatMap { leaf =>
      val useTargetSchema = (filters: Seq[ColumnFilter]) => {
        val targetSchemaChanges = targetSchemaProvider(qContext).targetSchemaFunc(filters)
        val tsChangeExists = isTargetSchemaChanging(targetSchemaChanges, leaf.startMs, leaf.endMs)
        val targetSchemaOpt = findTargetSchema(targetSchemaChanges, leaf.startMs, leaf.endMs)
        val allTSLabelsPresent = useTargetSchemaForShards(filters, targetSchemaOpt)
        !tsChangeExists && allTSLabelsPresent
      }
      shardsFromFilters(leaf.renamedFilters, qContext, leaf.startMs, leaf.endMs, useTargetSchema)
    }.toSet

    Some(shards)
  }
  // scalastyle:on method.length

  /**
   * Returns a set of shards iff a plan can be pushed-down to each.
   * See [[LogicalPlanUtils.getPushdownKeys]] for more info.
   */
  private def getPushdownShards(qContext: QueryContext,
                                plan: LogicalPlan): Option[Set[Int]] = {
    val getRawPushdownShards = (rs: RawSeries) => {
      if (qContext.plannerParams.shardOverrides.isEmpty) {
        getShardSpanFromLp(rs, qContext).get
      } else qContext.plannerParams.shardOverrides.get.toSet
    }
    LogicalPlanUtils.getPushdownKeys(
      plan,
      targetSchemaProvider(qContext),
      shardKeyMatcher,
      dataset.options.nonMetricShardColumns,
      getRawPushdownShards,
      rs => LogicalPlan.getRawSeriesFilters(rs).flatMap(shardKeyMatcher(_)))
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
        LogicalPlanUtils.renameLabels(lp.on, dsOptions.metricColumn),
        LogicalPlanUtils.renameLabels(lp.ignoring, dsOptions.metricColumn), dsOptions.metricColumn,
        rvRangeFromPlan(lp)))
    else
      Seq(BinaryJoinExec(qContext, targetActor, stitchedLhs, stitchedRhs, lp.operator, lp.cardinality,
        LogicalPlanUtils.renameLabels(lp.on, dsOptions.metricColumn),
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
      lp match {
        case bj: BinaryJoin =>
          materializeBinaryJoinNoPushdown(qContextWithShardOverride, bj,
            forceInProcess = true, forceDispatcher = Some(dispatcher)).plans
        case agg: Aggregate =>
          materializeAggregateNoPushdown(qContextWithShardOverride, agg,
            forceInProcess = true, forceDispatcher = Some(dispatcher)).plans
        case x => throw new IllegalArgumentException(s"unhandled type: ${x.getClass}")
      }

    }
    PlanResult(plans)
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

  private def materializePeriodicSeriesWithWindowing(qContext: QueryContext,
                                                     lp: PeriodicSeriesWithWindowing,
                                                     forceInProcess: Boolean): PlanResult = {
    val logicalPlanWithoutBucket = if (queryConfig.translatePromToFilodbHistogram) {
       removeBucket(Right(lp))._3.right.get
    } else lp

    val series = walkLogicalPlanTree(logicalPlanWithoutBucket.series, qContext, forceInProcess)
    val rawSource = logicalPlanWithoutBucket.series.isRaw

    /* Last function is used to get the latest value in the window for absent_over_time
    If no data is present AbsentFunctionMapper will return range vector with value 1 */

    val execRangeFn = if (logicalPlanWithoutBucket.function == RangeFunctionId.AbsentOverTime) Last
                      else InternalRangeFunction.lpToInternalFunc(logicalPlanWithoutBucket.function)

    val paramsExec = materializeFunctionArgs(logicalPlanWithoutBucket.functionArgs, qContext)
    val window = if (execRangeFn == InternalRangeFunction.Timestamp) None else Some(logicalPlanWithoutBucket.window)
    series.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(logicalPlanWithoutBucket.startMs,
      logicalPlanWithoutBucket.stepMs, logicalPlanWithoutBucket.endMs, window, Some(execRangeFn), qContext,
      logicalPlanWithoutBucket.stepMultipleNotationUsed,
      paramsExec, logicalPlanWithoutBucket.offsetMs, rawSource)))
    if (logicalPlanWithoutBucket.function == RangeFunctionId.AbsentOverTime) {
      val aggregate = Aggregate(AggregationOperator.Sum, logicalPlanWithoutBucket, Nil,
                                AggregateClause.byOpt(Seq("job")))
      // Add sum to aggregate all child responses
      // If all children have NaN value, sum will yield NaN and AbsentFunctionMapper will yield 1
      val aggregatePlanResult = PlanResult(Seq(addAggregator(aggregate, qContext.copy(plannerParams =
        qContext.plannerParams.copy(skipAggregatePresent = true)), series)))
      addAbsentFunctionMapper(aggregatePlanResult, logicalPlanWithoutBucket.columnFilters,
        RangeParams(logicalPlanWithoutBucket.startMs / 1000, logicalPlanWithoutBucket.stepMs / 1000,
          logicalPlanWithoutBucket.endMs / 1000), qContext)
    } else series
  }

  private def removeBucket(lp: Either[PeriodicSeries, PeriodicSeriesWithWindowing]) = {
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
          val filtersWithoutBucket = rawSeriesLp.filters.filterNot(_.column.equals(PromMetricLabel)).
            filterNot(_.column == "le") :+ ColumnFilter(PromMetricLabel,
            Equals(nameFilter.get.replace("_bucket", "")))
          val newLp =
            if (lp.isLeft)
             Left(lp.left.get.copy(rawSeries = rawSeriesLp.copy(filters = filtersWithoutBucket)))
            else
             Right(lp.right.get.copy(series = rawSeriesLp.copy(filters = filtersWithoutBucket)))
          (nameFilter, leFilter, newLp)
        }
      case _ => (None, None, lp)
    }
  }
  private def materializePeriodicSeries(qContext: QueryContext,
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

    val rawSeries = walkLogicalPlanTree(lpWithoutBucket.rawSeries, qContext, forceInProcess)
    rawSeries.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(lp.startMs, lp.stepMs, lp.endMs,
      None, None, qContext, stepMultipleNotationUsed = false, Nil, lp.offsetMs)))

    if (nameFilter.isDefined && nameFilter.head.endsWith("_bucket") && leFilter.isDefined) {
      val paramsExec = StaticFuncArgs(leFilter.head.toDouble, RangeParams(lp.startMs/1000, lp.stepMs/1000,
        lp.endMs/1000))
      rawSeries.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(HistogramBucket,
        Seq(paramsExec))))
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
    * If there is a _type_ filter, remove it and populate the schema name string.  This is because while
    * the _type_ filter is a real filter on time series, we don't index it using Lucene at the moment.
    */
  private def extractSchemaFilter(filters: Seq[ColumnFilter]): (Seq[ColumnFilter], Option[String]) = {
    var schemaOpt: Option[String] = None
    val newFilters = filters.filterNot { case ColumnFilter(label, filt) =>
      val isTypeFilt = label == TypeLabel
      if (isTypeFilt) filt match {
        case Filter.Equals(schema) => schemaOpt = Some(schema.asInstanceOf[String])
        case x: Any                 => throw new IllegalArgumentException(s"Illegal filter $x on _type_")
      }
      isTypeFilt
    }
    (newFilters, schemaOpt)
  }

  // scalastyle:off method.length
  private def materializeRawSeries(qContext: QueryContext,
                                   lp: RawSeries,
                                   forceInProcess: Boolean): PlanResult = {
    val spreadProvToUse = qContext.plannerParams.spreadOverride.getOrElse(spreadProvider)
    val offsetMillis: Long = lp.offsetMs.getOrElse(0)
    val colName = lp.columns.headOption
    val (renamedFilters, schemaOpt) = extractSchemaFilter(renameMetricFilter(lp.filters))
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

    // Record whether-or-not (a) any tschema changes during the query, and (b) the changing tshema was applied.
    // Use this info to help determine whether-or-not the results need to be stitched.
    val tschemaChangesOverTime = new Array[Boolean](1)
    tschemaChangesOverTime(0) = false

    val useTargetSchema = (filters: Seq[ColumnFilter]) => {
      val targetSchemaChanges = targetSchemaProvider(qContext).targetSchemaFunc(filters)

      // Change in Target Schema in query window, do not use target schema to find query shards
      val tsChangeExists = (rangeSelectorWithOffset match {
        case IntervalSelector(from, to) => isTargetSchemaChanging(targetSchemaChanges, from, to)
        case _                          => false
      })
      val targetSchemaOpt = (rangeSelectorWithOffset match {
        case IntervalSelector(from, to) => findTargetSchema(targetSchemaChanges, from, to)
        case _                          => None
      })
      val allTSLabelsPresent = useTargetSchemaForShards(filters, targetSchemaOpt)

      // Whether to use target-schema for calculating the target shards.
      // If there is change in target-schema or target-schema not defined or query doesn't have all the labels of
      // target-schema, use shardKeyHash to find the target shards.
      // If a target-schema is defined and is not changing during the query window and all the target-schema labels are
      // provided in query filters (Equals), then find the target shard to route to using ingestionShard helper method.
      val useTschema = !tsChangeExists && allTSLabelsPresent
      if (tsChangeExists) {
        // Record that some tschema changes exist. This will later invoke result stitching.
        tschemaChangesOverTime(0) = true
      }
      useTschema
    }

    val (startMs, endMs): (Long, Long) = rangeSelectorWithOffset match {
      case IntervalSelector(from, to) => (from, to)
      case _ => (0, Long.MaxValue)
    }

    val execPlans = shardsFromFilters(renamedFilters, qContext, startMs, endMs, useTargetSchema).map { shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess, qContext)
      MultiSchemaPartitionsExec(qContext, dispatcher, dsRef, shard, renamedFilters,
        toChunkScanMethod(rangeSelectorWithOffset), dsOptions.metricColumn, schemaOpt, colName)
    }
    // Stitch only if spread changes during the query-window.
    // When a target-schema changes during query window, data might be ingested in
    //   different shards after the change.
    PlanResult(execPlans, needsStitch || tschemaChangesOverTime(0))
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
      exec.LabelValuesExec(qContext, dispatcher, dsRef, shard, renamedFilters, labelNames, lp.startMs, lp.endMs)
    }
    PlanResult(metaExec)
  }

  // allow metadataQueries to get list of shards from shardKeyFilters only if
  //   filters are given for all shard-key columns
  private def canGetShardsFromFilters(renamedFilters: Seq[ColumnFilter],
                                      qContext: QueryContext): Boolean = {
    qContext.plannerParams.shardOverrides.isEmpty &&
      shardColumns.nonEmpty &&
      shardColumns.toSet.subsetOf(renamedFilters.map(_.column).toSet) &&
      shardColumns.forall { shardCol => renamedFilters.exists(f => f.column == shardCol) }
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
      exec.LabelNamesExec(qContext, dispatcher, dsRef, shard, renamedFilters, lp.startMs, lp.endMs)
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
      exec.LabelCardinalityExec(qContext, dispatcher, dsRef, shard, renamedFilters, lp.startMs, lp.endMs)
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
      exec.TsCardExec(qContext, dispatcher, dsRef, shard, lp.shardKeyPrefix, lp.numGroupByFields, clusterNameToPass,
        lp.version)
    }
    PlanResult(metaExec)
  }

  private def materializeSeriesKeysByFilters(qContext: QueryContext,
                                             lp: SeriesKeysByFilters,
                                             forceInProcess: Boolean): PlanResult = {
    // NOTE: _type_ filter support currently isn't there in series keys queries
    val (renamedFilters, _) = extractSchemaFilter(renameMetricFilter(lp.filters))

    val useTSForQueryShards = (filters: Seq[ColumnFilter]) => {
      // Change in Target Schema in query window, do not use target schema to find query shards
      val targetSchemaChanges = targetSchemaProvider(qContext).targetSchemaFunc(filters)
      val tsChangeExists = isTargetSchemaChanging(targetSchemaChanges, lp.startMs, lp.endMs)
      val targetSchemaOpt = findTargetSchema(targetSchemaChanges, lp.startMs, lp.endMs)
      val allTSLabelsPresent = useTargetSchemaForShards(filters, targetSchemaOpt)
      // No change in TargetSchema or not all target-schema labels are provided in the query
      !tsChangeExists && allTSLabelsPresent
    }

    val shardsToHit = if (canGetShardsFromFilters(renamedFilters, qContext)) {
      shardsFromFilters(renamedFilters, qContext, lp.startMs, lp.endMs, useTSForQueryShards)
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
    val (renamedFilters, schemaOpt) = extractSchemaFilter(renameMetricFilter(lp.filters))
    val metaExec = shardsFromFilters(renamedFilters, qContext, lp.startMs, lp.endMs).map { shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess, qContext)
      SelectChunkInfosExec(qContext, dispatcher, dsRef, shard, renamedFilters, toChunkScanMethod(lp.rangeSelector),
        schemaOpt, colName)
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
    val toReduceLevel1 = walkLogicalPlanTree(lp.vectors, qContext, forceInProcess)
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
