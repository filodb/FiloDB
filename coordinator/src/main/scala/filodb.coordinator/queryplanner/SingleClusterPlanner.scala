package filodb.coordinator.queryplanner

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon

import filodb.coordinator.ShardMapper
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

  /**
   * If TargetSchema exists and all of the target-schema label filters (equals) are provided in the query,
   * then return true.
   * @param filters Query Column Filters
   * @param targetSchema TargetSchema
   * @return useTargetSchema - use target-schema to calculate query shards
   */
  private def useTargetSchemaForShards(filters: Seq[ColumnFilter], targetSchema: Option[TargetSchemaChange]): Boolean =
    targetSchema match {
      case Some(ts) if ts.schema.nonEmpty => ts.schema
        .forall(s => filters.exists(cf => cf.column == s && cf.filter.isInstanceOf[Filter.Equals]))
      case _ => false
    }

  /**
   * Returns an occupied option with target schema labels iff they are defined and identical
   *   for every leaf LogicalPlan that draws data from a shard (i.e. isn't a leaf-level scalar).
   * Plans that do not draw data from any shards do not affect the result *unless* no plan draws
   *   data from a shard. In this case, Some(Seq.empty) is returned.
   */
  private def getUniversalTargetSchemaLabels(lp: LogicalPlan,
                                             tsp: TargetSchemaProvider): Option[Seq[String]] = {
    lp match {
      case nl: NonLeafLogicalPlan =>
        val tsLabelOpts = nl.children.map(getUniversalTargetSchemaLabels(_, tsp))
        if (tsLabelOpts.forall(_.isDefined)) {
          // Filter out empty lists, since these arise only from scalar plans, and we don't
          //   want those to force a None return.
          val nonEmpty = tsLabelOpts.filter(_.get.nonEmpty)
          // Check if all non-head elements equal the head (i.e. all are the same).
          if (nonEmpty.drop(1).forall(_ == tsLabelOpts.head)) {
            return nonEmpty.headOption.getOrElse(Some(Seq.empty))
          }
        }
        None
      case rs: RawSeries =>
        val rangeSelectorOpt: Option[(Long, Long)] = rs.rangeSelector match {
          case IntervalSelector(fromMs, toMs) => Some((fromMs, toMs))
          case _ => None
        }
        val targetSchemaOpt = if (rangeSelectorOpt.isDefined) {
          val (fromMs, toMs) = rangeSelectorOpt.get
          val tsChanges = tsp.targetSchemaFunc(rs.filters)
          findTargetSchema(tsChanges, fromMs, toMs).map(_.schema)
        } else None

        if (targetSchemaOpt.isDefined) {
          // make this assertion since the non-leaf case's logic requires it
          assert(targetSchemaOpt.get.size > 0, "expected target schema labels, but none exist")
          targetSchemaOpt
        } else None
      // Non-leaf plans are processed above; no non-leaf scalar will reach here.
      // Return empty Seq to indicate this is a leaf-level scalar.
      case sc: ScalarPlan => Some(Seq.empty)
      case _ => None
    }
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

  def shardsFromFilters(filters: Seq[ColumnFilter],
                        qContext: QueryContext,
                        useTargetSchemaForShards: Boolean = false): Seq[Int] = {

    val spreadProvToUse = qContext.plannerParams.spreadOverride.getOrElse(spreadProvider)

    require(shardColumns.nonEmpty || qContext.plannerParams.shardOverrides.nonEmpty,
      s"Dataset $dsRef does not have shard columns defined, and shard overrides were not mentioned")

    qContext.plannerParams.shardOverrides.getOrElse {
      val shardVals = shardColumns.map { shardCol =>
        // So to compute the shard hash we need shardCol == value filter (exact equals) for each shardColumn
        filters.find(f => f.column == shardCol) match {
          case Some(ColumnFilter(_, Filter.Equals(filtVal: String))) =>
            shardCol -> RecordBuilder.trimShardColumn(dsOptions, shardCol, filtVal)
          case Some(ColumnFilter(_, filter)) =>
            throw new BadQueryException(s"Found filter for shard column $shardCol but " +
              s"$filter cannot be used for shard key routing")
          case _ =>
            throw new BadQueryException(s"Could not find filter for shard key column " +
              s"$shardCol, shard key hashing disabled")
        }
      }
      val metric = shardVals.find(_._1 == dsOptions.metricColumn)
        .map(_._2)
        .getOrElse(throw new BadQueryException(s"Could not find metric value"))
      val shardValues = shardVals.filterNot(_._1 == dsOptions.metricColumn).map(_._2)
      logger.debug(s"For shardColumns $shardColumns, extracted metric $metric and shard values $shardValues")
      val targetSchemaChange = targetSchemaProvider(qContext).targetSchemaFunc(filters)
      val targetSchema = if (targetSchemaChange.nonEmpty) targetSchemaChange.last.schema else Seq.empty
      val shardHash = RecordBuilder.shardKeyHash(shardValues, dsOptions.metricColumn, metric, targetSchema)
      if(useTargetSchemaForShards) {
        val nonShardKeyLabelPairs = filters.filter(f => !shardColumns.contains(f.column)
                                                          && f.filter.isInstanceOf[Filter.Equals])
                                            .map(cf => cf.column ->
                                              cf.filter.asInstanceOf[Filter.Equals].value.toString).toMap
        val partitionHash = RecordBuilder.partitionKeyHash(nonShardKeyLabelPairs, shardVals.toMap, targetSchema,
          dsOptions.metricColumn, metric)
        // since target-schema filter is provided in the query, ingestionShard can be used to find the single shard
        // that can answer the query.
        Seq(shardMapperFunc.ingestionShard(shardHash, partitionHash, spreadProvToUse.spreadFunc(filters).last.spread))
      } else {
        shardMapperFunc.queryShards(shardHash, spreadProvToUse.spreadFunc(filters).last.spread)
      }
    }
  }

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

  /**
   * Returns the shards spanned by a LogicalPlan's data.
   * @return an occupied Option iff shards could be determined for all leaf-level plans.
   */
  private def getShardSpanFromLp(lp: LogicalPlan,
                                 qContext: QueryContext): Option[Set[Int]] = {

    case class LeafInfo(lp: LogicalPlan,
                        renamedFilters: Seq[ColumnFilter],
                        targetSchemaLabels: Option[Seq[String]])

    // construct a LeafInfo for each leaf...
    val tsp = targetSchemaProvider(qContext)
    val leafInfos = new ArrayBuffer[LeafInfo]()
    for (leafPlan <- findLeafLogicalPlans(lp)) {
      val filters = getColumnFilterGroup(leafPlan).map(_.toSeq)
      assert(filters.size == 1, s"expected leaf plan to yield single filter group, but got ${filters.size}")
      leafPlan match {
        case rs: RawSeries =>
          // Get time params from the RangeSelector, and use them to identify a TargetSchemaChanges.
          val tsLabels: Option[Seq[String]] = {
            val tsFunc = tsp.targetSchemaFunc(filters.head)
            rs.rangeSelector match {
              case is: IntervalSelector => findTargetSchema(tsFunc, is.from, is.to).map(_.schema)
              case _ => None
            }
          }
          leafInfos.append(LeafInfo(leafPlan, renameMetricFilter(filters.head), tsLabels))
        // Do nothing; not pulling data from any shards.
        case sc: ScalarPlan => {}
        // Note!! If an unrecognized plan type is encountered, this just pessimistically returns None.
        case _ => return None
      }
    }

    // if we can't extract shards from all filters, return an empty result
    if (!leafInfos.forall{ leaf =>
      canGetShardsFromFilters(leaf.renamedFilters, qContext)
    }) return None

    Some(leafInfos.flatMap{ leaf =>
      val useTargetSchema = leaf.targetSchemaLabels.isDefined && {
        val equalColFilterLabels = leaf.renamedFilters.filter(_.filter.isInstanceOf[Filter.Equals]).map(_.column)
        leaf.targetSchemaLabels.get.toSet.subsetOf(equalColFilterLabels.toSet)
      }
      shardsFromFilters(leaf.renamedFilters, qContext, useTargetSchema)
    }.toSet)
  }

  // scalastyle:off method.length
  // scalastyle:off cyclomatic.complexity
  /**
   * Returns the set of shards to which the LogicalPlan can be pushed down.
   * When this function returns a Some, the argument plan can be materialized for each shard
   *   in the result, and the concatenation of these shard-local joins will, on execute(), yield the
   *   same RangeVectors as the non-pushed-down plan.
   * See materializeWithPushdown for details about this pushdown optimization.
   * @return an occupied Option iff it is valid to perform the a pushdown optimization on the argument plan.
   */
  private def getPushdownShards(qContext: QueryContext,
                                lp: LogicalPlan): Option[Set[Int]] = {
    def helper(lp: LogicalPlan): Option[Set[Int]] = lp match {
      // VectorPlans can't currently be pushed down. Consider:
      //     foo{...} or vector(0)
      //   If foo{...} is sharded with spread=1, but both shards contain no foo{...} data,
      //   then both pushed-down plans will yield vector(0). These will be concatenated via a DistConcatExec.
      // VectorPlans can technically be pushed down when the join operation isn't a SetOperator,
      //   but the extra logic to support that unlikely use-case isn't worth the maintenance overhead.
      case vec: VectorPlan => None
      // SVDP's should not be pushed down. Consider:
      //   foo{...} + scalar(bar{...})
      // There are only three possible cases:
      //   (1) The scalar's data lies on a single shard. If foo's data lies exclusively on the same shard,
      //       then no pushdown is necessary. Otherwise, the scalar cannot be pushed down.
      //   (2) The scalar's data lies on multiple shards. Even if they are the same as foo's, the scalar's
      //       instant-vector needs to contain a single row; its data is aggregated under-the-hood.
      //       A pushdown would break this aggregation step.
      //   (3) The scalar's data lies on no shards. Then the scalar's selector is similar to:
      //         scalar(vector(0))
      //       which is not an intended use-case.
      case svdp: ScalarVaryingDoublePlan => None
      case svbo: ScalarVectorBinaryOperation => helper(svbo.vector)
      case ps: PeriodicSeries => helper(ps.rawSeries)
      case psw: PeriodicSeriesWithWindowing => helper(psw.series)
      case aif: ApplyInstantFunction => helper(aif.vectors)
      case bj: BinaryJoin =>
        // lhs/rhs must reside on the same set of shards, and target schema labels for all leaves must be
        //   discoverable, equal, and preserved by join keys
        val lhsShards = helper(bj.lhs)
        val rhsShards = helper(bj.rhs)
        val canPushdown = lhsShards != None && rhsShards != None &&
                          // either the shard groups are equal, or either of lhs/rhs includes only scalars.
                          (lhsShards == rhsShards || lhsShards.get.isEmpty || rhsShards.get.isEmpty) &&
                          {
                            val targetSchemaLabels =
                              getUniversalTargetSchemaLabels(bj, targetSchemaProvider(qContext))
                            targetSchemaLabels.isDefined &&
                              targetSchemaLabels.get.toSet.subsetOf(bj.on.toSet)
                          }
        // union lhs/rhs shards, since one might be empty (if it's a scalar)
        if (canPushdown) Some(lhsShards.get.union(rhsShards.get)) else None
      case agg: Aggregate =>
        // target schema labels for all leaves must be discoverable, equal, and preserved by join keys
        val shards = helper(agg.vectors)
        val canPushdown = shards != None &&
                          agg.clauseOpt.isDefined &&
                          agg.clauseOpt.get.clauseType == AggregateClause.ClauseType.By &&
                          {
                            val targetSchemaLabels =
                              getUniversalTargetSchemaLabels(agg, targetSchemaProvider(qContext))
                            targetSchemaLabels.isDefined &&
                            {
                              val byLabels = agg.clauseOpt.get.labels
                              targetSchemaLabels.get.toSet.subsetOf(byLabels.toSet)
                            }
                          }
        if (canPushdown) shards else None
      case nl: NonLeafLogicalPlan =>
        // Pessimistically require that this entire subtree lies on one shard (or none, if all scalars).
        val shardGroups = nl.children.map(helper(_))
        if (shardGroups.forall(_.isDefined)) {
          val shards = shardGroups.flatMap(_.get).toSet
          // if zero elements, then all children are scalars
          if (shards.size <= 1) Some(shards) else None
        } else None
      case rs: RawSeries => getShardSpanFromLp(rs, qContext)
      case sc: ScalarPlan => Some(Set.empty)  // don't want a None to end shard-group propagation
      case _ => None
    }
    helper(lp)
  }
  // scalastyle:on method.length
  // scalastyle:on cyclomatic.complexity

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
    // see materializeWithPushdown for details about the pushdown optimization.
    val pushdownShards = getPushdownShards(qContext, lp)
    if (pushdownShards.isDefined) {
      materializeWithPushdown(qContext, lp, pushdownShards.get, forceInProcess)
    } else {
      materializeBinaryJoinNoPushdown(qContext, lp, forceInProcess, None)
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

  private def materializeRawSeries(qContext: QueryContext,
                                   lp: RawSeries,
                                   forceInProcess: Boolean): PlanResult = {
    val spreadProvToUse = qContext.plannerParams.spreadOverride.getOrElse(spreadProvider)
    val offsetMillis: Long = lp.offsetMs.getOrElse(0)
    val colName = lp.columns.headOption
    val (renamedFilters, schemaOpt) = extractSchemaFilter(renameMetricFilter(lp.filters))
    val spreadChanges = spreadProvToUse.spreadFunc(renamedFilters)

    val targetSchemaChanges = targetSchemaProvider(qContext).targetSchemaFunc(renamedFilters)

    val rangeSelectorWithOffset = lp.rangeSelector match {
      case IntervalSelector(fromMs, toMs) => IntervalSelector(fromMs - offsetMillis - lp.lookbackMs.getOrElse(
                                             queryConfig.staleSampleAfterMs), toMs - offsetMillis)
      case _                              => lp.rangeSelector
    }
    val needsStitch = rangeSelectorWithOffset match {
      case IntervalSelector(from, to) => spreadChanges.exists(c => c.time >= from && c.time <= to)
      case _                          => false
    }

    // Change in Target Schema in query window, do not use target schema to find query shards
    val tsChangeExists = (rangeSelectorWithOffset match {
      case IntervalSelector(from, to) => isTargetSchemaChanging(targetSchemaChanges, from, to)
      case _                          => false
    })
    val targetSchemaOpt = (rangeSelectorWithOffset match {
      case IntervalSelector(from, to) => findTargetSchema(targetSchemaChanges, from, to)
      case _                          => None
    })
    val allTSLabelsPresent = useTargetSchemaForShards(renamedFilters, targetSchemaOpt)

    // Whether to use target-schema for calculating the target shards.
    // If there is change in target-schema or target-schema not defined or query doesn't have all the labels of
    // target-schema, use shardKeyHash to find the target shards.
    // If a target-schema is defined and is not changing during the query window and all the target-schema labels are
    // provided in query filters (Equals), then find the target shard to route to using ingestionShard helper method.
    val useTSForQueryShards = !tsChangeExists && allTSLabelsPresent

    val execPlans = shardsFromFilters(renamedFilters, qContext, useTSForQueryShards).map { shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess, qContext)
      MultiSchemaPartitionsExec(qContext, dispatcher, dsRef, shard, renamedFilters,
        toChunkScanMethod(rangeSelectorWithOffset), dsOptions.metricColumn, schemaOpt, colName)
    }
    // Stitch only if spread and/or target-schema changes during the query-window.
    // when target-schema changes during query window, data might be ingested in different shards after the change.
    PlanResult(execPlans, needsStitch || tsChangeExists)
  }

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
      shardsFromFilters(renamedFilters, qContext)
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

  // allow metadataQueries to get list of shards from shardKeyFilters only if all shardCols have Equals filter
  private def canGetShardsFromFilters(renamedFilters: Seq[ColumnFilter],
                                      qContext: QueryContext): Boolean = {
    if (qContext.plannerParams.shardOverrides.isEmpty && shardColumns.nonEmpty) {
      shardColumns.toSet.subsetOf(renamedFilters.map(_.column).toSet) &&
        shardColumns.forall { shardCol =>
          // So to compute the shard hash we need shardCol == value filter (exact equals) for each shardColumn
          renamedFilters.find(f => f.column == shardCol) match {
            case Some(ColumnFilter(_, Filter.Equals(_: String))) => true
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
      shardsFromFilters(renamedFilters, qContext)
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
    val shardsToHit = shardsFromFilters(renamedFilters, qContext)

    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess, qContext)
      exec.LabelCardinalityExec(qContext, dispatcher, dsRef, shard, renamedFilters, lp.startMs, lp.endMs)
    }
    PlanResult(metaExec)
  }

  private def materializeTsCardinalities(qContext: QueryContext,
                                         lp: TsCardinalities,
                                         forceInProcess: Boolean): PlanResult = {
    val metaExec = shardMapperFunc.assignedShards.map{ shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess, qContext)
      exec.TsCardExec(qContext, dispatcher, dsRef, shard, lp.shardKeyPrefix, lp.numGroupByFields)
    }
    PlanResult(metaExec)
  }

  private def materializeSeriesKeysByFilters(qContext: QueryContext,
                                             lp: SeriesKeysByFilters,
                                             forceInProcess: Boolean): PlanResult = {
    // NOTE: _type_ filter support currently isn't there in series keys queries
    val (renamedFilters, _) = extractSchemaFilter(renameMetricFilter(lp.filters))

    val targetSchemaChanges = targetSchemaProvider(qContext).targetSchemaFunc(renamedFilters)

    // Change in Target Schema in query window, do not use target schema to find query shards
    val tsChangeExists = isTargetSchemaChanging(targetSchemaChanges, lp.startMs, lp.endMs)
    val targetSchemaOpt = findTargetSchema(targetSchemaChanges, lp.startMs, lp.endMs)
    val allTSLabelsPresent = useTargetSchemaForShards(renamedFilters, targetSchemaOpt)

    // No change in TargetSchema or not all target-schema labels are provided in the query
    val useTSForQueryShards = !tsChangeExists && allTSLabelsPresent
    val shardsToHit = if (useTSForQueryShards || canGetShardsFromFilters(renamedFilters, qContext)) {
      shardsFromFilters(renamedFilters, qContext, useTSForQueryShards)
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
    val metaExec = shardsFromFilters(renamedFilters, qContext).map { shard =>
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
