package filodb.coordinator.queryengine2

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.duration._

import akka.actor.ActorRef
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler

import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.core.{DatasetRef}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.SpreadProvider
import filodb.core.store._
import filodb.prometheus.ast.Vectors.PromMetricLabel
import filodb.query.{exec, _}
import filodb.query.exec._


trait TsdbQueryParams
case class PromQlQueryParams(promQl: String, start: Long, step: Long, end: Long,
                        spread: Option[Int] = None, processFailure: Boolean = true) extends TsdbQueryParams

object UnavailablePromQlQueryParams extends TsdbQueryParams

/**
  * FiloDB Query Engine is the facade for execution of FiloDB queries.
  * It is meant for use inside FiloDB nodes to execute materialized
  * ExecPlans as well as from the client to execute LogicalPlans.
  */
class QueryEngine(dsRef: DatasetRef,
                  schemas: Schemas,
                  shardMapperFunc: => ShardMapper,
                  failureProvider: FailureProvider,
                  spreadProvider: SpreadProvider = StaticSpreadProvider(),
                  queryEngineConfig: Config = ConfigFactory.empty())
                   extends StrictLogging {
  /**
    * Intermediate Plan Result includes the exec plan(s) along with any state to be passed up the
    * plan building call tree during query planning.
    *
    * Not for runtime use.
    */
  private case class PlanResult(plans: Seq[ExecPlan], needsStitch: Boolean = false)

  private val mdNoShardKeyFilterRequests = Kamon.counter("queryengine-metadata-no-shardkey-requests")
  private val dsOptions = schemas.part.options

  /**
    * This is the facade to trigger orchestration of the ExecPlan.
    * It sends the ExecPlan to the destination where it will be executed.
    */
  def dispatchExecPlan(execPlan: ExecPlan)
                      (implicit sched: Scheduler,
                       timeout: FiniteDuration): Task[QueryResponse] = {
    val currentSpan = Kamon.currentSpan()
    Kamon.withSpan(currentSpan) {
      execPlan.dispatcher.dispatch(execPlan)
    }
  }

  /**
    * Converts Routes to ExecPlan
    */
  private def routeExecPlanMapper(routes: Seq[Route], rootLogicalPlan: LogicalPlan, queryId: String, submitTime: Long,
                                  options: QueryOptions, spreadProvider: SpreadProvider, lookBackTime: Long,
                                  tsdbQueryParams: TsdbQueryParams): ExecPlan = {

    val execPlans : Seq[ExecPlan]= routes.map { route =>
      route match {
        case route: LocalRoute => if (!route.timeRange.isDefined)
          generateLocalExecPlan(rootLogicalPlan, queryId, submitTime, options, spreadProvider)
        else
          generateLocalExecPlan(QueryRoutingPlanner.copyWithUpdatedTimeRange(rootLogicalPlan,
            route.asInstanceOf[LocalRoute].timeRange.get, lookBackTime), queryId, submitTime, options, spreadProvider)
        case route: RemoteRoute =>
          val timeRange = route.timeRange.get
          val queryParams = tsdbQueryParams.asInstanceOf[PromQlQueryParams]
          val routingConfig = queryEngineConfig.getConfig("routing")
          val promQlInvocationParams = PromQlInvocationParams(routingConfig, queryParams.promQl,
            (timeRange.startInMillis / 1000), queryParams.step, (timeRange.endInMillis / 1000), queryParams.spread,
            false)
          logger.debug("PromQlExec params:" + promQlInvocationParams)
          PromQlExec(queryId, InProcessPlanDispatcher(), dsRef, promQlInvocationParams, submitTime)
      }
    }

    if (execPlans.size == 1)
       execPlans.head
    else
      // Stitch RemoteExec plan results with local using InProcessorDispatcher
      // Sort to move RemoteExec in end as it does not have schema
       StitchRvsExec(queryId, InProcessPlanDispatcher(),
        execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlExec]))
  }

  /**
    * Converts a LogicalPlan to the ExecPlan
    */
  def materialize(rootLogicalPlan: LogicalPlan,
                  options: QueryOptions,
                  tsdbQueryParams: TsdbQueryParams): ExecPlan = {
    val queryId = UUID.randomUUID().toString
    val submitTime = System.currentTimeMillis()
    val querySpreadProvider = options.spreadProvider.getOrElse(spreadProvider)

    if (!QueryRoutingPlanner.isPeriodicSeriesPlan(rootLogicalPlan) || // It is a raw data query
       (!rootLogicalPlan.isRoutable) ||
      !tsdbQueryParams.isInstanceOf[PromQlQueryParams] || // We don't know the promql issued (unusual)
      (tsdbQueryParams.isInstanceOf[PromQlQueryParams] &&
        !tsdbQueryParams.asInstanceOf[PromQlQueryParams].processFailure) || // This is a query that was part of
      // failure routing
      !QueryRoutingPlanner.hasSingleTimeRange(rootLogicalPlan)) // Sub queries have different time ranges (unusual)
      {
       generateLocalExecPlan(rootLogicalPlan, queryId, submitTime, options, querySpreadProvider)
      } else {
      val periodicSeriesTime = QueryRoutingPlanner.getPeriodicSeriesTimeFromLogicalPlan(logicalPlan = rootLogicalPlan)
      val lookBackTime: Long = QueryRoutingPlanner.getRawSeriesStartTime(rootLogicalPlan).
        map(periodicSeriesTime.startInMillis - _).get

      val routingTime = TimeRange(periodicSeriesTime.startInMillis - lookBackTime, periodicSeriesTime.endInMillis)
      val failures =
        failureProvider.getFailures(dsRef, routingTime)
                        .sortWith(_.timeRange.startInMillis < _.timeRange.startInMillis)

      if (failures.isEmpty) {
        generateLocalExecPlan(rootLogicalPlan, queryId, submitTime, options, querySpreadProvider)
      } else {
        val promQlQueryParams = tsdbQueryParams.asInstanceOf[PromQlQueryParams]
        val routes : Seq[Route] = if (promQlQueryParams.start == promQlQueryParams.end) { // Instant Query
          if (failures.forall(_.isRemote.equals(false))) {
            Seq(RemoteRoute(Some(TimeRange(periodicSeriesTime.startInMillis, periodicSeriesTime.endInMillis))))
          } else {
            Seq(LocalRoute(None))
          }
        } else {
          QueryRoutingPlanner.plan(failures, periodicSeriesTime, lookBackTime,
            promQlQueryParams.step * 1000)
          }
        logger.debug("Routes:" + routes)
        routeExecPlanMapper(routes, rootLogicalPlan, queryId, submitTime, options, querySpreadProvider, lookBackTime,
          tsdbQueryParams)
      }
    }
  }

  private def generateLocalExecPlan(logicalPlan: LogicalPlan,
                          queryId: String,
                          submitTime: Long,
                          options: QueryOptions, spreadProvider: SpreadProvider) = {
    val materialized = walkLogicalPlanTree(logicalPlan, queryId, System.currentTimeMillis(),
      options, spreadProvider)
    match {
      case PlanResult(Seq(justOne), stitch) =>
        if (stitch) justOne.addRangeVectorTransformer(new StitchRvsMapper())
        justOne
      case PlanResult(many, stitch) =>
        val targetActor = pickDispatcher(many)
        many(0) match {
          case lve: LabelValuesExec => LabelValuesDistConcatExec(queryId, targetActor, many)
          case ske: PartKeysExec => PartKeysDistConcatExec(queryId, targetActor, many)
          case ep: ExecPlan =>
            val topPlan = DistConcatExec(queryId, targetActor, many)
            if (stitch) topPlan.addRangeVectorTransformer(new StitchRvsMapper())
            topPlan
        }
    }
    logger.debug(s"Materialized logical plan for dataset=$dsRef:" +
      s" $logicalPlan to \n${materialized.printTree()}")
    materialized

  }

  val shardColumns = dsOptions.shardKeyColumns.sorted

  private def shardsFromFilters(filters: Seq[ColumnFilter],
                                options: QueryOptions, spreadProvider : SpreadProvider): Seq[Int] = {
    require(shardColumns.nonEmpty || options.shardOverrides.nonEmpty,
      s"Dataset $dsRef does not have shard columns defined, and shard overrides were not mentioned")

    options.shardOverrides.getOrElse {
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
      val shardHash = RecordBuilder.shardKeyHash(shardValues, metric)
      shardMapperFunc.queryShards(shardHash, spreadProvider.spreadFunc(filters).last.spread)
    }
  }

  private def dispatcherForShard(shard: Int): PlanDispatcher = {
    val targetActor = shardMapperFunc.coordForShard(shard)
    if (targetActor == ActorRef.noSender) {
      logger.debug(s"ShardMapper: $shardMapperFunc")
      throw new RuntimeException(s"Shard: $shard is not available") // TODO fix this
    }
    ActorPlanDispatcher(targetActor)
  }

  /**
    * Walk logical plan tree depth-first and generate execution plans starting from the bottom
    *
    * @return ExecPlans that answer the logical plan provided
    */
  private def walkLogicalPlanTree(logicalPlan: LogicalPlan,
                                  queryId: String,
                                  submitTime: Long,
                                  options: QueryOptions,
                                  spreadProvider: SpreadProvider): PlanResult = {

    logicalPlan match {
      case lp: RawSeries                   => materializeRawSeries(queryId, submitTime, options, lp, spreadProvider)
      case lp: RawChunkMeta                => materializeRawChunkMeta(queryId, submitTime, options, lp, spreadProvider)
      case lp: PeriodicSeries              => materializePeriodicSeries(queryId, submitTime, options, lp,
                                              spreadProvider)
      case lp: PeriodicSeriesWithWindowing => materializePeriodicSeriesWithWindowing(queryId, submitTime, options, lp,
                                              spreadProvider)
      case lp: ApplyInstantFunction        => materializeApplyInstantFunction(queryId, submitTime, options, lp,
                                              spreadProvider)
      case lp: Aggregate                   => materializeAggregate(queryId, submitTime, options, lp, spreadProvider)
      case lp: BinaryJoin                  => materializeBinaryJoin(queryId, submitTime, options, lp, spreadProvider)
      case lp: ScalarVectorBinaryOperation => materializeScalarVectorBinOp(queryId, submitTime, options, lp,
                                              spreadProvider)
      case lp: LabelValues                 => materializeLabelValues(queryId, submitTime, options, lp, spreadProvider)
      case lp: SeriesKeysByFilters         => materializeSeriesKeysByFilters(queryId, submitTime, options, lp,
                                              spreadProvider)
      case lp: ApplyMiscellaneousFunction  => materializeApplyMiscellaneousFunction(queryId, submitTime, options, lp,
                                              spreadProvider)
      case lp: ApplySortFunction           => materializeApplySortFunction(queryId, submitTime, options, lp,
                                              spreadProvider)
      case lp: ScalarVaryingDoublePlan     => materializeScalarPlan(queryId, submitTime, options, lp, spreadProvider)
      case lp: ScalarTimeBasedPlan         => materializeScalarTimeBased(queryId, submitTime, options, lp,
                                              spreadProvider)
      case lp: VectorPlan                  => materializeVectorPlan(queryId, submitTime, options, lp, spreadProvider)
      case lp: ScalarFixedDoublePlan       => materializeFixedScalar(queryId, submitTime, options, lp, spreadProvider)
      case _                               => throw new BadQueryException("Invalid logical plan")

    }
  }

  private def materializeScalarVectorBinOp(queryId: String,
                                           submitTime: Long,
                                           options: QueryOptions,
                                           lp: ScalarVectorBinaryOperation,
                                           spreadProvider : SpreadProvider): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vector, queryId, submitTime, options, spreadProvider)
    val funcArg = materializeFunctionArgs(Seq(lp.scalarArg), queryId, submitTime, options, spreadProvider)
    vectors.plans.foreach(_.addRangeVectorTransformer(ScalarOperationMapper(lp.operator, lp.scalarIsLhs, funcArg)))
    vectors
  }

  private def materializeBinaryJoin(queryId: String,
                                    submitTime: Long,
                                    options: QueryOptions,
                                    lp: BinaryJoin, spreadProvider : SpreadProvider): PlanResult = {
    val lhs = walkLogicalPlanTree(lp.lhs, queryId, submitTime, options, spreadProvider)
    val stitchedLhs = if (lhs.needsStitch) Seq(StitchRvsExec(queryId, pickDispatcher(lhs.plans), lhs.plans))
                      else lhs.plans
    val rhs = walkLogicalPlanTree(lp.rhs, queryId, submitTime, options, spreadProvider)
    val stitchedRhs = if (rhs.needsStitch) Seq(StitchRvsExec(queryId, pickDispatcher(rhs.plans), rhs.plans))
                      else rhs.plans
    // TODO Currently we create separate exec plan node for stitching.
    // Ideally, we can go one step further and add capability to NonLeafNode plans to pre-process
    // and transform child results individually before composing child results together.
    // In theory, more efficient to use transformer than to have separate exec plan node to avoid IO.
    // In the interest of keeping it simple, deferring decorations to the ExecPlan. Add only if needed after measuring.

    val targetActor = pickDispatcher(stitchedLhs ++ stitchedRhs)
    val joined = if (lp.operator.isInstanceOf[SetOperator])
      Seq(exec.SetOperatorExec(queryId, targetActor, stitchedLhs, stitchedRhs, lp.operator,
        lp.on, lp.ignoring, dsOptions.metricColumn))
    else
      Seq(BinaryJoinExec(queryId, targetActor, stitchedLhs, stitchedRhs, lp.operator, lp.cardinality,
        lp.on, lp.ignoring, lp.include, dsOptions.metricColumn))
    PlanResult(joined, false)
  }

  private def materializeAggregate(queryId: String,
                                   submitTime: Long,
                                   options: QueryOptions,
                                   lp: Aggregate, spreadProvider : SpreadProvider): PlanResult = {
    val toReduceLevel1 = walkLogicalPlanTree(lp.vectors, queryId, submitTime, options, spreadProvider )
    // Now we have one exec plan per shard
    /*
     * Note that in order for same overlapping RVs to not be double counted when spread is increased,
     * one of the following must happen
     * 1. Step instants must be chosen so time windows dont span shards.
     * 2. We pump data into multiple shards for sometime so atleast one shard will fully contain any time window
     *
     * Pulling all data into one node and stich before reducing (not feasible, doesnt scale). So we will
     * not stitch
     *
     * Starting off with solution 1 first until (2) or some other approach is decided on.
     */
    toReduceLevel1.plans.foreach {
      _.addRangeVectorTransformer(AggregateMapReduce(lp.operator, lp.params, lp.without, lp.by))
    }

    val toReduceLevel2 =
      if (toReduceLevel1.plans.size >= 16) {
        // If number of children is above a threshold, parallelize aggregation
        val groupSize = Math.sqrt(toReduceLevel1.plans.size).ceil.toInt
        toReduceLevel1.plans.grouped(groupSize).map { nodePlans =>
          val reduceDispatcher = nodePlans.head.dispatcher
          ReduceAggregateExec(queryId, reduceDispatcher, nodePlans, lp.operator, lp.params)
        }.toList
      } else toReduceLevel1.plans

    val reduceDispatcher = pickDispatcher(toReduceLevel2)
    val reducer = ReduceAggregateExec(queryId, reduceDispatcher, toReduceLevel2, lp.operator, lp.params)
    reducer.addRangeVectorTransformer(AggregatePresenter(lp.operator, lp.params))
    PlanResult(Seq(reducer), false) // since we have aggregated, no stitching
  }

  private def materializeApplyInstantFunction(queryId: String,
                                              submitTime: Long,
                                              options: QueryOptions,
                                              lp: ApplyInstantFunction, spreadProvider : SpreadProvider): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, queryId, submitTime, options, spreadProvider)
    val paramsExec = materializeFunctionArgs(lp.functionArgs, queryId, submitTime, options, spreadProvider)
    vectors.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(lp.function, paramsExec)))
    vectors
  }

  private def materializePeriodicSeriesWithWindowing(queryId: String,
                                                     submitTime: Long,
                                                     options: QueryOptions,
                                                     lp: PeriodicSeriesWithWindowing,
                                                     spreadProvider: SpreadProvider): PlanResult = {
    val rawSeries = walkLogicalPlanTree(lp.rawSeries, queryId, submitTime, options, spreadProvider)
    val execRangeFn = InternalRangeFunction.lpToInternalFunc(lp.function)
    val paramsExec = materializeFunctionArgs(lp.functionArgs, queryId, submitTime, options, spreadProvider)
    rawSeries.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(lp.start, lp.step,
      lp.end, Some(lp.window), Some(execRangeFn), paramsExec)))
    rawSeries
  }

  private def materializePeriodicSeries(queryId: String,
                                        submitTime: Long,
                                       options: QueryOptions,
                                       lp: PeriodicSeries, spreadProvider : SpreadProvider): PlanResult = {
    val rawSeries = walkLogicalPlanTree(lp.rawSeries, queryId, submitTime, options, spreadProvider)
    rawSeries.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(lp.start, lp.step, lp.end,
      None, None, Nil)))
    rawSeries
  }

  private def materializeRawSeries(queryId: String,
                                   submitTime: Long,
                                   options: QueryOptions,
                                   lp: RawSeries, spreadProvider : SpreadProvider): PlanResult = {
    val colName = lp.columns.headOption
    val renamedFilters = renameMetricFilter(lp.filters)
    val spreadChanges = spreadProvider.spreadFunc(renamedFilters)
    val schemaOpt = None
    val needsStitch = lp.rangeSelector match {
      case IntervalSelector(from, to) => spreadChanges.exists(c => c.time >= from && c.time <= to)
      case _                          => false
    }
    val execPlans = shardsFromFilters(renamedFilters, options, spreadProvider).map { shard =>
      val dispatcher = dispatcherForShard(shard)
      MultiSchemaPartitionsExec(queryId, submitTime, options.sampleLimit, dispatcher, dsRef, shard,
        renamedFilters, toChunkScanMethod(lp.rangeSelector), schemaOpt, colName)
    }
    PlanResult(execPlans, needsStitch)
  }

  private def materializeLabelValues(queryId: String,
                                     submitTime: Long,
                                     options: QueryOptions,
                                     lp: LabelValues,
                                     spreadProvider: SpreadProvider): PlanResult = {
    val filters = lp.labelConstraints.map { case (k, v) =>
      new ColumnFilter(k, Filter.Equals(v))
    }.toSeq
    // If the label is PromMetricLabel and is different than dataset's metric name,
    // replace it with dataset's metric name. (needed for prometheus plugins)
    val metricLabelIndex = lp.labelNames.indexOf(PromMetricLabel)
    val labelNames = if (metricLabelIndex > -1 && dsOptions.metricColumn != PromMetricLabel)
      lp.labelNames.updated(metricLabelIndex, dsOptions.metricColumn) else lp.labelNames

    val shardsToHit = if (shardColumns.toSet.subsetOf(lp.labelConstraints.keySet)) {
                        shardsFromFilters(filters, options, spreadProvider)
                      } else {
                        mdNoShardKeyFilterRequests.increment()
                        shardMapperFunc.assignedShards
                      }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard)
      exec.LabelValuesExec(queryId, submitTime, options.sampleLimit, dispatcher, dsRef, shard,
        filters, labelNames, lp.lookbackTimeInMillis)
    }
    PlanResult(metaExec, false)
  }

  private def materializeSeriesKeysByFilters(queryId: String,
                                     submitTime: Long,
                                     options: QueryOptions,
                                     lp: SeriesKeysByFilters, spreadProvider : SpreadProvider): PlanResult = {
    val renamedFilters = renameMetricFilter(lp.filters)
    val filterCols = lp.filters.map(_.column).toSet
    val shardsToHit = if (shardColumns.toSet.subsetOf(filterCols)) {
                        shardsFromFilters(lp.filters, options, spreadProvider)
                      } else {
                        mdNoShardKeyFilterRequests.increment()
                        shardMapperFunc.assignedShards
                      }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard)
      PartKeysExec(queryId, submitTime, options.sampleLimit, dispatcher, dsRef, shard,
        schemas.part, renamedFilters, lp.start, lp.end)
    }
    PlanResult(metaExec, false)
  }

  private def materializeRawChunkMeta(queryId: String,
                                      submitTime: Long,
                                      options: QueryOptions,
                                      lp: RawChunkMeta,
                                      spreadProvider: SpreadProvider): PlanResult = {
    // Translate column name to ID and validate here
    val colName = if (lp.column.isEmpty) None else Some(lp.column)
    val schemaOpt = None
    val renamedFilters = renameMetricFilter(lp.filters)
    val metaExec = shardsFromFilters(renamedFilters, options, spreadProvider).map { shard =>
      val dispatcher = dispatcherForShard(shard)
      SelectChunkInfosExec(queryId, submitTime, options.sampleLimit, dispatcher, dsRef, shard,
        renamedFilters, toChunkScanMethod(lp.rangeSelector), schemaOpt, colName)
    }
    PlanResult(metaExec, false)
  }

  private def materializeApplyMiscellaneousFunction(queryId: String,
                                                    submitTime: Long,
                                                    options: QueryOptions,
                                                    lp: ApplyMiscellaneousFunction,
                                                    spreadProvider: SpreadProvider): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, queryId, submitTime, options, spreadProvider)
    vectors.plans.foreach(_.addRangeVectorTransformer(MiscellaneousFunctionMapper(lp.function, lp.stringArgs)))
    vectors
  }

  private def materializeFunctionArgs(functionParams: Seq[FunctionArgsPlan],
                                      queryId: String,
                                      submitTime: Long,
                                      options: QueryOptions,
                                      spreadProvider: SpreadProvider): Seq[FuncArgs] = {
    if (functionParams.isEmpty){
      Nil
    } else {
      functionParams.map { param =>
        param match {
          case num: ScalarFixedDoublePlan => StaticFuncArgs(num.scalar, num.timeStepParams)
          case s: ScalarVaryingDoublePlan => ExecPlanFuncArgs(generateLocalExecPlan(s, queryId, submitTime, options,
                                             spreadProvider), s.timeStepParams)
          case  t: ScalarTimeBasedPlan    => TimeFuncArgs(t.rangeParams)
          case _                          => throw new UnsupportedOperationException("Invalid logical plan")
        }
      }
    }
  }

  private def materializeScalarPlan(queryId: String, submitTime: Long,
                                    options: QueryOptions,
                                    lp: ScalarVaryingDoublePlan,
                                    spreadProvider: SpreadProvider): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, queryId, submitTime, options, spreadProvider)
        if (vectors.plans.length > 1) {
          val targetActor = pickDispatcher(vectors.plans)
          val topPlan = DistConcatExec(queryId, targetActor, vectors.plans)
          topPlan.addRangeVectorTransformer((ScalarFunctionMapper(lp.function, lp.timeStepParams)))
          PlanResult(Seq(topPlan), vectors.needsStitch)
        } else {
          vectors.plans.foreach(_.addRangeVectorTransformer(ScalarFunctionMapper(lp.function, lp.timeStepParams)))
          vectors
        }
  }

  private def materializeApplySortFunction(queryId: String,
                                           submitTime: Long,
                                           options: QueryOptions,
                                           lp: ApplySortFunction,
                                           spreadProvider: SpreadProvider): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, queryId, submitTime, options, spreadProvider)
    if(vectors.plans.length > 1) {
      val targetActor = pickDispatcher(vectors.plans)
      val topPlan = DistConcatExec(queryId, targetActor, vectors.plans)
      topPlan.addRangeVectorTransformer((SortFunctionMapper(lp.function)))
      PlanResult(Seq(topPlan), vectors.needsStitch)
    } else {
      vectors.plans.foreach(_.addRangeVectorTransformer(SortFunctionMapper(lp.function)))
      vectors
    }
  }

  private def materializeScalarTimeBased(queryId: String,
                                         submitTime: Long,
                                         options: QueryOptions,
                                         lp: ScalarTimeBasedPlan,
                                         spreadProvider: SpreadProvider): PlanResult = {
    val scalarTimeBasedExec = TimeScalarGeneratorExec(queryId, dsRef, lp.rangeParams, lp.function,
      options.sampleLimit)
    PlanResult(Seq(scalarTimeBasedExec), false)
  }

  private def materializeVectorPlan(queryId: String,
                                   submitTime: Long,
                                   options: QueryOptions,
                                   lp: VectorPlan, spreadProvider : SpreadProvider): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.scalars, queryId, submitTime, options, spreadProvider)
    vectors.plans.foreach(_.addRangeVectorTransformer(VectorFunctionMapper()))
    vectors
  }

  private def materializeFixedScalar(queryId: String,
                                     submitTime: Long,
                                     options: QueryOptions,
                                     lp: ScalarFixedDoublePlan,
                                     spreadProvider: SpreadProvider): PlanResult = {
    val scalarFixedDoubleExec = ScalarFixedDoubleExec(queryId, dsRef, lp.timeStepParams, lp.scalar,
      options.sampleLimit)
    PlanResult(Seq(scalarFixedDoubleExec), false)
  }

    /**
   * Renames Prom AST __name__ metric name filters to one based on the actual metric column of the dataset,
   * if it is not the prometheus standard
   */
  private def renameMetricFilter(filters: Seq[ColumnFilter]): Seq[ColumnFilter] =
    if (dsOptions.metricColumn != PromMetricLabel) {
      filters map {
        case ColumnFilter(PromMetricLabel, filt) => ColumnFilter(dsOptions.metricColumn, filt)
        case other: ColumnFilter                 => other
      }
    } else {
      filters
    }

  private def toChunkScanMethod(rangeSelector: RangeSelector): ChunkScanMethod = {
    rangeSelector match {
      case IntervalSelector(from, to) => TimeRangeChunkScan(from, to)
      case AllChunksSelector          => AllChunkScan
      case EncodedChunksSelector      => ???
      case WriteBufferSelector        => WriteBufferChunkScan
      case InMemoryChunksSelector     => InMemoryChunkScan
      case _ => ???
    }
  }

  /**
    * Picks one dispatcher randomly from child exec plans passed in as parameter
    */
  private def pickDispatcher(children: Seq[ExecPlan]): PlanDispatcher = {
    val childTargets = children.map(_.dispatcher)
    // Above list can contain duplicate dispatchers, and we don't make them distinct.
    // Those with more shards must be weighed higher
    val rnd = ThreadLocalRandom.current()
    childTargets.iterator.drop(rnd.nextInt(childTargets.size)).next
  }
}
