package filodb.coordinator.queryengine2

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.eval.Task

import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.QueryOptions
import filodb.core.Types
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Dataset
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.store._
import filodb.prometheus.ast.Vectors.PromMetricLabel
import filodb.query.{exec, _}
import filodb.query.exec._

/**
  * FiloDB Query Engine is the facade for execution of FiloDB queries.
  * It is meant for use inside FiloDB nodes to execute materialized
  * ExecPlans as well as from the client to execute LogicalPlans.
  */
class QueryEngine(dataset: Dataset,
                  shardMapperFunc: => ShardMapper)
                   extends StrictLogging {

  /**
    * Intermediate Plan Result includes the exec plan(s) along with any state to be passed up the
    * plan building call tree during query planning.
    *
    * Not for runtime use.
    */
  private case class PlanResult(plans: Seq[ExecPlan], needsStitch: Boolean = false)

  private val mdNoShardKeyFilterRequests = Kamon.counter("queryengine-metadata-no-shardkey-requests")

  /**
    * This is the facade to trigger orchestration of the ExecPlan.
    * It sends the ExecPlan to the destination where it will be executed.
    */
  def dispatchExecPlan(execPlan: ExecPlan)
                      (implicit sched: ExecutionContext,
                       timeout: FiniteDuration): Task[QueryResponse] = {
    val currentSpan = Kamon.currentSpan()
    Kamon.withSpan(currentSpan) {
      execPlan.dispatcher.dispatch(execPlan)
    }
  }

  /**
    * Converts a LogicalPlan to the ExecPlan
    */
  def materialize(rootLogicalPlan: LogicalPlan,
                  options: QueryOptions): ExecPlan = {
    val queryId = UUID.randomUUID().toString
    val materialized = walkLogicalPlanTree(rootLogicalPlan, queryId, System.currentTimeMillis(), options) match {
      case PlanResult(Seq(justOne), stitch) =>
        if (stitch) justOne.addRangeVectorTransformer(new StitchRvsMapper())
        justOne
      case PlanResult(many, stitch) =>
        val targetActor = pickDispatcher(many)
        many(0) match {
          case lve: LabelValuesExec =>LabelValuesDistConcatExec(queryId, targetActor, many)
          case ske: PartKeysExec => PartKeysDistConcatExec(queryId, targetActor, many)
          case ep: ExecPlan =>
            val topPlan = DistConcatExec(queryId, targetActor, many)
            if (stitch) topPlan.addRangeVectorTransformer(new StitchRvsMapper())
            topPlan
        }
    }
    logger.debug(s"Materialized logical plan for dataset=${dataset.ref}:" +
      s" $rootLogicalPlan to \n${materialized.printTree()}")
    materialized
  }

  val shardColumns = dataset.options.shardKeyColumns.sorted

  private def shardsFromFilters(filters: Seq[ColumnFilter],
                                options: QueryOptions): Seq[Int] = {
    require(shardColumns.nonEmpty || options.shardOverrides.nonEmpty,
      s"Dataset ${dataset.ref} does not have shard columns defined, and shard overrides were not mentioned")

    options.shardOverrides.getOrElse {
      val shardVals = shardColumns.map { shardCol =>
        // So to compute the shard hash we need shardCol == value filter (exact equals) for each shardColumn
        filters.find(f => f.column == shardCol) match {
          case Some(ColumnFilter(_, Filter.Equals(filtVal: String))) =>
            shardCol -> RecordBuilder.trimShardColumn(dataset, shardCol, filtVal)
          case Some(ColumnFilter(_, filter)) =>
            throw new BadQueryException(s"Found filter for shard column $shardCol but " +
              s"$filter cannot be used for shard key routing")
          case _ =>
            throw new BadQueryException(s"Could not find filter for shard key column " +
              s"$shardCol, shard key hashing disabled")
        }
      }
      val metric = shardVals.find(_._1 == dataset.options.metricColumn)
                            .map(_._2)
                            .getOrElse(throw new BadQueryException(s"Could not find metric value"))
      val shardValues = shardVals.filterNot(_._1 == dataset.options.metricColumn).map(_._2)
      logger.debug(s"For shardColumns $shardColumns, extracted metric $metric and shard values $shardValues")
      val shardHash = RecordBuilder.shardKeyHash(shardValues, metric)
      shardMapperFunc.queryShards(shardHash, options.spreadProvider.spreadFunc(filters).last.spread)
    }
  }

  private def dispatcherForShard(shard: Int): PlanDispatcher = {
    val targetActor = shardMapperFunc.coordForShard(shard)
    if (targetActor == ActorRef.noSender) throw new RuntimeException("Not all shards available") // TODO fix this
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
                                  options: QueryOptions): PlanResult = {
    logicalPlan match {
      case lp: RawSeries =>                   materializeRawSeries(queryId, submitTime, options, lp)
      case lp: RawChunkMeta =>                materializeRawChunkMeta(queryId, submitTime, options, lp)
      case lp: PeriodicSeries =>              materializePeriodicSeries(queryId, submitTime, options, lp)
      case lp: PeriodicSeriesWithWindowing => materializePeriodicSeriesWithWindowing(queryId, submitTime, options, lp)
      case lp: ApplyInstantFunction =>        materializeApplyInstantFunction(queryId, submitTime, options, lp)
      case lp: Aggregate =>                   materializeAggregate(queryId, submitTime, options, lp)
      case lp: BinaryJoin =>                  materializeBinaryJoin(queryId, submitTime, options, lp)
      case lp: ScalarVectorBinaryOperation => materializeScalarVectorBinOp(queryId, submitTime, options, lp)
      case lp: LabelValues =>                 materializeLabelValues(queryId, submitTime, options, lp)
      case lp: SeriesKeysByFilters =>         materializeSeriesKeysByFilters(queryId, submitTime, options, lp)
      case lp: ApplyMiscellaneousFunction =>  materializeApplyMiscellaneousFunction(queryId, submitTime, options, lp)
    }
  }

  private def materializeScalarVectorBinOp(queryId: String,
                                           submitTime: Long,
                                           options: QueryOptions,
                                           lp: ScalarVectorBinaryOperation): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vector, queryId, submitTime, options)
    vectors.plans.foreach(_.addRangeVectorTransformer(ScalarOperationMapper(lp.operator, lp.scalar, lp.scalarIsLhs)))
    vectors
  }

  private def materializeBinaryJoin(queryId: String,
                                    submitTime: Long,
                                    options: QueryOptions,
                                    lp: BinaryJoin): PlanResult = {
    val lhs = walkLogicalPlanTree(lp.lhs, queryId, submitTime, options)
    val stitchedLhs = if (lhs.needsStitch) Seq(StitchRvsExec(queryId, pickDispatcher(lhs.plans), lhs.plans))
                      else lhs.plans
    val rhs =  walkLogicalPlanTree(lp.rhs, queryId, submitTime, options)
    val stitchedRhs = if (rhs.needsStitch) Seq(StitchRvsExec(queryId, pickDispatcher(rhs.plans), rhs.plans))
                      else rhs.plans
    // TODO Currently we create separate exec plan node for stitching.
    // Ideally, we can go one step further and add capability to NonLeafNode plans to pre-process
    // and transform child results individually before composing child results together.
    // In theory, more efficient to use transformer than to have separate exec plan node to avoid IO.
    // In the interest of keeping it simple, deferring decorations to the ExecPlan. Add only if needed after measuring.

    val targetActor = pickDispatcher(stitchedLhs ++ stitchedRhs)
    val joined = Seq(BinaryJoinExec(queryId, targetActor, stitchedLhs, stitchedRhs, lp.operator, lp.cardinality,
                                    lp.on, lp.ignoring))
    PlanResult(joined, false)
  }

  private def materializeAggregate(queryId: String,
                                   submitTime: Long,
                                   options: QueryOptions,
                                   lp: Aggregate): PlanResult = {
    val toReduce = walkLogicalPlanTree(lp.vectors, queryId, submitTime, options)
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
    toReduce.plans.foreach(_.addRangeVectorTransformer(AggregateMapReduce(lp.operator, lp.params, lp.without, lp.by)))
    // One could do another level of aggregation per node too. Ignoring for now
    val reduceDispatcher = pickDispatcher(toReduce.plans)
    val reducer = ReduceAggregateExec(queryId, reduceDispatcher, toReduce.plans, lp.operator, lp.params)
    reducer.addRangeVectorTransformer(AggregatePresenter(lp.operator, lp.params))
    PlanResult(Seq(reducer), false) // since we have aggregated, no stitching
  }

  private def materializeApplyInstantFunction(queryId: String,
                                              submitTime: Long,
                                              options: QueryOptions,
                                              lp: ApplyInstantFunction): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, queryId, submitTime, options)
    vectors.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(lp.function, lp.functionArgs)))
    vectors
  }

  private def materializePeriodicSeriesWithWindowing(queryId: String,
                                                     submitTime: Long,
                                                     options: QueryOptions,
                                                     lp: PeriodicSeriesWithWindowing): PlanResult ={
    val rawSeries = walkLogicalPlanTree(lp.rawSeries, queryId, submitTime, options)
    rawSeries.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(lp.start, lp.step,
      lp.end, Some(lp.window), Some(lp.function), lp.functionArgs)))
    rawSeries
  }

  private def materializePeriodicSeries(queryId: String,
                                        submitTime: Long,
                                       options: QueryOptions,
                                       lp: PeriodicSeries): PlanResult = {
    val rawSeries = walkLogicalPlanTree(lp.rawSeries, queryId, submitTime, options)
    rawSeries.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(lp.start, lp.step, lp.end,
      None, None, Nil)))
    rawSeries
  }

  private def materializeRawSeries(queryId: String,
                                   submitTime: Long,
                                   options: QueryOptions,
                                   lp: RawSeries): PlanResult = {
    val colIDs = getColumnIDs(dataset, lp.columns)
    val renamedFilters = renameMetricFilter(lp.filters)
    val spreadChanges = options.spreadProvider.spreadFunc(renamedFilters)
    val needsStitch = lp.rangeSelector match {
      case IntervalSelector(from, to) => spreadChanges.exists(c => c.time >= from && c.time <= to)
      case _                          => false
    }
    val execPlans = shardsFromFilters(renamedFilters, options).map { shard =>
      val dispatcher = dispatcherForShard(shard)
      SelectRawPartitionsExec(queryId, submitTime, options.sampleLimit, dispatcher, dataset.ref, shard,
        renamedFilters, toChunkScanMethod(lp.rangeSelector), colIDs)
    }
    PlanResult(execPlans, needsStitch)
  }

  private def materializeLabelValues(queryId: String,
                                      submitTime: Long,
                                      options: QueryOptions,
                                      lp: LabelValues): PlanResult = {
    val filters = lp.labelConstraints.map { case (k, v) =>
      new ColumnFilter(k, Filter.Equals(v))
    }.toSeq
    // If the label is PromMetricLabel and is different than dataset's metric name,
    // replace it with dataset's metric name. (needed for prometheus plugins)
    val metricLabelIndex = lp.labelNames.indexOf(PromMetricLabel)
    val labelNames = if (metricLabelIndex > -1 && dataset.options.metricColumn != PromMetricLabel)
      lp.labelNames.updated(metricLabelIndex, dataset.options.metricColumn) else lp.labelNames

    val shardsToHit = if (shardColumns.toSet.subsetOf(lp.labelConstraints.keySet)) {
                        shardsFromFilters(filters, options)
                      } else {
                        mdNoShardKeyFilterRequests.increment()
                        shardMapperFunc.assignedShards
                      }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard)
      exec.LabelValuesExec(queryId, submitTime, options.sampleLimit, dispatcher, dataset.ref, shard,
        filters, labelNames, lp.lookbackTimeInMillis)
    }
    PlanResult(metaExec, false)
  }

  private def materializeSeriesKeysByFilters(queryId: String,
                                     submitTime: Long,
                                     options: QueryOptions,
                                     lp: SeriesKeysByFilters): PlanResult = {
    val renamedFilters = renameMetricFilter(lp.filters)
    val filterCols = lp.filters.map(_.column).toSet
    val shardsToHit = if (shardColumns.toSet.subsetOf(filterCols)) {
                        shardsFromFilters(lp.filters, options)
                      } else {
                        mdNoShardKeyFilterRequests.increment()
                        shardMapperFunc.assignedShards
                      }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard)
      PartKeysExec(queryId, submitTime, options.sampleLimit, dispatcher, dataset.ref, shard,
        renamedFilters, lp.start, lp.end)
    }
    PlanResult(metaExec, false)
  }

  private def materializeRawChunkMeta(queryId: String,
                                      submitTime: Long,
                                      options: QueryOptions,
                                      lp: RawChunkMeta): PlanResult = {
    // Translate column name to ID and validate here
    val colName = if (lp.column.isEmpty) dataset.options.valueColumn else lp.column
    val colID = dataset.colIDs(colName).get.head
    val renamedFilters = renameMetricFilter(lp.filters)
    val metaExec = shardsFromFilters(renamedFilters, options).map { shard =>
      val dispatcher = dispatcherForShard(shard)
      SelectChunkInfosExec(queryId, submitTime, options.sampleLimit, dispatcher, dataset.ref, shard,
        renamedFilters, toChunkScanMethod(lp.rangeSelector), colID)
    }
    PlanResult(metaExec, false)
  }

  private def materializeApplyMiscellaneousFunction(queryId: String,
                                              submitTime: Long,
                                              options: QueryOptions,
                                              lp: ApplyMiscellaneousFunction): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, queryId, submitTime, options)
    vectors.plans.foreach(_.addRangeVectorTransformer(MiscellaneousFunctionMapper(lp.function, lp.functionArgs)))
    vectors
  }

  /**
   * Renames Prom AST __name__ metric name filters to one based on the actual metric column of the dataset,
   * if it is not the prometheus standard
   */
  private def renameMetricFilter(filters: Seq[ColumnFilter]): Seq[ColumnFilter] =
    if (dataset.options.metricColumn != PromMetricLabel) {
      filters map {
        case ColumnFilter(PromMetricLabel, filt) => ColumnFilter(dataset.options.metricColumn, filt)
        case other: ColumnFilter                 => other
      }
    } else {
      filters
    }

  /**
    * Convert column name strings into columnIDs.  NOTE: column names should not include row key columns
    * as those are automatically prepended.
    */
  private def getColumnIDs(dataset: Dataset, cols: Seq[String]): Seq[Types.ColumnId] = {
    val realCols = if (cols.isEmpty) Seq(dataset.options.valueColumn) else cols
    val ids = dataset.colIDs(realCols: _*)
      .recover(missing => throw new BadQueryException(s"Undefined columns $missing"))
      .get
    // avoid duplication if first ids are already row keys
    if (ids.take(dataset.rowKeyIDs.length) == dataset.rowKeyIDs) { ids }
    else { dataset.rowKeyIDs ++ ids }
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
