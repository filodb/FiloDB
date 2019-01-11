package filodb.coordinator.queryengine2

import java.util.{SplittableRandom, UUID}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task

import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.QueryOptions
import filodb.core.Types
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Dataset
import filodb.core.query.{ColumnFilter, Filter}
import filodb.prometheus.ast.Vectors.PromMetricLabel
import filodb.query.{exec, _}
import filodb.query.InstantFunctionId.HistogramQuantile
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
    * This is the facade to trigger orchestration of the ExecPlan.
    * It sends the ExecPlan to the destination where it will be executed.
    */
  def dispatchExecPlan(execPlan: ExecPlan)
                      (implicit sched: ExecutionContext,
                       timeout: FiniteDuration): Task[QueryResponse] = {
    execPlan.dispatcher.dispatch(execPlan)
  }

  /**
    * Converts a LogicalPlan to the ExecPlan
    */
  def materialize(rootLogicalPlan: LogicalPlan,
                  options: QueryOptions): ExecPlan = {
    val queryId = UUID.randomUUID().toString
    val materialized = walkLogicalPlanTree(rootLogicalPlan, queryId, System.currentTimeMillis(), options) match {
      case Seq(justOne) =>
        justOne
      case many =>
        val targetActor = pickDispatcher(many)
        many(0) match {
          case lve: LabelValuesExec => LabelValuesDistConcatExec(queryId, targetActor, many)
          case ske: PartKeysExec => PartKeysDistConcatExec(queryId, targetActor, many)
          case ep: ExecPlan => DistConcatExec(queryId, targetActor, many)
        }
    }
    logger.debug(s"Materialized logical plan: $rootLogicalPlan to \n${materialized.printTree()}")
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
      val metric = shardVals.filter(_._1 == dataset.options.metricColumn).headOption
                            .map(_._2)
                            .getOrElse(throw new BadQueryException(s"Could not find metric value"))
      val shardValues = shardVals.filterNot(_._1 == dataset.options.metricColumn).map(_._2)
      logger.debug(s"For shardColumns $shardColumns, extracted metric $metric and shard values $shardValues")
      val shardHash = RecordBuilder.shardKeyHash(shardValues, metric)
      shardMapperFunc.queryShards(shardHash, options.spreadFunc(filters))
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
                                  options: QueryOptions): Seq[ExecPlan] = {
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
    }
  }

  private def materializeScalarVectorBinOp(queryId: String,
                                           submitTime: Long,
                                           options: QueryOptions,
                                           lp: ScalarVectorBinaryOperation): Seq[ExecPlan] = {
    val vectors = walkLogicalPlanTree(lp.vector, queryId, submitTime, options)
    vectors.foreach(_.addRangeVectorTransformer(ScalarOperationMapper(lp.operator, lp.scalar, lp.scalarIsLhs)))
    vectors
  }

  private def materializeBinaryJoin(queryId: String,
                                    submitTime: Long,
                                    options: QueryOptions,
                                    lp: BinaryJoin): Seq[ExecPlan] = {
    val lhs = walkLogicalPlanTree(lp.lhs, queryId, submitTime, options)
    val rhs = walkLogicalPlanTree(lp.rhs, queryId, submitTime, options)
    val targetActor = pickDispatcher(lhs ++ rhs)
    Seq(BinaryJoinExec(queryId, targetActor, lhs, rhs, lp.operator, lp.cardinality, lp.on, lp.ignoring))
  }

  private def materializeAggregate(queryId: String,
                                   submitTime: Long,
                                   options: QueryOptions,
                                   lp: Aggregate): Seq[ExecPlan] = {
    val toReduce = walkLogicalPlanTree(lp.vectors, queryId, submitTime, options) // Now we have one exec plan per shard
    toReduce.foreach(_.addRangeVectorTransformer(AggregateMapReduce(lp.operator, lp.params, lp.without, lp.by)))
    // One could do another level of aggregation per node too. Ignoring for now
    val reduceDispatcher = pickDispatcher(toReduce)
    val reducer = ReduceAggregateExec(queryId, reduceDispatcher, toReduce, lp.operator, lp.params)
    reducer.addRangeVectorTransformer(AggregatePresenter(lp.operator, lp.params))
    Seq(reducer)
  }

  private def materializeApplyInstantFunction(queryId: String,
                                              submitTime: Long,
                                              options: QueryOptions,
                                              lp: ApplyInstantFunction): Seq[ExecPlan] = {
    val vectors = walkLogicalPlanTree(lp.vectors, queryId, submitTime, options)
    lp.function match {
      case HistogramQuantile =>
        vectors.foreach(_.addRangeVectorTransformer(HistogramQuantileMapper(lp.functionArgs)))
      case _ =>
        vectors.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(lp.function, lp.functionArgs)))
    }
    vectors
  }

  private def materializePeriodicSeriesWithWindowing(queryId: String,
                                                     submitTime: Long,
                                                     options: QueryOptions,
                                                     lp: PeriodicSeriesWithWindowing): Seq[ExecPlan] = {
    val rawSeries = walkLogicalPlanTree(lp.rawSeries, queryId, submitTime, options)
    rawSeries.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(lp.start, lp.step,
      lp.end, Some(lp.window), Some(lp.function), lp.functionArgs)))
    rawSeries
  }

  private def materializePeriodicSeries(queryId: String,
                                        submitTime: Long,
                                       options: QueryOptions,
                                       lp: PeriodicSeries): Seq[ExecPlan] = {
    val rawSeries = walkLogicalPlanTree(lp.rawSeries, queryId, submitTime, options)
    rawSeries.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(lp.start, lp.step, lp.end,
      None, None, Nil)))
    rawSeries
  }

  private def materializeRawSeries(queryId: String,
                                   submitTime: Long,
                                   options: QueryOptions,
                                   lp: RawSeries): Seq[ExecPlan] = {
    val colIDs = getColumnIDs(dataset, lp.columns)
    val renamedFilters = renameMetricFilter(lp.filters)
    shardsFromFilters(renamedFilters, options).map { shard =>
      val dispatcher = dispatcherForShard(shard)
      SelectRawPartitionsExec(queryId, submitTime, options.itemLimit, dispatcher, dataset.ref, shard,
        renamedFilters, toRowKeyRange(lp.rangeSelector), colIDs)
    }
  }

  private def materializeLabelValues(queryId: String,
                                      submitTime: Long,
                                      options: QueryOptions,
                                      lp: LabelValues): Seq[LabelValuesExec] = {
    val filters = lp.labelConstraints.map { case (k, v) =>
      new ColumnFilter(k, Filter.Equals(v))
    }.toSeq
    // If the label is PromMetricLabel and is different than dataset's metric name,
    // replace it with dataset's metric name. (needed for prometheus plugins)
    val labelName = if (PromMetricLabel == lp.labelName && dataset.options.metricColumn != PromMetricLabel)
      dataset.options.metricColumn else lp.labelName

    val shardsToHit = if (shardColumns.toSet.subsetOf(lp.labelConstraints.keySet)) {
                        shardsFromFilters(filters, options)
                      } else {
                        shardMapperFunc.assignedShards
                      }
    shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard)
      exec.LabelValuesExec(queryId, submitTime, options.itemLimit, dispatcher, dataset.ref, shard,
        filters, labelName, lp.lookbackTimeInMillis)
    }
  }

  private def materializeSeriesKeysByFilters(queryId: String,
                                     submitTime: Long,
                                     options: QueryOptions,
                                     lp: SeriesKeysByFilters): Seq[PartKeysExec] = {
    val renamedFilters = renameMetricFilter(lp.filters)
    shardsFromFilters(renamedFilters, options).map { shard =>
      val dispatcher = dispatcherForShard(shard)
      PartKeysExec(queryId, submitTime, options.itemLimit, dispatcher, dataset.ref, shard,
        renamedFilters, lp.start, lp.end)
    }
  }

  private def materializeRawChunkMeta(queryId: String,
                                      submitTime: Long,
                                      options: QueryOptions,
                                      lp: RawChunkMeta): Seq[ExecPlan] = {
    // Translate column name to ID and validate here
    val colName = if (lp.column.isEmpty) dataset.options.valueColumn else lp.column
    val colID = dataset.colIDs(colName).get.head
    val renamedFilters = renameMetricFilter(lp.filters)
    shardsFromFilters(renamedFilters, options).map { shard =>
      val dispatcher = dispatcherForShard(shard)
      SelectChunkInfosExec(queryId, submitTime, options.itemLimit, dispatcher, dataset.ref, shard,
        renamedFilters, toRowKeyRange(lp.rangeSelector), colID)
    }
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

  private def toRowKeyRange(rangeSelector: RangeSelector): RowKeyRange = {
    rangeSelector match {
      case IntervalSelector(from, to) => RowKeyInterval(BinaryRecord(dataset, from),
                                                        BinaryRecord(dataset, to))
      case AllChunksSelector          => AllChunks
      case EncodedChunksSelector      => EncodedChunks
      case WriteBufferSelector        => WriteBuffers
      case InMemoryChunksSelector     => InMemoryChunks
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
    childTargets.iterator.drop(QueryEngine.random.nextInt(childTargets.size)).next
  }
}

object QueryEngine {
  val random = new SplittableRandom()
}
