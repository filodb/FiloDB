package filodb.query.exec

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.reactive.Observable
import spire.syntax.cfor._

import filodb.core.memstore.SchemaMismatch
import filodb.core.query._
import filodb.memory.format.ZeroCopyUTF8String
import filodb.query._
import filodb.query.exec.aggregator.RowAggregator

/**
  * Reduce combined aggregates from children. Can be applied in a
  * hierarchical manner multiple times to arrive at result.
  */
trait ReduceAggregateExec extends NonLeafExecPlan {
  def childAggregates: Seq[ExecPlan]

  def children: Seq[ExecPlan] = childAggregates
  def aggrOp: AggregationOperator
  def aggrParams: Seq[Any]

  protected def args: String = s"aggrOp=$aggrOp, aggrParams=$aggrParams"

  protected def composeStreaming(childResponses: Observable[(Observable[RangeVector], Int)],
                                 schemas: Observable[(ResultSchema, Int)],
                                 querySession: QuerySession): Observable[RangeVector] = {
    val firstSchema = schemas.map(_._1).firstOptionL.map(_.getOrElse(ResultSchema.empty))
    val results = childResponses.flatMap(_._1)
    reduce(results, firstSchema, querySession)
  }

  protected def compose(childResponses: Observable[(QueryResult, Int)],
                        firstSchema: Task[ResultSchema],
                        querySession: QuerySession): Observable[RangeVector] = {
    val results = childResponses.flatMap(res => Observable.fromIterable(res._1.result))
    reduce(results, firstSchema, querySession)
  }

  private def reduce(results: Observable[RangeVector],
                     firstSchema: Task[ResultSchema],
                     querySession: QuerySession) = {
    val task = for { schema <- firstSchema}
      yield {
        // For absent function schema can be empty
        if (schema == ResultSchema.empty) Observable.empty
        else {
          val aggregator = RowAggregator(aggrOp, aggrParams, schema)
          RangeVectorAggregator.mapReduce(
            aggregator, skipMapPhase = true, results, rv => rv.key,
            //querySession.qContext.plannerParams.enforcedLimits.groupByCardinality,
            queryContext)
        }
      }
    Observable.fromTask(task).flatten
  }
}

/**
  * Use when child ExecPlan's span single local partition
  */
final case class LocalPartitionReduceAggregateExec(queryContext: QueryContext,
                                                   dispatcher: PlanDispatcher,
                                                   childAggregates: Seq[ExecPlan],
                                                   aggrOp: AggregationOperator,
                                                   aggrParams: Seq[Any]) extends ReduceAggregateExec {
  /**
   * Requiring strict result schema match for Aggregation within filodb cluster
   * since fixedVectorLen presence will enable fast-reduce when possible
   */
  override def reduceSchemas(r1: ResultSchema, r2: ResultSchema): ResultSchema = {
    if (r1.isEmpty) r2
    else if (r2.isEmpty) r1
    else if (r1 != r2) {
      throw SchemaMismatch(r1.toString, r2.toString, getClass.getSimpleName)
    } else r1
  }
}

/**
  * Use when child ExecPlan's span multiple partitions
  */
final case class MultiPartitionReduceAggregateExec(queryContext: QueryContext,
                                                   dispatcher: PlanDispatcher,
                                                   childAggregates: Seq[ExecPlan],
                                                   aggrOp: AggregationOperator,
                                                   aggrParams: Seq[Any]) extends ReduceAggregateExec {
  // retain non-strict reduceSchemas method in super (ExecPlan trait) since
  // colIds, fixedVectorLen etc are not transmitted in cross-partition json response
}

/**
  * Performs aggregation operation across RangeVectors within a shard
  */
final case class AggregateMapReduce(aggrOp: AggregationOperator,
                                    aggrParams: Seq[Any],
                                    clauseOpt: Option[AggregateClause] = None,
                                    funcParams: Seq[FuncArgs] = Nil) extends RangeVectorTransformer {

  import filodb.query.AggregateClause.ClauseType

  val clauseLabelSet : Set[ZeroCopyUTF8String] = {
    if (clauseOpt.isEmpty) Set()
    else clauseOpt.get.labels.map(ZeroCopyUTF8String(_)).toSet
  }

  protected[exec] def args: String = {
    var byLabels: Seq[String] = Nil
    var withoutLabels: Seq[String] = Nil
    if (clauseOpt.nonEmpty) {
      clauseOpt.get.clauseType match {
        case ClauseType.By => byLabels = clauseOpt.get.labels
        case ClauseType.Without => withoutLabels = clauseOpt.get.labels
      }
    }
    s"aggrOp=$aggrOp, aggrParams=$aggrParams, without=$withoutLabels, by=$byLabels"
  }

  def apply(source: Observable[RangeVector],
            querySession: QuerySession,
            limit: Int,
            sourceSchema: ResultSchema,
            paramResponse: Seq[Observable[ScalarRangeVector]] = Nil): Observable[RangeVector] = {
    val aggregator = RowAggregator(aggrOp, aggrParams, sourceSchema)

    def grouping(rv: RangeVector): RangeVectorKey = {
      val rvLabelValues = rv.key.labelValues
      val groupBy: Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = clauseOpt.map { clause =>
        clause.clauseType match {
          case ClauseType.By => rvLabelValues.filter(lv => clauseLabelSet.contains(lv._1))
          case ClauseType.Without => rvLabelValues.filterNot(lv => clauseLabelSet.contains(lv._1))
        }
      }.getOrElse(Map())
      CustomRangeVectorKey(groupBy)
    }

    // IF no grouping is done AND prev transformer is Periodic (has fixed length), use optimal path
    if (clauseOpt.isEmpty && sourceSchema.fixedVectorLen.isDefined) {
      sourceSchema.fixedVectorLen.filter(_ <= querySession.queryConfig.fastReduceMaxWindows).map { numWindows =>
        RangeVectorAggregator.fastReduce(aggregator, false, source, numWindows)
      }.getOrElse {
        RangeVectorAggregator.mapReduce(aggregator, skipMapPhase = false, source, grouping, querySession.qContext)
      }
    } else {
      RangeVectorAggregator.mapReduce(aggregator, skipMapPhase = false, source, grouping, querySession.qContext)
    }
  }

  override def schema(source: ResultSchema): ResultSchema = {
    val aggregator = RowAggregator(aggrOp, aggrParams, source)
    // TODO we assume that second column needs to be aggregated. Other dataset types need to be accommodated.
    aggregator.reductionSchema(source)
  }
}

final case class AggregatePresenter(aggrOp: AggregationOperator,
                                    aggrParams: Seq[Any],
                                    rangeParams: RangeParams,
                                    funcParams: Seq[FuncArgs] = Nil) extends RangeVectorTransformer {

  protected[exec] def args: String = s"aggrOp=$aggrOp, aggrParams=$aggrParams, rangeParams=$rangeParams"

  def apply(source: Observable[RangeVector],
            querySession: QuerySession,
            limit: Int,
            sourceSchema: ResultSchema,
            paramResponse: Seq[Observable[ScalarRangeVector]]): Observable[RangeVector] = {
    val aggregator = RowAggregator(aggrOp, aggrParams, sourceSchema)
    RangeVectorAggregator.present(aggregator, source, limit, rangeParams, querySession.queryStats)
  }

  override def schema(source: ResultSchema): ResultSchema = {
    val aggregator = RowAggregator(aggrOp, aggrParams, source)
    aggregator.presentationSchema(source)
  }
}

/**
  * Aggregation has three phases:
  * 1. Map: Map raw data points to AggregateResult RowReaders.
  * 2. Reduce: Reduce aggregate result RowReaders into fewer aggregate results. This may happen multiple times.
  * 3. Present: Convert the aggregation result into the final presentable result.
  *
  * This singleton is the facade for the above operations.
  */
object RangeVectorAggregator extends StrictLogging {

  trait CloseableIterator[R] extends Iterator[R] {
    def close(): Unit
  }

  /**
    * This method is the facade for map and reduce steps of the aggregation.
    * In the reduction-only (non-leaf) phases, skipMapPhase should be true.
    */
  def mapReduce(rowAgg: RowAggregator,
                skipMapPhase: Boolean,
                source: Observable[RangeVector],
                grouping: RangeVector => RangeVectorKey,
                queryContext: QueryContext): Observable[RangeVector] = {
    // reduce the range vectors using the foldLeft construct. This results in one aggregate per group.
    val task = source.toListL.map { rvs =>
      val period = rvs.headOption.flatMap(_.outputRange)
      // now reduce each group and create one result range vector per group
      val groupedResult = mapReduceInternal(rvs, rowAgg, skipMapPhase, grouping, queryContext)

      // if group-by cardinality breaches the limit, throw exception
      val groupByEnforcedLimit = queryContext.plannerParams.enforcedLimits.groupByCardinality
      if (groupedResult.size > groupByEnforcedLimit) {
        logger.warn(queryContext.getQueryLogLine(
          s"Exceeded enforced group-by cardinality limit ${groupByEnforcedLimit}. "
        ))
        throw new BadQueryException(
          s"Query exceeded group-by cardinality limit ${groupByEnforcedLimit}. " +
          "Try applying more filters or reduce query range. "
        )
      }
      val groupByWarnLimit = queryContext.plannerParams.enforcedLimits.groupByCardinality
      if (groupedResult.size > groupByWarnLimit) {
        logger.info(queryContext.getQueryLogLine(
          s"Exceeded warning group-by cardinality limit ${groupByWarnLimit}. "
        ))
      }
      groupedResult.map { case (rvk, aggHolder) =>
        val rowIterator = new CustomCloseCursor(aggHolder.map(_.toRowReader))(aggHolder.close())
        IteratorBackedRangeVector(rvk, rowIterator, period)
      }
    }
    Observable.fromTask(task).flatMap(rvs => Observable.fromIterable(rvs))
  }

  /**
    * This method is the facade for the present step of the aggregation
    */
  def present(aggregator: RowAggregator,
              source: Observable[RangeVector],
              limit: Int,
              rangeParams: RangeParams,
              queryStats: QueryStats): Observable[RangeVector] = {
    source.flatMap(rv => Observable.fromIterable(aggregator.present(rv, limit, rangeParams, queryStats)))
  }

  private def mapReduceInternal(rvs: List[RangeVector],
                                rowAgg: RowAggregator,
                                skipMapPhase: Boolean,
                                grouping: RangeVector => RangeVectorKey,
                                queryContext: QueryContext):
                                Map[RangeVectorKey, CloseableIterator[rowAgg.AggHolderType]] = {
    logger.trace(s"mapReduceInternal on ${rvs.size} RangeVectors...")
    var acc = rowAgg.zero
    val mapInto = rowAgg.newRowToMapInto
    rvs.groupBy(grouping).mapValues { rvs =>
      new CloseableIterator[rowAgg.AggHolderType] {
        // create tuple from rv since rows() will create new iter each time
        val itsAndKeys = rvs.map { rv => (rv.rows(), rv.key) }
        def hasNext: Boolean = {
          // Dont use forAll since it short-circuits hasNext invocation
          // It is important to invoke hasNext on all iterators to release shared locks
          var ret = false
          itsAndKeys.foreach { itAndKey =>
            if (itAndKey._1.hasNext) ret = true
          }
          ret
        }
        def next(): rowAgg.AggHolderType = {
          acc.resetToZero()
          queryContext.checkQueryTimeout(this.getClass.getName)
          itsAndKeys.foreach { case (rowIter, rvk) =>
            val mapped = if (skipMapPhase) rowIter.next() else rowAgg.map(rvk, rowIter.next(), mapInto)
            acc = if (skipMapPhase) rowAgg.reduceAggregate(acc, mapped) else rowAgg.reduceMappedRow(acc, mapped)
          }
          acc
        }
        def close() = rvs.foreach(_.rows().close())
      }
    }
  }

  /**
   * A fast reduce method intended specifically for the case when no grouping needs to be done AND
   * the previous transformer is a PeriodicSampleMapper with fixed output lengths.
   * It's much faster than mapReduce() since it iterates through each vector first and then from vector to vector.
   * Time wise first iteration also uses less memory for high-cardinality use cases and reduces the
   * time window of holding chunk map locks to each time series, instead of the entire query.
   */
  // scalastyle:off method.length
  def fastReduce(rowAgg: RowAggregator,
                 skipMapPhase: Boolean,
                 source: Observable[RangeVector],
                 outputLen: Int): Observable[RangeVector] = {
    // Can't use an Array here because rowAgg.AggHolderType does not have a ClassTag
    val accs = collection.mutable.ArrayBuffer.fill(outputLen)(rowAgg.zero)
    var count = 0
    var period: Option[RvRange] = None

    // FoldLeft means we create the source PeriodicMapper etc and process immediately.  We can release locks right away
    // NOTE: ChunkedWindowIterator automatically releases locks after last window.  So it should all just work.  :)
    val aggObs = if (skipMapPhase) {
      source.foldLeft(accs) { case (_, rv) =>
        count += 1
        val rowIter = rv.rows
        if (period.isEmpty) period = rv.outputRange
        try {
          cforRange { 0 until outputLen } { i =>
            accs(i) = rowAgg.reduceAggregate(accs(i), rowIter.next)
          }
        } finally {
          rowIter.close()
        }
        accs
      }
    } else {
      val mapIntos = Array.fill(outputLen)(rowAgg.newRowToMapInto)
      source.foldLeft(accs) { case (_, rv) =>
        count += 1
        val rowIter = rv.rows
        if (period.isEmpty) period = rv.outputRange
        try {
          cforRange { 0 until outputLen } { i =>
            val mapped = rowAgg.map(rv.key, rowIter.next, mapIntos(i))
            accs(i) = rowAgg.reduceMappedRow(accs(i), mapped)
          }
        } finally {
          rowIter.close()
        }
        accs
      }
    }

    // convert the aggregations to range vectors
    aggObs.flatMap { _ =>
      if (count > 0) {
        import NoCloseCursor._ // The base range vectors are already closed, so no close propagation needed
        Observable.now(IteratorBackedRangeVector(CustomRangeVectorKey.empty,
          NoCloseCursor(accs.toIterator.map(_.toRowReader)), period))
      } else {
        Observable.empty
      }
    }
  }
}
