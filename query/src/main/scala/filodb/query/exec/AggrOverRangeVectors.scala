package filodb.query.exec

import scala.collection.mutable.ArrayBuffer

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.reactive.Observable
import spire.syntax.cfor._

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

  protected def compose(childResponses: Observable[(QueryResponse, Int)],
                        firstSchema: Task[ResultSchema],
                        querySession: QuerySession): Observable[RangeVector] = {
    val results = childResponses.flatMap {
        case (QueryResult(_, _, result), _) => Observable.fromIterable(result)
        case (QueryError(_, ex), _)         => throw ex
    }
    val task = for { schema <- firstSchema }
               yield {
                 val aggregator = RowAggregator(aggrOp, aggrParams, schema)
                 RangeVectorAggregator.mapReduce(aggregator, skipMapPhase = true, results, rv => rv.key,
                   querySession.qContext.groupByCardLimit)
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
                                                   aggrParams: Seq[Any]) extends ReduceAggregateExec

/**
  * Use when child ExecPlan's span multiple partitions
  */
final case class MultiPartitionReduceAggregateExec(queryContext: QueryContext,
                                                   dispatcher: PlanDispatcher,
                                                   childAggregates: Seq[ExecPlan],
                                                   aggrOp: AggregationOperator,
                                                   aggrParams: Seq[Any]) extends ReduceAggregateExec {
  // overriden since it can reduce schemas with different vector lengths as long as the columns are same
  override def reduceSchemas(rs: ResultSchema, resp: QueryResult): ResultSchema =
    IgnoreFixedVectorLenAndColumnNamesSchemaReducer.reduceSchema(rs, resp)
}




/**
  * Performs aggregation operation across RangeVectors within a shard
  */
final case class AggregateMapReduce(aggrOp: AggregationOperator,
                                    aggrParams: Seq[Any],
                                    without: Seq[String],
                                    by: Seq[String],
                                    funcParams: Seq[FuncArgs] = Nil) extends RangeVectorTransformer {
  require(without == Nil || by == Nil, "Cannot specify both without and by clause")
  val withoutLabels = without.map(ZeroCopyUTF8String(_)).toSet
  val byLabels = by.map(ZeroCopyUTF8String(_)).toSet

  protected[exec] def args: String =
    s"aggrOp=$aggrOp, aggrParams=$aggrParams, without=$without, by=$by"

  def apply(source: Observable[RangeVector],
            querySession: QuerySession,
            limit: Int,
            sourceSchema: ResultSchema,
            paramResponse: Seq[Observable[ScalarRangeVector]] = Nil): Observable[RangeVector] = {
    val aggregator = RowAggregator(aggrOp, aggrParams, sourceSchema)

    def grouping(rv: RangeVector): RangeVectorKey = {
      val groupBy: Map[ZeroCopyUTF8String, ZeroCopyUTF8String] =
        if (by.nonEmpty) rv.key.labelValues.filter(lv => byLabels.contains(lv._1))
        else if (without.nonEmpty) rv.key.labelValues.filterNot(lv =>withoutLabels.contains(lv._1))
        else Map.empty
      CustomRangeVectorKey(groupBy)
    }

    // IF no grouping is done AND prev transformer is Periodic (has fixed length), use optimal path
    if (without.isEmpty && by.isEmpty && sourceSchema.fixedVectorLen.isDefined) {
      sourceSchema.fixedVectorLen.filter(_ <= querySession.queryConfig.fastReduceMaxWindows).map { numWindows =>
        RangeVectorAggregator.fastReduce(aggregator, false, source, numWindows)
      }.getOrElse {
        RangeVectorAggregator.mapReduce(aggregator, skipMapPhase = false, source, grouping,
          querySession.qContext.groupByCardLimit)
      }
    } else {
      RangeVectorAggregator.mapReduce(aggregator, skipMapPhase = false, source, grouping,
        querySession.qContext.groupByCardLimit)
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
                                    funcParams: Seq[FuncArgs] = Nil) extends RangeVectorTransformer {

  protected[exec] def args: String = s"aggrOp=$aggrOp, aggrParams=$aggrParams"

  def apply(source: Observable[RangeVector],
            querySession: QuerySession,
            limit: Int,
            sourceSchema: ResultSchema,
            paramResponse: Seq[Observable[ScalarRangeVector]]): Observable[RangeVector] = {
    val aggregator = RowAggregator(aggrOp, aggrParams, sourceSchema)
    RangeVectorAggregator.present(aggregator, source, limit)
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
                cardinalityLimit: Int = Int.MaxValue): Observable[RangeVector] = {
    // reduce the range vectors using the foldLeft construct. This results in one aggregate per group.
    val task = source.toListL.map { rvs =>
      // now reduce each group and create one result range vector per group
      val groupedResult = mapReduceInternal(rvs, rowAgg, skipMapPhase, grouping)

      // if group-by cardinality breaches the limit, throw exception
      if (groupedResult.size > cardinalityLimit)
        throw new BadQueryException(s"This query results in more than $cardinalityLimit group-by cardinality limit. " +
          s"Try applying more filters")
      groupedResult.map { case (rvk, aggHolder) =>
        val rowIterator = new CustomCloseCursor(aggHolder.map(_.toRowReader))(aggHolder.close())
        IteratorBackedRangeVector(rvk, rowIterator)
      }
    }
    Observable.fromTask(task).flatMap(rvs => Observable.fromIterable(rvs))
  }

  /**
    * This method is the facade for the present step of the aggregation
    */
  def present(aggregator: RowAggregator,
              source: Observable[RangeVector],
              limit: Int): Observable[RangeVector] = {
    source.flatMap(rv => Observable.fromIterable(aggregator.present(rv, limit)))
  }

  private def mapReduceInternal(rvs: List[RangeVector],
                                rowAgg: RowAggregator,
                                skipMapPhase: Boolean,
                                grouping: RangeVector => RangeVectorKey):
                                Map[RangeVectorKey, CloseableIterator[rowAgg.AggHolderType]] = {
    logger.trace(s"mapReduceInternal on ${rvs.size} RangeVectors...")
    var acc = rowAgg.zero
    val mapInto = rowAgg.newRowToMapInto
    rvs.groupBy(grouping).mapValues { rvs =>
      new CloseableIterator[rowAgg.AggHolderType] {
        val itsAndKeys = rvs.map { rv => (rv.rows, rv.key) }
        def hasNext: Boolean = {
          // Dont use forAll since it short-circuits hasNext invocation
          // It is important to invoke hasNext on all iterators to release shared locks
          var hnRet = false
          itsAndKeys.foreach { itKey =>
            if (itKey._1.hasNext) hnRet = true
          }
          hnRet
        }
        def next(): rowAgg.AggHolderType = {
          acc.resetToZero()
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
  def fastReduce(rowAgg: RowAggregator,
                 skipMapPhase: Boolean,
                 source: Observable[RangeVector],
                 outputLen: Int): Observable[RangeVector] = {
    // Can't use an Array here because rowAgg.AggHolderType does not have a ClassTag
    val accs = collection.mutable.ArrayBuffer.fill(outputLen)(rowAgg.zero)
    var count = 0
    // keeps track of all iters to close
    val toClose = ArrayBuffer.empty[RangeVectorCursor]

    // FoldLeft means we create the source PeriodicMapper etc and process immediately.  We can release locks right away
    // NOTE: ChunkedWindowIterator automatically releases locks after last window.  So it should all just work.  :)
    val aggObs = if (skipMapPhase) {
      source.foldLeftF(accs) { case (_, rv) =>
        count += 1
        val rowIter = rv.rows
        toClose += rowIter
        cforRange { 0 until outputLen } { i =>
          accs(i) = rowAgg.reduceAggregate(accs(i), rowIter.next)
        }
        accs
      }
    } else {
      val mapIntos = Array.fill(outputLen)(rowAgg.newRowToMapInto)
      source.foldLeftF(accs) { case (_, rv) =>
        count += 1
        val rowIter = rv.rows
        toClose += rowIter
        cforRange { 0 until outputLen } { i =>
          val mapped = rowAgg.map(rv.key, rowIter.next, mapIntos(i))
          accs(i) = rowAgg.reduceMappedRow(accs(i), mapped)
        }
        accs
      }
    }

    aggObs.flatMap { _ =>
      if (count > 0) {
        val iter = new CustomCloseCursor(accs.toIterator.map(_.toRowReader))(toClose.foreach(_.close()))
        Observable.now(IteratorBackedRangeVector(CustomRangeVectorKey.empty, iter))
      } else {
        Observable.empty
      }
    }
  }
}
