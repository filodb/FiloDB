package filodb.query.exec

import monix.reactive.Observable
import scala.collection.mutable.ListBuffer

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.core.query.Filter.Equals
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.query.{BinaryOperator, InstantFunctionId, MiscellaneousFunctionId, QueryConfig, SortFunctionId}
import filodb.query.InstantFunctionId.HistogramQuantile
import filodb.query.MiscellaneousFunctionId.{LabelJoin, LabelReplace}
import filodb.query.SortFunctionId.{Sort, SortDesc}
import filodb.query.exec.binaryOp.BinaryOperatorFunction
import filodb.query.exec.rangefn._

/**
  * Implementations can provide ways to transform RangeVector
  * results generated by ExecPlan nodes.
  *
  * Reason why these are not ExecPlan nodes themselves is because
  * they can be applied on the same node where the base RangeVectors
  * are sourced.
  *
  * It can safely be assumed that the operations in these nodes are
  * compute intensive and not I/O intensive.
  */
trait RangeVectorTransformer extends java.io.Serializable {
  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema): Observable[RangeVector]

  /**
    * Default implementation retains source schema
    */
  def schema(source: ResultSchema): ResultSchema = source

  /**
    * Args to use for the RangeVectorTransformer for printTree purposes only.
    * DO NOT change to a val. Increases heap usage.
    */
  protected[exec] def args: String
}

object RangeVectorTransformer {
  def valueColumnType(schema: ResultSchema): ColumnType = {
    require(schema.isTimeSeries, s"Schema $schema is not time series based, cannot continue query")
    require(schema.columns.size >= 2, s"Schema $schema has less than 2 columns, cannot continue query")
    schema.columns(1).colType
  }
}

/**
  * Applies an instant vector function to every instant/row of the
  * range vectors
  */
final case class InstantVectorFunctionMapper(function: InstantFunctionId,
                                             funcParams: Seq[Any] = Nil) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"function=$function, funcParams=$funcParams"

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema): Observable[RangeVector] = {
    RangeVectorTransformer.valueColumnType(sourceSchema) match {
      case ColumnType.HistogramColumn =>
        val instantFunction = InstantFunction.histogram(function, funcParams)
        if (instantFunction.isHToDoubleFunc) {
          source.map { rv =>
            IteratorBackedRangeVector(rv.key, new H2DoubleInstantFuncIterator(rv.rows, instantFunction.asHToDouble))
          }
        } else if (instantFunction.isHistDoubleToDoubleFunc && sourceSchema.isHistDouble) {
          source.map { rv =>
            IteratorBackedRangeVector(rv.key, new HD2DoubleInstantFuncIterator(rv.rows, instantFunction.asHDToDouble))
          }
        } else {
          throw new UnsupportedOperationException(s"Sorry, function $function is not supported right now")
        }
      case ColumnType.DoubleColumn =>
        if (function == HistogramQuantile) {
          // Special mapper to pull all buckets together from different Prom-schema time series
          val mapper = HistogramQuantileMapper(funcParams)
          mapper.apply(source, queryConfig, limit, sourceSchema)
        } else {
          val instantFunction = InstantFunction.double(function, funcParams)
          source.map { rv =>
            IteratorBackedRangeVector(rv.key, new DoubleInstantFuncIterator(rv.rows, instantFunction))
          }
        }
      case cType: ColumnType =>
        throw new UnsupportedOperationException(s"Column type $cType is not supported for instant functions")
    }
  }

  override def schema(source: ResultSchema): ResultSchema = {
    // if source is histogram, determine what output column type is
    // otherwise pass along the source
    RangeVectorTransformer.valueColumnType(source) match {
      case ColumnType.HistogramColumn =>
        val instantFunction = InstantFunction.histogram(function, funcParams)
        if (instantFunction.isHToDoubleFunc || instantFunction.isHistDoubleToDoubleFunc) {
          // Hist to Double function, so output schema is double
          source.copy(columns = Seq(source.columns.head, ColumnInfo("value", ColumnType.DoubleColumn)))
        } else { source }
      case cType: ColumnType          =>
        source
    }
  }
}

private class DoubleInstantFuncIterator(rows: Iterator[RowReader],
                                        instantFunction: DoubleInstantFunction,
                                        result: TransientRow = new TransientRow()) extends Iterator[RowReader] {
  final def hasNext: Boolean = rows.hasNext
  final def next(): RowReader = {
    val next = rows.next()
    val newValue = instantFunction(next.getDouble(1))
    result.setValues(next.getLong(0), newValue)
    result
  }
}

private class H2DoubleInstantFuncIterator(rows: Iterator[RowReader],
                                          instantFunction: HistToDoubleIFunction,
                                          result: TransientRow = new TransientRow()) extends Iterator[RowReader] {
  final def hasNext: Boolean = rows.hasNext
  final def next(): RowReader = {
    val next = rows.next()
    val newValue = instantFunction(next.getHistogram(1))
    result.setValues(next.getLong(0), newValue)
    result
  }
}

private class HD2DoubleInstantFuncIterator(rows: Iterator[RowReader],
                                           instantFunction: HDToDoubleIFunction,
                                           result: TransientRow = new TransientRow()) extends Iterator[RowReader] {
  final def hasNext: Boolean = rows.hasNext
  final def next(): RowReader = {
    val next = rows.next()
    val newValue = instantFunction(next.getHistogram(1), next.getDouble(2))
    result.setValues(next.getLong(0), newValue)
    result
  }
}

/**
  * Applies a binary operation involving a scalar to every instant/row of the
  * range vectors
  */
final case class ScalarOperationMapper(operator: BinaryOperator,
                                       scalar: AnyVal,
                                       scalarOnLhs: Boolean) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"operator=$operator, scalar=$scalar"

  val operatorFunction = BinaryOperatorFunction.factoryMethod(operator)

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema): Observable[RangeVector] = {
    source.map { rv =>
      val resultIterator: Iterator[RowReader] = new Iterator[RowReader]() {

        private val rows = rv.rows
        private val result = new TransientRow()
        private val sclrVal = scalar.asInstanceOf[Double]

        override def hasNext: Boolean = rows.hasNext

        override def next(): RowReader = {
          val next = rows.next()
          val nextVal = next.getDouble(1)
          val newValue = if (scalarOnLhs) operatorFunction.calculate(sclrVal, nextVal)
                         else  operatorFunction.calculate(nextVal, sclrVal)
          result.setValues(next.getLong(0), newValue)
          result
        }
      }
      IteratorBackedRangeVector(rv.key, resultIterator)
    }
  }

  // TODO all operation defs go here and get invoked from mapRangeVector
}

final case class MiscellaneousFunctionMapper(function: MiscellaneousFunctionId,
                                             funcParams: Seq[Any] = Nil) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"function=$function, funcParams=$funcParams"

  val miscFunction: MiscellaneousFunction = {
    function match {
      case LabelReplace => LabelReplaceFunction(funcParams)
      case LabelJoin    => LabelJoinFunction(funcParams)
      case _            => throw new UnsupportedOperationException(s"$function not supported.")
    }
  }

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema): Observable[RangeVector] = {
    miscFunction.execute(source)
  }
}

final case class SortFunctionMapper(function: SortFunctionId) extends RangeVectorTransformer {
  protected[exec] def args: String =
    s"function=$function"

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema): Observable[RangeVector] = {
    if (sourceSchema.columns(1).colType == ColumnType.DoubleColumn) {

      val ordering: Ordering[Double] = function match {
        case Sort => (Ordering[Double])
        case SortDesc => (Ordering[Double]).reverse
        case _ => throw new UnsupportedOperationException(s"$function not supported.")
      }

      val resultRv = source.toListL.map { rvs =>
        rvs.map { rv =>
          new RangeVector {
            override def key: RangeVectorKey = rv.key

            override def rows: Iterator[RowReader] = new BufferableIterator(rv.rows).buffered
          }
        }.sortBy { rv => rv.rows.asInstanceOf[BufferedIterator[RowReader]].head.getDouble(1)
        }(ordering)

      }.map(Observable.fromIterable)

      Observable.fromTask(resultRv).flatten
    } else {
      source
    }
  }
}

final case class AbsentFunctionMapper(columnFilter: Seq[ColumnFilter], rangeParams: RangeParams, metricColumn: String)
  extends RangeVectorTransformer {

  protected[exec] def args: String =
    s"columnFilter=$columnFilter rangeParams=$rangeParams metricColumn=$metricColumn"

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema): Observable[RangeVector] = {

      val resultRv = source.toListL.map { rvs =>
       if (rvs.isEmpty) {
         Seq(new RangeVector {
            override def key: RangeVectorKey = {
              val labelsFromFilter = columnFilter.filter(_.filter.isInstanceOf[Equals]).
                  filterNot(_.column.equals(metricColumn)).map {
                  c => {
                    ZeroCopyUTF8String(c.column) -> ZeroCopyUTF8String(c.filter.valuesStrings.head.asInstanceOf[String])
                  }
                }.toMap
                CustomRangeVectorKey(labelsFromFilter)
              }

            override def rows: Iterator[RowReader] = {
              val rowList = new ListBuffer[TransientRow]()
              for (i <- rangeParams.start to rangeParams.end by rangeParams.step) {
                rowList += new TransientRow(i*1000, 1)
              }
              rowList.iterator
            }
          })
        }
        else {
         Seq.empty[RangeVector]
        }
      }.map(Observable.fromIterable)

      Observable.fromTask(resultRv).flatten
    }
  }

