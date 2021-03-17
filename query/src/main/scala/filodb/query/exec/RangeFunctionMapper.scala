package filodb.query.exec

import monix.reactive.Observable

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.query.exec.InternalRangeFunction.AvgWithSumAndCountOverTime
import filodb.query.exec.rangefn._
import filodb.query.util.IndexedArrayQueue

/**
  * Simple transformer that returns RangeVector's of size one
  * which are the results of the time range aggregation for the entire input
  */
final case class RangeFunctionMapper(
  functionId: InternalRangeFunction,
  queryContext: QueryContext,
  funcParams: Seq[FuncArgs] = Nil
) extends RangeVectorTransformer {

  val isLastFn =
    (functionId == InternalRangeFunction.LastSampleHistMax) ||
    (functionId == InternalRangeFunction.Timestamp)

  protected[exec] def args: String = s"functionId=$functionId"

  def apply(
    source: Observable[RangeVector],
    querySession: QuerySession,
    limit: Int,
    sourceSchema: ResultSchema,
    paramResponse: Seq[Observable[ScalarRangeVector]]
  ): Observable[RangeVector] = {
    val valColType = RangeVectorTransformer.valueColumnType(sourceSchema)
    val rangeFuncGen = RangeFunction.generatorFor(
      sourceSchema,
      Some(functionId),
      valColType,
      querySession.queryConfig,
      funcParams,
      false
    )
    val sampleRangeFunc = rangeFuncGen()
    val rvs = sampleRangeFunc match {
      // Iterator-based: Wrap long columns to yield a double value
      case f: RangeFunction if valColType == ColumnType.LongColumn =>
        source.map { rv =>
          IteratorBackedRangeVector(rv.key,
            new RangeFunctionIterator(
              new LongToDoubleIterator(rv.rows), rangeFuncGen().asSliding, querySession.queryConfig
            )
          )
        }
      // Otherwise just feed in the double column
      case f: RangeFunction =>
        source.map { rv =>
          IteratorBackedRangeVector(rv.key,
            new RangeFunctionIterator(rv.rows, rangeFuncGen().asSliding, querySession.queryConfig)
          )
        }
    }
    rvs
  }


  // Transform source double or long to double schema
  override def schema(source: ResultSchema): ResultSchema = {
    // Special treatment for downsampled gauge schema.
    // Source schema will be the selected columns. Result of this mapper should be regular timestamp/value
    // since the avg will be calculated using sum and count
    // FIXME dont like that this is hardcoded; but the check is needed.
    if (
      functionId == AvgWithSumAndCountOverTime &&
      source.columns.map(_.name) == Seq("timestamp", "sum", "count")
    ) {
      source.copy(columns = Seq(
        ColumnInfo("timestamp", ColumnType.TimestampColumn),
        ColumnInfo("value", ColumnType.DoubleColumn)
      ))
    } else {
      source.copy(
        columns = source.columns.zipWithIndex.map {
          // Transform if its not a row key column
          case (ColumnInfo(name, ColumnType.LongColumn), i) if i >= source.numRowKeyColumns =>
            ColumnInfo("value", ColumnType.DoubleColumn)
          case (ColumnInfo(name, ColumnType.IntColumn), i) if i >= source.numRowKeyColumns =>
            ColumnInfo(name, ColumnType.DoubleColumn)
          // For double columns, just rename the output so that it's the same no matter source schema.
          // After all after applying a range function, source column doesn't matter anymore
          // NOTE: we only rename if i is 1 or second column.  If its 2 it might be max which cannot be renamed
          case (ColumnInfo(name, ColumnType.DoubleColumn), i) if i == 1 =>
           ColumnInfo("value", ColumnType.DoubleColumn)
          case (c: ColumnInfo, _) => c
        },
        fixedVectorLen = Some(1)
      )
    }
  }
}

/**
  * Decorates a raw series iterator to apply a range vector function
  * on periodic time windows
  */
class RangeFunctionIterator(
  rvc: RangeVectorCursor,
  rangeFunction: RangeFunction,
  queryConfig: QueryConfig
) extends RangeVectorCursor {
  private val sampleToEmit = new TransientRow()
  // samples queue
  private val windowQueue = new IndexedArrayQueue[TransientRow]()
  // this is the object that will be exposed to the RangeFunction
  private val windowSamples = new QueueBasedWindow(windowQueue)
  // to avoid creation of object per sample, we use a pool
  val windowSamplesPool = new TransientRowPool()

  override def close(): Unit = rvc.close()

  override def hasNext: Boolean = rvc.hasNext

  override def next(): TransientRow = {
    val cur = rvc.next()
    val toAdd = windowSamplesPool.get
    toAdd.copyFrom(cur)
    val start = toAdd.timestamp
    windowQueue.add(toAdd)
    var end = start
    while (rvc.hasNext) {
      val cur = rvc.next()
      val toAdd = windowSamplesPool.get
      toAdd.copyFrom(cur)
      end = toAdd.timestamp
      windowQueue.add(toAdd)
      rangeFunction.addedToWindow(toAdd, windowSamples)
    }

    // apply function on window samples
    rangeFunction.apply(start, end, windowSamples, sampleToEmit, queryConfig)
    sampleToEmit
  }
}


