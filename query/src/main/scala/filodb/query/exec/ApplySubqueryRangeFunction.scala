package filodb.query.exec

import monix.reactive.Observable

import filodb.core.query._
import filodb.query.exec.rangefn.RangeFunction
import filodb.query.exec.rangefn.RangeFunction.RangeFunctionGenerator
import filodb.query.util.IndexedArrayQueue

// This is an experimental feature of FiloDB not to be used
// in production settings. The class can be used to produce
// subquery results equivalent to range_query API. Currently,
// Prometheus implementation of subqueries and range_query is
// different, ie equivalent queries like range query for 5 minutes
// with step 1 minute might not produce same results as subquery
// with 5 minute lookback and 1 minute step. This class provides
// subquery implementation that is equivalent to range query API.
// sum_over_time(foo[6:3])[4:2], where now=18
final case class ApplySubqueryRangeFunction(
  subqueryExpRangeParams: RangeParams, // start = now-4, end=now, step = 2
  underlyingLookbackMs: Long, // 6
  underlyingStepMs: Long, // 3
  subqueryRangeFunction: InternalRangeFunction
) extends RangeVectorTransformer {
  override def funcParams: Seq[FuncArgs] = Nil

  override protected[exec] def args: String =
    s"subqueryEprRangeParams=$subqueryExpRangeParams," +
    s"subqueryRangeFunction=$subqueryRangeFunction," +
    s"underlyingLookbackMs=$underlyingLookbackMs,underlyingStepMs=$underlyingStepMs"

  def apply(
    source: Observable[RangeVector],
    querySession: QuerySession,
    limit: Int,
    sourceSchema: ResultSchema,
    paramsResponse: Seq[Observable[ScalarRangeVector]]
  ): Observable[RangeVector] = {

    //val underlyingRangeStepMs = underlyingPeriodicSeriesRange.stepSecs * 1000

    // Need to skip these many samples for window operations
    //val skip = (subqueryStepMs / underlyingRangeStepMs).toInt
    val valColType = ResultSchema.valueColumnType(sourceSchema)
    val rangeFuncGen = RangeFunction.generatorFor(
      sourceSchema,
      Some(subqueryRangeFunction),
      valColType,
      querySession.queryConfig, funcParams, false
    )
    source.map { rv =>
      IteratorBackedRangeVector(
        rv.key,
        new SubquerySlidingWindowIterator(
          subqueryExpRangeParams.startSecs * 1000,
          subqueryExpRangeParams.endSecs * 1000,
          subqueryExpRangeParams.stepSecs * 1000,
          underlyingLookbackMs,
          underlyingStepMs,
          rv.rows,
          rangeFuncGen,
          querySession.queryConfig
        ),
        None
      )
    }
  }

  override def schema(source: ResultSchema): ResultSchema = {
    // since range function is NOT optional, the resulting vectors length will be
    // of size 1, not the length of the underlying PeriodicSeries
    source.copy(fixedVectorLen = Some(1))
  }

}

// sum_over_time(foo[5m:1m])[3m:90s]
class SubquerySlidingWindowIterator(
   startMs: Long, // now - 3m
   endMs : Long, // now
   stepMs: Long, // 90s
   underlyingLookbackMs: Long, //5m
   underlyingStepMs: Long, //1m
   rvc: RangeVectorCursor,
   rfGenerator: RangeFunctionGenerator, //TODO require reset for all range functions
   //rangeFunction: RangeFunction,
   queryConfig: QueryConfig
 ) extends RangeVectorCursor {
  private val sampleToEmit = new TransientRow()

  // samples queue that will have ALL of the samples in the current range
  // whether they are applicable for the current window or not
  private val rangeQueue = new IndexedArrayQueue[TransientRow]()

  // to avoid creation of object per sample, we use a pool
  val windowSamplesPool = new TransientRowPool()

  private var ts = 0L
  private var curWindowStartMs = startMs - underlyingLookbackMs
  private var curWindowEndMs = startMs
  private var finished = false

  override def close(): Unit = rvc.close()

  override def hasNext: Boolean = {
    if (finished) {
      false
    } else {
      rvc.hasNext
    }
  }

  // The general idea is to keep a buffer of samples that are
  // in the range of the next window to evaluate. The trick, however, is NOT to expose
  // the entire buffer to a range function to produce next value. Instead, we have
  // to recompute the entire window on every next() call.
  // The complexity would be N*B, where N is the total number of samples to emit (next calls)
  // and B is the number of samples needed to compute one sample to emit.
  // for example, you have a window of 5 minutes and your outer step is 30 seconds, but inner
  // step is 60 seconds. To compute one sample you need 6 points, however, your buffer will have
  // 11 points (5 points in between). If total number of outer steps is 20, and total number
  // 6 points need to be re-evaluated at every step, the complexity would be 20*6.
  // Space complexity would be approximately 2x of PeriodicSamplesMapper.
  override def next(): TransientRow = {
    cleanBuffer()
    addSamplesToBuffer()
    val rangeFunction : RangeFunction = rfGenerator().asSliding
    // samples queue
    val windowQueue = new IndexedArrayQueue[TransientRow]()
    // this is the object that will be exposed to the RangeFunction
    val windowSamples = new QueueBasedWindow(windowQueue)
    for (i <- 0 until rangeQueue.size) {
      val sample = rangeQueue.apply(i)
      // not ALL underlying periodic series samples need to be used for calculation
      // of the results of this particular window
      if (
        sample.timestamp <= curWindowEndMs &&
        (sample.timestamp - curWindowStartMs) % underlyingStepMs == 0
      ) {
        windowQueue.add(sample)
        rangeFunction.addedToWindow(sample, windowSamples)
      }
    }
    rangeFunction.apply(curWindowStartMs, curWindowEndMs, windowSamples, sampleToEmit, queryConfig)
    curWindowStartMs += stepMs
    curWindowEndMs += stepMs
    sampleToEmit
  }

  def cleanBuffer() : Unit = {
    while (rangeQueue.size > 0 && rangeQueue.head.timestamp < curWindowStartMs) {
      rangeQueue.remove()
    }
  }

  def addSamplesToBuffer() : Unit = {
    while(ts < curWindowEndMs && ts <=endMs && rvc.hasNext) {
      val cur = rvc.next()

      //TODO verify that windowSamplesPool won't be leaking memory
      //as the samples need to be pushed back
      val toAdd = windowSamplesPool.get
      toAdd.copyFrom(cur)
      ts = toAdd.timestamp
      if (ts >= curWindowStartMs) {
        rangeQueue.add(toAdd)
      }


      if (ts >= endMs) {
        finished = true
      }
    }
  }
}

