package filodb.query.exec

import monix.reactive.Observable
import filodb.core.query._
import filodb.query.exec.rangefn.RangeFunction
import filodb.query.exec.rangefn.RangeFunction.RangeFunctionGenerator
import filodb.query.util.IndexedArrayQueue

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
    val valColType = RangeVectorTransformer.valueColumnType(sourceSchema)
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
        )
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

  private var ts = 0l
  private var curWindowStartMs = startMs - underlyingLookbackMs
  private var curWindowEndMs = startMs
  private var finished = false

  override def close(): Unit = rvc.close()

  override def hasNext: Boolean =  {
    if (finished) {
      false
    } else {
      rvc.hasNext
    }
  }

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



    // now we have all the samples added, we need to remove those that are
    // definitely not needed anymore
    //      var end = start
    //      while (rvc.hasNext) {
    //        val cur = rvc.next()
    //        val toAdd = windowSamplesPool.get
    //        toAdd.copyFrom(cur)
    //        end = toAdd.timestamp
    //        windowQueue.add(toAdd)
    //        rangeFunction.addedToWindow(toAdd, windowSamples)
    //      }

    // apply function on window samples
  }

  def cleanBuffer() : Unit = {
    while (rangeQueue.size > 0 && rangeQueue.head.timestamp < curWindowStartMs) {
      rangeQueue.remove()
    }
  }

  def addSamplesToBuffer() : Unit = {
    while(ts < curWindowEndMs && ts <=endMs && rvc.hasNext) {
      val cur = rvc.next()

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

