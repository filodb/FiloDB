package filodb.query.exec

import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.DatasetRef
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.core.store.ChunkSource
import filodb.query.{BadQueryException, QueryResponse, QueryResult, ScalarFunctionId}
import filodb.query.ScalarFunctionId.{DayOfMonth, DayOfWeek, DaysInMonth, Hour, Minute, Month, Time, Year}

/**
  * Exec Plans for time functions which can execute locally without being dispatched as they don't have any input
  * Query example: time(), hour()
  */
case class TimeScalarGeneratorExec(queryContext: QueryContext,
                                   dataset: DatasetRef, params: RangeParams,
                                   function: ScalarFunctionId) extends LeafExecPlan {

  val columns: Seq[ColumnInfo] = Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn))

  /**
    * Sub classes should override this method to provide a concrete
    * implementation of the operation represented by this exec plan
    * node
    */
  override def doExecute(source: ChunkSource,
                         querySession: QuerySession)
                        (implicit sched: Scheduler): ExecResult = {
    throw new IllegalStateException("doExecute should not be called for TimeScalarGeneratorExec since it represents" +
      "a readily available static value")
  }

  /**
    * Args to use for the ExecPlan for printTree purposes only.
    * DO NOT change to a val. Increases heap usage.
    */
  override protected def args: String = s"params = $params, function = $function"

  override def execute(source: ChunkSource,
                       querySession: QuerySession)
                      (implicit sched: Scheduler): Task[QueryResponse] = {
    val span = Kamon.currentSpan()
    val resultSchema = ResultSchema(columns, 1)
    val rangeVectors : Seq[RangeVector] = function match {
      case Time        => Seq(TimeScalar(params))
      case Hour        => Seq(HourScalar(params))
      case Minute      => Seq(MinuteScalar(params))
      case Month       => Seq(MonthScalar(params))
      case Year        => Seq(YearScalar(params))
      case DayOfMonth  => Seq(DayOfMonthScalar(params))
      case DayOfWeek   => Seq(DayOfWeekScalar(params))
      case DaysInMonth => Seq(DaysInMonthScalar(params))
      case _           => throw new BadQueryException("Invalid Function")
    }
    // Please note that the following needs to be wrapped inside `runWithSpan` so that the context will be propagated
    // across threads. Note that task/observable will not run on the thread where span is present since
    // kamon uses thread-locals.
    Kamon.runWithSpan(span, true) {
      Task {
        rangeVectorTransformers.foldLeft((Observable.fromIterable(rangeVectors), resultSchema)) { (acc, transf) =>
          val paramRangeVector: Seq[Observable[ScalarRangeVector]] = transf.funcParams.map(_.getResult)
          (transf.apply(acc._1, querySession, queryContext.plannerParams.sampleLimit, acc._2,
            paramRangeVector), transf.schema(acc._2))
        }._1.toListL.map({
          QueryResult(queryContext.queryId, resultSchema, _)
        })
      }.flatten
    }
  }

  /**
    * The dispatcher is used to dispatch the ExecPlan
    * to the node where it will be executed. The Query Engine
    * will supply this parameter
    */
  override final def dispatcher: PlanDispatcher = InProcessPlanDispatcher
}
