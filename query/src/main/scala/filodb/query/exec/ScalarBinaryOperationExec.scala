package filodb.query.exec

import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.DatasetRef
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.core.store.ChunkSource
import filodb.query.{BinaryOperator, QueryConfig, QueryResponse, QueryResult}
import filodb.query.exec.binaryOp.BinaryOperatorFunction

/**
  * Exec Plans for scalar binary operations which can execute locally without being dispatched
  * Query example: 1 + 2, 1 < bool(2)
  */
case class ScalarBinaryOperationExec(queryContext: QueryContext,
                                     dataset: DatasetRef,
                                     params: RangeParams,
                                     lhs: Double,
                                     rhs: Double,
                                     operator: BinaryOperator) extends LeafExecPlan {

  val columns: Seq[ColumnInfo] = Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn))

  /**
    * Sub classes should override this method to provide a concrete
    * implementation of the operation represented by this exec plan
    * node
    */
  override def doExecute(source: ChunkSource, queryConfig: QueryConfig)
                        (implicit sched: Scheduler): ExecResult = {
    throw new IllegalStateException("doExecute should not be called for ScalarBinaryOperationExec since it represents" +
      "a readily available static value")
  }

  /**
    * Args to use for the ExecPlan for printTree purposes only.
    * DO NOT change to a val. Increases heap usage.
    */
  override protected def args: String = s"params = $params, lhs=$lhs, rhs=$rhs, operator=$operator"

  override def execute(source: ChunkSource,
                       queryConfig: QueryConfig)
                      (implicit sched: Scheduler): Task[QueryResponse] = {
    val execPlan2Span = Kamon.spanBuilder(s"execute-${getClass.getSimpleName}")
      .asChildOf(Kamon.currentSpan())
      .tag("query-id", queryContext.queryId)
      .start()
    val resultSchema = ResultSchema(columns, 1)
    val operatorFunction = BinaryOperatorFunction.factoryMethod(operator)

    val rangeVectors : Seq[RangeVector] = Seq(ScalarFixedDouble(params, operatorFunction.calculate(lhs, rhs)))
    // Please note that the following needs to be wrapped inside `runWithSpan` so that the context will be propagated
    // across threads. Note that task/observable will not run on the thread where span is present since
    // kamon uses thread-locals.
    Kamon.runWithSpan(execPlan2Span, true) {
      Task {
        val span = Kamon.spanBuilder(s"transform-${getClass.getSimpleName}")
          .asChildOf(execPlan2Span)
          .tag("query-id", queryContext.queryId)
          .start()
        rangeVectorTransformers.foldLeft((Observable.fromIterable(rangeVectors), resultSchema)) { (acc, transf) =>
          span.mark(transf.getClass.getSimpleName)
          val paramRangeVector: Seq[Observable[ScalarRangeVector]] = transf.funcParams.map(_.getResult)
          (transf.apply(acc._1, queryConfig, queryContext.sampleLimit, acc._2, paramRangeVector), transf.schema(acc._2))
        }._1.toListL.map({
          span.finish()
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
