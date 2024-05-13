package filodb.query.exec

import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.DatasetRef
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.core.store.ChunkSource
import filodb.query.{BinaryOperator, QueryResponse, QueryResult}
import filodb.query.exec.binaryOp.BinaryOperatorFunction




/**
  * Exec Plans for scalar binary operations which can execute locally without being dispatched
  * Query example: 1 + 2, 1 < bool(2), (1 + 2) < bool 3 + 4
  */
case class ScalarBinaryOperationExec(queryContext: QueryContext,
                                     dataset: DatasetRef,
                                     params: RangeParams,
                                     lhs: Either[Double, ScalarBinaryOperationExec],
                                     rhs: Either[Double, ScalarBinaryOperationExec],
                                     operator: BinaryOperator,
                                     dispatcher: InProcessPlanDispatcher) extends LeafExecPlan {

  val columns: Seq[ColumnInfo] = Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn))
  val resultSchema = ResultSchema(columns, 1)
  @transient lazy val operatorFunction = BinaryOperatorFunction.factoryMethod(operator)

  def evaluate: Double = {
    if (lhs.isRight  && rhs.isRight) operatorFunction.calculate(lhs.right.get.evaluate, rhs.right.get.evaluate)
    else if (lhs.isLeft && rhs.isLeft) operatorFunction.calculate(lhs.left.get, rhs.left.get)
    else if (lhs.isRight) operatorFunction.calculate(lhs.right.get.evaluate, rhs.left.get)
    else operatorFunction.calculate(lhs.left.get, rhs.right.get.evaluate)
  }

  /**
    * Args to use for the ExecPlan for printTree purposes only.
    * DO NOT change to a val. Increases heap usage.
    */
  override protected def args: String = s"params = $params, operator = $operator, lhs = $lhs, rhs = $rhs"

  override def toString: String = s"params = $params, operator=$operator, lhs=${lhs.toString}, rhs=${rhs.toString}"

  /**
    * Sub classes should override this method to provide a concrete
    * implementation of the operation represented by this exec plan
    * node
    */
  override def doExecute(source: ChunkSource,
                         querySession: QuerySession)
                        (implicit sched: Scheduler): ExecResult = {
    throw new IllegalStateException("doExecute should not be called for ScalarBinaryOperationExec since it represents" +
      "a static value")
  }

  override def execute(source: ChunkSource,
                       querySession: QuerySession)
                      (implicit sched: Scheduler): Task[QueryResponse] = {
    val rangeVectors : Seq[RangeVector] = Seq(ScalarFixedDouble(params, evaluate))
    Kamon.runWithSpan(Kamon.currentSpan(), false) {
      Task {
        rangeVectorTransformers.foldLeft((Observable.fromIterable(rangeVectors), resultSchema)) {
          case ((rvs, schema), rvTransformer) =>
          val paramRangeVector: Seq[Observable[ScalarRangeVector]] =
            rvTransformer.funcParams.map(_.getResult(querySession, source))
          (rvTransformer.apply(rvs, querySession, queryContext.plannerParams.enforcedLimits.execPlanSamples, schema,
            paramRangeVector), rvTransformer.schema(schema))
        }._1.toListL.map({
          QueryResult(queryContext.queryId, resultSchema, _,
            QueryStats(), QueryWarnings(), querySession.resultCouldBePartial,
            querySession.partialResultsReason
          )
        })
      }.flatten
    }
  }
}
