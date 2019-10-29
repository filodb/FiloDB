package filodb.query.exec

import filodb.core.DatasetRef
import filodb.core.metadata.Column.ColumnType
import filodb.core.query.{ColumnInfo, RangeParams, RangeVector, ResultSchema, ScalarFixedDouble, ScalarVector, SerializedRangeVector}
import filodb.core.store.ChunkSource
import filodb.query.Query.qLogger
import filodb.query.{QueryConfig, QueryResponse, QueryResult}
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import scala.concurrent.duration.FiniteDuration
case class ScalarFixedDoubleExec(id: String,
                               dataset: DatasetRef, params: RangeParams,
                                 value: Double,
                               limit: Int,
                               submitTime: Long = System.currentTimeMillis()) extends LeafExecPlan {


  /**
    * Sub classes should override this method to provide a concrete
    * implementation of the operation represented by this exec plan
    * node
    */
  override protected def doExecute(source: ChunkSource, queryConfig: QueryConfig)(implicit sched: Scheduler, timeout: FiniteDuration): Observable[RangeVector] = ???

  /**
    * Sub classes should implement this with schema of RangeVectors returned
    * from doExecute() abstract method.
    */
  override protected def schemaOfDoExecute(): ResultSchema = ???

  /**
    * Args to use for the ExecPlan for printTree purposes only.
    * DO NOT change to a val. Increases heap usage.
    */
  override protected def args: String =  s"params=$params, value=$value"


  override def execute(source: ChunkSource,
                       queryConfig: QueryConfig)
                      (implicit sched: Scheduler,
                       timeout: FiniteDuration): Task[QueryResponse] = {
    val columns: Seq[ColumnInfo] = Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
      ColumnInfo("value", ColumnType.DoubleColumn))
    val recSchema = SerializedRangeVector.toSchema(columns)
    val resultSchema = ResultSchema(columns, 1)
    val rangeVectors : Seq[RangeVector] = Seq(ScalarFixedDouble(params, value))

    Task {
      rangeVectorTransformers.foldLeft((Observable.fromIterable(rangeVectors), resultSchema)) { (acc, transf) =>
        qLogger.debug(s"queryId: ${id} Setting up Transformer ${transf.getClass.getSimpleName} with ${transf.args}")
        val paramRangeVector: Observable[ScalarVector] = if (transf.funcParams.isEmpty) {
          Observable.empty
        } else {
          transf.funcParams.head.getResult
        }

        (transf.apply(acc._1, queryConfig, limit, acc._2, paramRangeVector), transf.schema(acc._2))
      }._1.toListL.map(QueryResult(id, resultSchema, _))
    }.flatten

  }

  /**
    * The dispatcher is used to dispatch the ExecPlan
    * to the node where it will be executed. The Query Engine
    * will supply this parameter
    */
  override def dispatcher: PlanDispatcher = InProcessPlanDispatcher()

}
