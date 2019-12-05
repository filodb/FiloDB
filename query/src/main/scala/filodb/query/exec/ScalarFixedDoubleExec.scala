package filodb.query.exec

import scala.concurrent.duration.FiniteDuration

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.DatasetRef
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.core.store.ChunkSource
import filodb.query.{QueryConfig, QueryResponse, QueryResult}
import filodb.query.Query.qLogger


/**
  * Exec Plans for fixed scalars which can execute locally without being dispatched
  */
case class ScalarFixedDoubleExec(id: String,
                                 dataset: DatasetRef,
                                 params: RangeParams,
                                 value: Double,
                                 limit: Int,
                                 submitTime: Long = System.currentTimeMillis()) extends LeafExecPlan {

  val columns: Seq[ColumnInfo] = Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
    ColumnInfo("value", ColumnType.DoubleColumn))

  /**
    * Sub classes should override this method to provide a concrete
    * implementation of the operation represented by this exec plan
    * node
    */
  override def doExecute(source: ChunkSource, queryConfig: QueryConfig)
                        (implicit sched: Scheduler, timeout: FiniteDuration): ExecResult = {
    throw new IllegalStateException("doExecute should not be called for ScalarFixedDoubleExec since it represents a " +
      "readily available static value")
  }

  /**
    * Args to use for the ExecPlan for printTree purposes only.
    * DO NOT change to a val. Increases heap usage.
    */
  override protected def args: String = s"params = $params, value = $value"


  override def execute(source: ChunkSource,
                       queryConfig: QueryConfig)
                      (implicit sched: Scheduler,
                       timeout: FiniteDuration): Task[QueryResponse] = {
    val recSchema = SerializedRangeVector.toSchema(columns)
    val resultSchema = ResultSchema(columns, 1)
    val rangeVectors : Seq[RangeVector] = Seq(ScalarFixedDouble(params, value))

    Task {
      rangeVectorTransformers.foldLeft((Observable.fromIterable(rangeVectors), resultSchema)) { (acc, transf) =>
        qLogger.debug(s"queryId: ${id} Setting up Transformer ${transf.getClass.getSimpleName} with ${transf.args}")
        val paramRangeVector: Seq[Observable[ScalarRangeVector]] = transf.funcParams.map(_.getResult)
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
