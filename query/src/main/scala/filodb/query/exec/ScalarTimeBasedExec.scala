package filodb.query.exec

import filodb.core.DatasetRef
import filodb.core.metadata.Column.ColumnType
import filodb.core.query.{ColumnInfo, HourScalar, RangeParams, RangeVector, ResultSchema, SerializedRangeVector, TimeScalar}
import filodb.core.store.ChunkSource
import filodb.query.ScalarFunctionId.{Hour, Time}
import filodb.query.{BadQueryException, QueryConfig, QueryResponse, QueryResult, ScalarFunctionId}
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import scala.concurrent.duration.FiniteDuration
case class ScalarTimeBasedExec(id: String,
                               dataset: DatasetRef, params: RangeParams,
                               function: ScalarFunctionId,
                               submitTime: Long = System.currentTimeMillis()) extends LeafExecPlan {

  /**
    * Limit on number of samples returned by this ExecPlan
    */
  override def limit: Int = ???


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
  override protected def args: String = params.toString


  override def execute(source: ChunkSource,
                       queryConfig: QueryConfig)
                      (implicit sched: Scheduler,
                       timeout: FiniteDuration): Task[QueryResponse] = {
    val columns: Seq[ColumnInfo] = Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
      ColumnInfo("value", ColumnType.DoubleColumn))
    val recSchema = SerializedRangeVector.toSchema(columns)
    val resultSchema = ResultSchema(columns, 1)
    val rangeVectors = function match {
      case Time => Seq(TimeScalar (params))
      case Hour => Seq(HourScalar(params))
      case _    => throw new BadQueryException("Invalid Function")
    }
    Task(QueryResult(id, resultSchema, rangeVectors))
  }

  /**
    * The dispatcher is used to dispatch the ExecPlan
    * to the node where it will be executed. The Query Engine
    * will supply this parameter
    */
  override def dispatcher: PlanDispatcher = null
}
