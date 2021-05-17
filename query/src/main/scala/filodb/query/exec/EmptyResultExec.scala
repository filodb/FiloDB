package filodb.query.exec

import monix.eval.Task
import monix.execution.Scheduler

import filodb.core.DatasetRef
import filodb.core.metadata.Column.ColumnType
import filodb.core.query.{ColumnInfo, EmptyQueryConfig, QueryContext, QuerySession, ResultSchema}
import filodb.core.store.ChunkSource
import filodb.query.{QueryResponse, QueryResult}

case class EmptyResultExec(queryContext: QueryContext,
                           dataset: DatasetRef) extends LeafExecPlan {
  override def dispatcher: PlanDispatcher = InProcessPlanDispatcher(EmptyQueryConfig)

  override def execute(source: ChunkSource,
                       querySession: QuerySession)
                      (implicit sched: Scheduler): Task[QueryResponse] = {
    Task(QueryResult(queryContext.queryId,
      new ResultSchema(Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
                           ColumnInfo("value", ColumnType.DoubleColumn)), 1),
      Seq.empty, false, None))
  }

  override def doExecute(source: ChunkSource,
                         querySession: QuerySession)
                        (implicit sched: Scheduler): ExecResult = ???

  override protected def args: String = ""
}