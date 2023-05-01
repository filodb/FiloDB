package filodb.query.exec

import monix.eval.Task
import monix.execution.Scheduler

import filodb.core.DatasetRef
import filodb.core.query.{QueryContext, QuerySession, QueryStats, ResultSchema}
import filodb.core.store.ChunkSource
import filodb.query.{QueryResponse, QueryResult}

case class EmptyResultExec(queryContext: QueryContext,
                           dataset: DatasetRef,
                           dispatcher: InProcessPlanDispatcher) extends LeafExecPlan {

  override def execute(source: ChunkSource,
                       querySession: QuerySession)
                      (implicit sched: Scheduler): Task[QueryResponse] = {
    Task(QueryResult(queryContext.queryId, ResultSchema.timeDouble,
      Seq.empty, QueryStats(), false, None))
  }

  override def doExecute(source: ChunkSource,
                         querySession: QuerySession)
                        (implicit sched: Scheduler): ExecResult = ???

  override protected def args: String = ""
}