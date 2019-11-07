package filodb.query.exec

import monix.eval.Task
import monix.reactive.Observable

import filodb.core.query._
import filodb.query._
import filodb.query.Query.qLogger

/**
  * Simply concatenate results from child ExecPlan objects
  */
final case class DistConcatExec(id: String,
                                dispatcher: PlanDispatcher,
                                children: Seq[ExecPlan]) extends NonLeafExecPlan {
  require(children.nonEmpty)

  protected def args: String = ""

  protected def compose(childResponses: Observable[(QueryResponse, Int)],
                        firstSchema: Task[ResultSchema],
                        queryConfig: QueryConfig): Observable[RangeVector] = {
    qLogger.debug(s"DistConcatExec: Concatenating results")
    childResponses.flatMap {
      case (QueryResult(_, _, result), _) => Observable.fromIterable(result)
      case (QueryError(_, ex), _)         => throw ex
    }
  }
}
