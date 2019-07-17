package filodb.query.exec;
import monix.reactive.Observable

import filodb.core.metadata.Dataset
import filodb.core.query._
import filodb.query._
import filodb.query.Query.qLogger

final case class RemoteExec (id: String,
        dispatcher: PlanDispatcher,
        children: Seq[ExecPlan], serializedLogicalPlan: LogicalPlan) extends NonLeafExecPlan {
    require(children.nonEmpty)

    protected def args: String = ""

    protected def schemaOfCompose(dataset: Dataset): ResultSchema = ResultSchema(Nil, 0, Map.empty) //dummy resultSchema

    protected def compose(dataset: Dataset,
                          childResponses: Observable[(QueryResponse, Int)],
    queryConfig: QueryConfig): Observable[RangeVector] = {
        qLogger.debug(s"RemoteExec: Concatenating results")
        childResponses.flatMap {
            case (QueryResult(_, _, result), _) => Observable.fromIterable(result)
            case (QueryError(_, ex), _)         => throw ex
        }
    }
}
