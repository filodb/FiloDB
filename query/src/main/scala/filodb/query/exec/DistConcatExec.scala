package filodb.query.exec

import filodb.core.memstore.SchemaMismatch
import monix.eval.Task
import monix.reactive.Observable
import filodb.core.query._
import filodb.query._

/**
  * Simply concatenate results from child ExecPlan objects
  */
final case class DistConcatExec(queryContext: QueryContext,
                                dispatcher: PlanDispatcher,
                                children: Seq[ExecPlan]) extends NonLeafExecPlan {
  require(children.nonEmpty)

  protected def args: String = ""

  protected def compose(childResponses: Observable[(QueryResponse, Int)],
                        firstSchema: Task[ResultSchema],
                        querySession: QuerySession): Observable[RangeVector] = {
    childResponses.flatMap {
      case (QueryResult(_, _, result), _) => Observable.fromIterable(result)
      case (QueryError(_, ex), _)         => throw ex
    }
  }

  override def reduceSchemas(rs: ResultSchema, resp: QueryResult): ResultSchema = {
    resp match {
      case QueryResult(_, schema, _) if rs == ResultSchema.empty =>
        schema     /// First schema, take as is
      case QueryResult(_, schema, _) =>
        if (!rs.hasSameColumnsAs(schema)) throw SchemaMismatch(rs.toString, schema.toString)
        val fixedVecLen = if (rs.fixedVectorLen.isEmpty && schema.fixedVectorLen.isEmpty) None
        else Some(rs.fixedVectorLen.getOrElse(0) + schema.fixedVectorLen.getOrElse(0))
        rs.copy(fixedVectorLen = fixedVecLen)
    }
  }

}
