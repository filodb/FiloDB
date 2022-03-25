package filodb.query.exec

import monix.eval.Task
import monix.reactive.Observable

import filodb.core.query._
import filodb.query._

/**
  * Simply concatenate results from child ExecPlan objects
  */
trait DistConcatExec extends NonLeafExecPlan {
  require(children.nonEmpty)

  protected def args: String = ""

  protected def compose(childResponses: Observable[(QueryResponse, Int)],
                        firstSchema: Task[ResultSchema],
                        querySession: QuerySession): Observable[RangeVector] = {
    childResponses.flatMap {
      case (QueryResult(_, _, result, _, _, _), _) => Observable.fromIterable(result)
      case (QueryError(_, _, ex), _)         => throw ex
    }
  }
}

/**
  * Use when child ExecPlan's span single local partition
  */
final case class LocalPartitionDistConcatExec(queryContext: QueryContext,
                                              dispatcher: PlanDispatcher,
                                              children: Seq[ExecPlan]) extends DistConcatExec {
  override def withDispatcherHelper(planDispatcher: PlanDispatcher): ExecPlan = {
    copy(dispatcher = planDispatcher)
  }
  override def withChildrenHelper(children: Seq[ExecPlan]): NonLeafExecPlan = {
    copy(children = children)
  }
}

/**
  * Wrapper/Nonleaf execplan to split long range PeriodicPlan to multiple smaller execs.
  * It executes child plans sequentially and merges results using StitchRvsMapper
  */
final case class SplitLocalPartitionDistConcatExec(queryContext: QueryContext,
                                     dispatcher: PlanDispatcher,
                                     children: Seq[ExecPlan], outputRvRange: Option[RvRange],
                                    override val parallelChildTasks: Boolean = false) extends DistConcatExec {

  addRangeVectorTransformer(StitchRvsMapper(outputRvRange))

  // overriden since it can reduce schemas with different vector lengths as long as the columns are same
  override def reduceSchemas(rs: ResultSchema, resp: QueryResult): ResultSchema =
    IgnoreFixedVectorLenAndColumnNamesSchemaReducer.reduceSchema(rs, resp)

  override def withDispatcherHelper(planDispatcher: PlanDispatcher): ExecPlan = {
    copy(dispatcher = planDispatcher)
  }
  override def withChildrenHelper(children: Seq[ExecPlan]): NonLeafExecPlan = {
    copy(children = children)
  }
}

/**
  * Use when child ExecPlan's span multiple partitions
  */
final case class MultiPartitionDistConcatExec(queryContext: QueryContext,
                                              dispatcher: PlanDispatcher,
                                              children: Seq[ExecPlan]) extends DistConcatExec {
  override def withDispatcherHelper(planDispatcher: PlanDispatcher): ExecPlan = {
    copy(dispatcher = planDispatcher)
  }
  override def withChildrenHelper(children: Seq[ExecPlan]): NonLeafExecPlan = {
    copy(children = children)
  }
  // overriden since it can reduce schemas with different vector lengths as long as the columns are same
  override def reduceSchemas(rs: ResultSchema, resp: QueryResult): ResultSchema =
    IgnoreFixedVectorLenAndColumnNamesSchemaReducer.reduceSchema(rs, resp)
}