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

  protected def compose(childResponses: Observable[(QueryResult, Int)],
                        firstSchema: Task[ResultSchema],
                        querySession: QuerySession): Observable[RangeVector] = {
    childResponses.flatMap(res => Observable.fromIterable(res._1.result))
  }
}

/**
  * Use when child ExecPlan's span single local partition
  */
final case class LocalPartitionDistConcatExec(queryContext: QueryContext,
                                              dispatcher: PlanDispatcher,
                                              children: Seq[ExecPlan]) extends DistConcatExec

/**
  * Wrapper/Nonleaf execplan to split long range PeriodicPlan to multiple smaller execs.
  * It executes child plans sequentially and merges results using StitchRvsMapper
  */
final case class SplitLocalPartitionDistConcatExec(queryContext: QueryContext,
                                     dispatcher: PlanDispatcher,
                                     children: Seq[ExecPlan], outputRvRange: Option[RvRange],
                                    override val parallelChildTasks: Boolean = false) extends DistConcatExec {
  addRangeVectorTransformer(StitchRvsMapper(outputRvRange))
}

/**
  * Use when child ExecPlan's span multiple partitions
  */
final case class MultiPartitionDistConcatExec(queryContext: QueryContext,
                                              dispatcher: PlanDispatcher,
                                              children: Seq[ExecPlan]) extends DistConcatExec