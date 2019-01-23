package filodb.query.exec

import monix.reactive.Observable

import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.Dataset
import filodb.core.query._
import filodb.query._
import filodb.query.Query.qLogger

sealed trait RowKeyRange

case class RowKeyInterval(from: BinaryRecord, to: BinaryRecord) extends RowKeyRange
case object AllChunks extends RowKeyRange
case object WriteBuffers extends RowKeyRange
case object InMemoryChunks extends RowKeyRange
case object EncodedChunks extends RowKeyRange

/**
  * Simply concatenate results from child ExecPlan objects
  */
final case class DistConcatExec(id: String,
                                dispatcher: PlanDispatcher,
                                children: Seq[ExecPlan]) extends NonLeafExecPlan {
  require(!children.isEmpty)

  protected def args: String = ""

  protected def schemaOfCompose(dataset: Dataset): ResultSchema = children.head.schema(dataset)

  protected def compose(childResponses: Observable[QueryResponse],
                        queryConfig: QueryConfig): Observable[RangeVector] = {
    qLogger.debug(s"DistConcatExec: Concatenating results")
    childResponses.flatMap {
      case qr: QueryResult => Observable.fromIterable(qr.result)
      case qe: QueryError => throw qe.t
    }
  }
}
