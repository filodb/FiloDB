package filodb.query.exec;

import filodb.core.DatasetRef
import filodb.core.metadata.Dataset
import filodb.core.query._
import filodb.core.store.ChunkSource
import filodb.query._
import monix.execution.Scheduler
import monix.reactive.Observable

import scala.concurrent.duration.FiniteDuration

final case class RemoteExecParams(logicalPlan: LogicalPlan,
                                  queryOptions: QueryOptions)

final case class RemoteExec(id: String,
                            dispatcher: PlanDispatcher, serializedLogicalPlan: LogicalPlan,
                            dataset: DatasetRef, params: RemoteExecParams, submitTime: Long = System.currentTimeMillis()) extends LeafExecPlan {
  protected def args: String = ""

  /**
    * Limit on number of samples returned by this ExecPlan
    */
  override def limit: Int = ???

  // override def dataset: DatasetRef = ???

  /**
    * Sub classes should override this method to provide a concrete
    * implementation of the operation represented by this exec plan
    * node
    */
  override protected def doExecute(source: ChunkSource, dataset: Dataset, queryConfig: QueryConfig)
                                  (implicit sched: Scheduler, timeout: FiniteDuration): Observable[RangeVector] = ???

  /**
    * Sub classes should implement this with schema of RangeVectors returned
    * from doExecute() abstract method.
    */
  override protected def schemaOfDoExecute(dataset: Dataset): ResultSchema = ???
}
