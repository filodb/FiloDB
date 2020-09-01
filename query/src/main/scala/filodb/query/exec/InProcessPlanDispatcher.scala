package filodb.query.exec

import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.{DatasetRef, Types}
import filodb.core.memstore.PartLookupResult
import filodb.core.metadata.Schemas
import filodb.core.query.{EmptyQueryConfig, QueryConfig, QuerySession}
import filodb.core.store._
import filodb.query.QueryResponse

/**
  * Dispatcher which will make a No-Op style call to ExecPlan#excecute().
  * Goal is that Non-Leaf plans can be executed locally in JVM and make network
  * calls only for children.
  */
case object InProcessPlanDispatcher extends PlanDispatcher {

  // Empty query config, since its does not apply in case of non-leaf plans
  val queryConfig: QueryConfig = EmptyQueryConfig

  override def dispatch(plan: ExecPlan)(implicit sched: Scheduler): Task[QueryResponse] = {
    // unsupported source since its does not apply in case of non-leaf plans
    val source = UnsupportedChunkSource()

    // Please note that the following needs to be wrapped inside `runWithSpan` so that the context will be propagated
    // across threads. Note that task/observable will not run on the thread where span is present since
    // kamon uses thread-locals.
    Kamon.runWithSpan(Kamon.currentSpan(), false) {
      // translate implicit ExecutionContext to monix.Scheduler
      val querySession = QuerySession(plan.queryContext, queryConfig)
      plan.execute(source, querySession)
    }
  }

}

/**
  * No-op chunk source which does nothing and throws exception for all functions.
  */
case class UnsupportedChunkSource() extends ChunkSource {
  def scanPartitions(ref: DatasetRef,
                     iter: PartLookupResult,
                     colIds: Seq[Types.ColumnId],
                     querySession: QuerySession): Observable[ReadablePartition] =
    throw new UnsupportedOperationException("This operation is not supported")

  def lookupPartitions(ref: DatasetRef,
                       partMethod: PartitionScanMethod,
                       chunkMethod: ChunkScanMethod,
                       querySession: QuerySession): PartLookupResult =
    throw new UnsupportedOperationException("This operation is not supported")

  override def groupsInDataset(dataset: DatasetRef): Int =
    throw new UnsupportedOperationException("This operation is not supported")

  def schemas(ref: DatasetRef): Option[Schemas] = None

  override def stats: ChunkSourceStats =
    throw new UnsupportedOperationException("This operation is not supported")

  override def getScanSplits(dataset: DatasetRef, splitsPerNode: Int): Seq[ScanSplit] =
    throw new UnsupportedOperationException("This operation is not supported")

  override def readRawPartitions(ref: DatasetRef, maxChunkTime: Long,
                                 partMethod: PartitionScanMethod,
                                 chunkMethod: ChunkScanMethod): Observable[RawPartData] =
    throw new UnsupportedOperationException("This operation is not supported")

  /**
    * True if this store is in the mode of serving downsampled data.
    * This is used to switch ingestion and query behaviors for downsample cluster.
    */
  override def isDownsampleStore: Boolean = false
}

