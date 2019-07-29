package filodb.query.exec

import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.DatasetRef
import filodb.core.Types.ColumnId
import filodb.core.metadata.Dataset
import filodb.core.store.{ChunkScanMethod, ChunkSource, ChunkSourceStats, PartitionScanMethod, RawPartData,
  ReadablePartition, ScanSplit}
import filodb.query.{QueryConfig, QueryResponse}

/**
  * Dispatcher which will make a No-Op style call to ExecPlan#excecute().
  * Goal is that Non-Leaf plans can be executed locally in JVM and make network
  * calls only for children.
  * @param dataset to be used by ExecPlan#execute
  */
case class InProcessPlanDispatcher(dataset: Dataset) extends PlanDispatcher {

  override def dispatch(plan: ExecPlan)(implicit sched: Scheduler,
                                        timeout: FiniteDuration): Task[QueryResponse] = {

    // Empty query config, since its does not apply in case of non-leaf plans
    val queryConfig: QueryConfig = new QueryConfig(ConfigFactory.empty())
    // unsupported source since its does not apply in case of non-leaf plans
    val source = UnSupportedChunkSource()
    // translate implicit ExecutionContext to monix.Scheduler
    plan.execute(source, dataset, queryConfig)
  }

}

/**
  * No-op chunk source which does nothing and throws exception for all functions.
  */
case class UnSupportedChunkSource() extends ChunkSource {

  override def scanPartitions(dataset: Dataset, columnIDs: Seq[ColumnId], partMethod: PartitionScanMethod,
                              chunkMethod: ChunkScanMethod): Observable[ReadablePartition] =
    throw new UnsupportedOperationException("This operation is not supported")

  override def groupsInDataset(dataset: Dataset): Int =
    throw new UnsupportedOperationException("This operation is not supported")

  override def stats: ChunkSourceStats =
    throw new UnsupportedOperationException("This operation is not supported")

  override def getScanSplits(dataset: DatasetRef, splitsPerNode: Int): Seq[ScanSplit] =
    throw new UnsupportedOperationException("This operation is not supported")

  override def readRawPartitions(dataset: Dataset, columnIDs: Seq[ColumnId], partMethod: PartitionScanMethod,
                                 chunkMethod: ChunkScanMethod): Observable[RawPartData] =
    throw new UnsupportedOperationException("This operation is not supported")
}

