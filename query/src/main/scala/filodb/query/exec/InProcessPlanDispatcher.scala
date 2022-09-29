package filodb.query.exec

import java.net.InetAddress

import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import scala.concurrent.TimeoutException
import scala.concurrent.duration.DurationLong

import filodb.core.{DatasetRef, Types}
import filodb.core.memstore.PartLookupResult
import filodb.core.memstore.ratelimit.CardinalityRecord
import filodb.core.metadata.Schemas
import filodb.core.query.{QueryConfig, QuerySession, QueryStats, ResultSchema}
import filodb.core.store._
import filodb.query.{QueryResponse, QueryResult, StreamQueryResponse}
import filodb.query.Query.qLogger

/**
  * Executes an ExecPlan on the current thread.
  */
  case class InProcessPlanDispatcher(queryConfig: QueryConfig) extends PlanDispatcher {

  val clusterName = InetAddress.getLocalHost().getHostName()

  override def dispatch(plan: ExecPlanWithClientParams,
                        source: ChunkSource)(implicit sched: Scheduler): Task[QueryResponse] = {
    lazy val emptyPartialResult = QueryResult(plan.execPlan.queryContext.queryId, ResultSchema.empty, Nil,
      QueryStats(), true, Some("Result may be partial since query on some shards timed out"))

    // Please note that the following needs to be wrapped inside `runWithSpan` so that the context will be propagated
    // across threads. Note that task/observable will not run on the thread where span is present since
    // kamon uses thread-locals.
    // Dont finish span since this code didnt create it
    Kamon.runWithSpan(Kamon.currentSpan(), false) {
      // translate implicit ExecutionContext to monix.Scheduler
      val querySession = QuerySession(plan.execPlan.queryContext, queryConfig,
                              streamingDispatch = PlanDispatcher.streamingResultsEnabled,
                              catchMultipleLockSetErrors = true)
      plan.execPlan.execute(source, querySession)
        .timeout(plan.clientParams.deadline.milliseconds)
        .guarantee(Task.eval(querySession.close()))
        .onErrorRecover {
        case e: TimeoutException if (plan.execPlan.queryContext.plannerParams.allowPartialResults)
        =>
          qLogger.warn(s"Swallowed TimeoutException for query id: ${plan.execPlan.queryContext.queryId} " +
            s"since partial result was enabled: ${e.getMessage}")
          emptyPartialResult
      }
    }
  }

  override def isLocalCall: Boolean = true

  override def dispatchStreaming(plan: ExecPlanWithClientParams,
                                 source: ChunkSource)
                                (implicit sched: Scheduler): Observable[StreamQueryResponse] = ???
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

  override def isDownsampleStore: Boolean = false

  override def scanTsCardinalities(ref: DatasetRef, shards: Seq[Int],
                                   shardKeyPrefix: Seq[String], depth: Int): scala.Seq[CardinalityRecord] =
    throw new UnsupportedOperationException("This operation is not supported")

  override def acquireSharedLock(ref: DatasetRef, shardNum: Int, querySession: QuerySession): Unit =
    throw new UnsupportedOperationException("This operation is not supported")

  override def checkReadyForQuery(ref: DatasetRef, shard: Int, querySession: QuerySession): Unit =
    throw new UnsupportedOperationException("This operation is not supported")
}

