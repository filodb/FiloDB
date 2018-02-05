package filodb.coordinator.queryengine

import scala.concurrent.ExecutionContext

import akka.actor.ActorRef
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.reactive.Observable

import filodb.coordinator.ShardMapper
import filodb.core.metadata.Dataset
import filodb.core.query._
import filodb.core.store.{ChunkSource, PartitionScanMethod}

object Engine extends StrictLogging {
  import filodb.coordinator.client.QueryCommands._
  import Utils._

  /**
   * Query Engine APIs
   */
  def execute(physicalPlan: ExecPlan[_, _], dataset: Dataset, source: ChunkSource): Task[Result] = {
    logger.debug(s"Starting execution of physical plan for dataset ${dataset.ref}:\n$physicalPlan")
    physicalPlan.executeToResult(source, dataset)
  }

  /*************
   * Helper ExecPlan primitives - esp for distribution
   *************/

  /**
   * Distributes subplans that return Observable of Vector or Tuple, concatenating the result into a final
   * Observable of Vector or Tuple (or other supported type of observables)
   * @param coordsAndPlans a Seq of (NodeCoordinatorActor ref, childPlan to send to node)
   * @param parallelism the max number of simultaneous tasks to distribute
   * @param t the maximum Timeout to wait for any individual request to come back
   */
  class DistributeConcat[O](coordsAndPlans: Seq[(ActorRef, ExecPlan[_, Observable[O]])],
                             parallelism: Int)
                            (implicit oMaker: ResultMaker[Observable[O]], t: Timeout, ec: ExecutionContext)
  extends MultiExecNode[Observable[O], Observable[O]](coordsAndPlans.map(_._2)) {
    def execute(source: ChunkSource, dataset: Dataset): Observable[O] = {
      logger.debug(s"Distributing ExecPlans:\n${coordsAndPlans.mkString("\n\n")}")
      val coordsAndMsgs = coordsAndPlans.map { case (c, p) => (c, ExecPlanQuery(dataset.ref, p)) }
      scatterGather[QueryResult](coordsAndMsgs, parallelism)
        .flatMap { case QueryResult(_, res) => oMaker.fromResult(res) }
    }
  }

  object DistributeConcat {
    /**
     * Common case of DistributeConcat where one knows the PartitionScanMethods and wants to distribute work
     * to other FiloDB nodes containing shards.
     * @param shards the shard numbers to distribute the subplans to
     * @param shardMap the ShardMapper containing a mapping of shards to node ActorRefs
     * @param parallelism the max number of simultaneous tasks to distribute
     * @param childPlanFn function to turn methods into plans
     */
    def apply[O](methods: Seq[PartitionScanMethod], shardMap: ShardMapper, parallelism: Int)
                (childPlanFn: PartitionScanMethod => ExecPlan[_, Observable[O]])
                (implicit oMaker: ResultMaker[Observable[O]], t: Timeout, ec: ExecutionContext): DistributeConcat[O] = {
      val coordsAndPlans = methods.map { method =>
        (shardMap.coordForShard(method.shard), childPlanFn(method))
      }
      new DistributeConcat[O](coordsAndPlans, parallelism)
    }
  }
}