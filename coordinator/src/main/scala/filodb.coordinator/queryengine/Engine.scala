package filodb.coordinator.queryengine

import java.util.Date

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.reactive.Observable
import org.scalactic.{Bad, Or}
import org.scalactic.OptionSugar._

import filodb.coordinator.ShardMapper
import filodb.coordinator.client.LogicalPlan
import filodb.coordinator.client.LogicalPlan.{PartitionsInstant, PartitionsRange}
import filodb.core.ErrorResponse
import filodb.core.metadata.Dataset
import filodb.core.query._
import filodb.core.store.{ChunkSource, PartitionScanMethod}

object Engine extends StrictLogging {

  case class ExecArgs(dataset: Dataset,
                      shardMap: ShardMapper,
                      submitTime: Long = new Date().getTime)

  import Utils._
  import filodb.coordinator.client.QueryCommands._

  /**
    * Query Engine APIs
    */
  def execute(physicalPlan: ExecPlan[_, _], dataset: Dataset, source: ChunkSource, limit: Int): Task[Result] = {
    logger.debug(s"Starting execution of physical plan for dataset ${dataset.ref}:\n$physicalPlan")
    try {
      physicalPlan.executeToResult(source, dataset, limit)
    } catch {
      case e: Exception => Task.raiseError(e)
    }
  }

  def materialize(plan: LogicalPlan,
                  execArgs: ExecArgs,
                  queryOptions: QueryOptions = QueryOptions())
                 (implicit ec: ExecutionContext)
  : ExecPlan[_, _] Or ErrorResponse = {
    implicit val askTimeout = Timeout(queryOptions.queryTimeoutSecs.seconds)
    plan match {
      case PartitionsInstant(partQuery, cols) =>
        // TODO: extract the last value of every vector only. OR, report a time range for the single value aggregate
        scatterGatherPlan(partQuery, MostRecentSample, execArgs, cols, queryOptions)
      case PartitionsRange(partQuery, dataQuery, cols) =>
        scatterGatherPlan(partQuery, dataQuery, execArgs, cols, queryOptions)
      case other: LogicalPlan => Bad(BadQuery(s"Unsupported logical plan $other"))
    }
  }


  protected def validateFunction(funcName: String): AggregationFunction Or ErrorResponse =
    AggregationFunction.withNameInsensitiveOption(funcName)
      .toOr(BadQuery(s"No such aggregation function $funcName"))

  protected def validateCombiner(combinerName: String): CombinerFunction Or ErrorResponse =
    CombinerFunction.withNameInsensitiveOption(combinerName)
      .toOr(BadQuery(s"No such combiner function $combinerName"))


  // Validate and convert "raw" logical plans into physical plan
  protected def scatterGatherPlan(partQuery: PartitionQuery,
                                  dataQuery: DataQuery,
                                  execArgs: ExecArgs,
                                  columns: Seq[String],
                                  options: QueryOptions)
                                 (implicit t: Timeout, ec: ExecutionContext)
  : ExecPlan[_, _] Or ErrorResponse = {
    for {colIDs <- getColumnIDs(execArgs.dataset, columns)
         chunkMethod <- validateDataQuery(execArgs.dataset, dataQuery)
         partMethods <- validatePartQuery(execArgs.dataset, execArgs.shardMap, partQuery, options)}
      yield {
        // Use distributeConcat to scatter gather Vectors or Tuples from each shard
        val plan = dataQuery match {
          case MostRecentSample =>
            Engine.DistributeConcat(partMethods, execArgs.shardMap,
              options.parallelism, options.itemLimit, execArgs.submitTime) { method =>
              ExecPlan.streamLastTuplePlan(execArgs.dataset, colIDs, method)
            }
          case _ =>
            Engine.DistributeConcat(partMethods, execArgs.shardMap,
              options.parallelism, options.itemLimit, execArgs.submitTime) { method =>
              new ExecPlan.LocalVectorReader(colIDs, method, chunkMethod)
            }
        }
        plan
      }
  }


  /** ***********
    * Helper ExecPlan primitives - esp for distribution
    * ************/

  /**
    * Distributes subplans that return Observable of Vector or Tuple, concatenating the result into a final
    * Observable of Vector or Tuple (or other supported type of observables)
    *
    * @param coordsAndPlans a Seq of (NodeCoordinatorActor ref, childPlan to send to node)
    * @param parallelism    the max number of simultaneous tasks to distribute
    * @param itemLimit      the max number of items to return from each child
    * @param t              the maximum Timeout to wait for any individual request to come back
    */
  class DistributeConcat[O](coordsAndPlans: Seq[(ActorRef, ExecPlan[_, Observable[O]])],
                            parallelism: Int, itemLimit: Int, submitTime: Long)
                           (implicit oMaker: ResultMaker[Observable[O]], t: Timeout, ec: ExecutionContext)
    extends MultiExecNode[Observable[O], Observable[O]](coordsAndPlans.map(_._2)) {
    def execute(source: ChunkSource, dataset: Dataset): Observable[O] = {
      logger.debug(s"Distributing ExecPlans:\n${coordsAndPlans.mkString("\n\n")}")
      val coordsAndMsgs = coordsAndPlans.map {
        case (c, p) => (c, ExecPlanQuery(dataset.ref, p, itemLimit, submitTime))
      }
      scatterGather[QueryResult](coordsAndMsgs, parallelism)
        .flatMap { case QueryResult(_, res) => oMaker.fromResult(res) }
    }

    def args: Seq[String] = coordsAndPlans.map(_._1.toString)
  }

  object DistributeConcat {
    //scalastyle:off
    /**
      * Common case of DistributeConcat where one knows the PartitionScanMethods and wants to distribute work
      * to other FiloDB nodes containing shards.
      *
      * @param shardMap    the ShardMapper containing a mapping of shards to node ActorRefs
      * @param parallelism the max number of simultaneous tasks to distribute
      * @param itemLimit   the max number of items to return from each child
      * @param childPlanFn function to turn methods into plans
      */
    def apply[O](methods: Seq[PartitionScanMethod], shardMap: ShardMapper, parallelism: Int,
                 itemLimit: Int, submitTime: Long)
                (childPlanFn: PartitionScanMethod => ExecPlan[_, Observable[O]])
                (implicit oMaker: ResultMaker[Observable[O]], t: Timeout, ec: ExecutionContext): DistributeConcat[O] = {
      val coordsAndPlans = methods.map { method =>
        (shardMap.coordForShard(method.shard), childPlanFn(method))
      }
      new DistributeConcat[O](coordsAndPlans, parallelism, itemLimit, submitTime)
    }

    //scalastyle:on
  }


}