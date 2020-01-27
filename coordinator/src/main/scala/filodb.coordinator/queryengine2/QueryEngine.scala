package filodb.coordinator.queryengine2

import java.util.UUID

import scala.concurrent.duration._

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler

import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.core.{DatasetRef, SpreadProvider}
import filodb.core.metadata.Schemas
import filodb.query._
import filodb.query.exec._


trait TsdbQueryParams
case class PromQlQueryParams(promQl: String, start: Long, step: Long, end: Long,
                             spread: Option[Int] = None, processFailure: Boolean = true) extends TsdbQueryParams
object UnavailablePromQlQueryParams extends TsdbQueryParams

/**
  * FiloDB Query Engine is the facade for execution of FiloDB queries.
  * It is meant for use inside FiloDB nodes to execute materialized
  * ExecPlans as well as from the client to execute LogicalPlans.
  */
class QueryEngine(dsRef: DatasetRef,
                  schemas: Schemas,
                  shardMapperFunc: => ShardMapper,
                  downsampleMapperFunc: => ShardMapper,
                  failureProvider: FailureProvider,
                  spreadProvider: SpreadProvider = StaticSpreadProvider(),
                  queryEngineConfig: Config = ConfigFactory.empty())
                   extends StrictLogging {

  val localPlanner = new SinglePodPlanner(dsRef, schemas, shardMapperFunc)
  val multiPodPlanner = new MultiPodPlanner(dsRef, localPlanner, failureProvider, queryEngineConfig)

  /**
    * This is the facade to trigger orchestration of the ExecPlan.
    * It sends the ExecPlan to the destination where it will be executed.
    */
  def dispatchExecPlan(execPlan: ExecPlan)
                      (implicit sched: Scheduler,
                       timeout: FiniteDuration): Task[QueryResponse] = {
    val currentSpan = Kamon.currentSpan()
    Kamon.withSpan(currentSpan) {
      execPlan.dispatcher.dispatch(execPlan)
    }
  }


  /**
    * Converts a LogicalPlan to the ExecPlan
    */
  def materialize(rootLogicalPlan: LogicalPlan,
                  options: QueryOptions,
                  tsdbQueryParams: TsdbQueryParams): ExecPlan = {

    multiPodPlanner.materialize(rootLogicalPlan, options, tsdbQueryParams)
  }


}
