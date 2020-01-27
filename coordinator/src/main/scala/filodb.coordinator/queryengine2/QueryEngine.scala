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

/**
  * FiloDB Query Engine is the facade for execution of FiloDB queries.
  * It is meant for use inside FiloDB nodes to execute materialized
  * ExecPlans as well as from the client to execute LogicalPlans.
  */
class QueryEngine(dsRef: DatasetRef,
                  schemas: Schemas,
                  shardMapperFunc: => ShardMapper,
//                  downsampleMapperFunc: => ShardMapper,
                  failureProvider: FailureProvider,
                  spreadProvider: SpreadProvider = StaticSpreadProvider(),
                  queryEngineConfig: Config = ConfigFactory.empty()) extends StrictLogging {

  val rawClusterPlanner = new SingleClusterPlanner(dsRef, schemas, spreadProvider, shardMapperFunc)
  val downsampleClusterPlanner = new SingleClusterPlanner(dsRef, schemas, spreadProvider, shardMapperFunc)
  val downsampleStitchPlanner = new DownsampleStitchPlanner(rawClusterPlanner, downsampleClusterPlanner)
  val haPlanner = new HighAvailabilityPlanner(dsRef, rawClusterPlanner, failureProvider,
                                              spreadProvider, queryEngineConfig)
  //val multiPodPlanner = new MultiPodPlanner(podLocalityProvider, haPlanner)

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
                  options: QueryOptions): ExecPlan = {
    val queryId = UUID.randomUUID().toString
    val submitTime = System.currentTimeMillis()
    haPlanner.materialize(queryId, submitTime, rootLogicalPlan, options)
  }
}
