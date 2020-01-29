package filodb.coordinator.queryengine2

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging

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
                  queryEngineConfig: Config = ConfigFactory.empty()) extends QueryPlanner with StrictLogging {

  // Note the composition of query planners below using decorator pattern
  val rawClusterPlanner = new SingleClusterPlanner(dsRef, schemas, spreadProvider, shardMapperFunc)
  val downsampleClusterPlanner = new SingleClusterPlanner(dsRef, schemas, spreadProvider, shardMapperFunc)
  val downsampleStitchPlanner = new LongTimeRangePlanner(rawClusterPlanner, downsampleClusterPlanner)
  // TODO haPlanner should later use downsampleStitchPlanner
  val haPlanner = new HighAvailabilityPlanner(dsRef, rawClusterPlanner, failureProvider,
                                              spreadProvider, queryEngineConfig)
  //val multiPodPlanner = new MultiClusterPlanner(podLocalityProvider, haPlanner)

  def materialize(rootLogicalPlan: LogicalPlan, options: QueryContext): ExecPlan = {
    haPlanner.materialize(rootLogicalPlan, options)
  }
}
