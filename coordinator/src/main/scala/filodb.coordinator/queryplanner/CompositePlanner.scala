package filodb.coordinator.queryplanner

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.core.{DatasetRef, SpreadProvider}
import filodb.core.metadata.Schemas
import filodb.query._
import filodb.query.exec._

/**
  * Query Planner implementation that composes other planners to provide overall capability
  * of high availability, downsampling and (later) multi-cluster partitioning.
  */
class CompositePlanner(dsRef: DatasetRef,
                       schemas: Schemas,
                       shardMapperFunc: => ShardMapper,
                       downsampleMapperFunc: => ShardMapper,
                       failureProvider: FailureProvider,
                       rawDataRetentionMillis: Long,
                       spreadProvider: SpreadProvider = StaticSpreadProvider(),
                       stitchDispatcher: => PlanDispatcher = { InProcessPlanDispatcher },
                       queryEngineConfig: Config = ConfigFactory.empty()) extends QueryPlanner with StrictLogging {

  // Note the composition of query planners below using decorator pattern
  val rawClusterPlanner = new SingleClusterPlanner(dsRef, schemas, shardMapperFunc, spreadProvider)
  val downsampleClusterPlanner = new SingleClusterPlanner(dsRef, schemas, downsampleMapperFunc, spreadProvider)
  val longTimeRangePlanner = new LongTimeRangePlanner(rawClusterPlanner, downsampleClusterPlanner,
                                                         rawDataRetentionMillis, stitchDispatcher)
  val haPlanner = new HighAvailabilityPlanner(dsRef, longTimeRangePlanner, failureProvider, queryEngineConfig)
  //val multiPodPlanner = new MultiClusterPlanner(podLocalityProvider, haPlanner)

  def materialize(rootLogicalPlan: LogicalPlan, options: QueryContext): ExecPlan = {
    haPlanner.materialize(rootLogicalPlan, options)
  }
}
