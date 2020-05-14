package filodb.coordinator.queryplanner

import com.typesafe.scalalogging.StrictLogging
import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.core.{DatasetRef, SpreadProvider}
import filodb.core.metadata.Schemas
import filodb.core.query.QueryContext
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
                       earliestRawTimestampFn: => Long,
                       earliestDownsampleTimestampFn: => Long,
                       plannerProvider: PlannerProvider,
                       queryConfig: QueryConfig,
                       spreadProvider: SpreadProvider = StaticSpreadProvider(),
                       stitchDispatcher: => PlanDispatcher = { InProcessPlanDispatcher }) extends QueryPlanner
  with StrictLogging {

  // Note the composition of query planners below using decorator pattern
  val rawClusterPlanner = new SingleClusterPlanner(dsRef, schemas, shardMapperFunc,
                                  earliestRawTimestampFn, queryConfig, spreadProvider)
  val downsampleClusterPlanner = new SingleClusterPlanner(dsRef, schemas, downsampleMapperFunc,
                                  earliestDownsampleTimestampFn, queryConfig, spreadProvider)
  val longTimeRangePlanner = new LongTimeRangePlanner(rawClusterPlanner, downsampleClusterPlanner,
                                          earliestRawTimestampFn, stitchDispatcher)
  val haPlanner = new HighAvailabilityPlanner(dsRef, longTimeRangePlanner, failureProvider, queryConfig)
  val multiClusterPlanner = new MultiClusterPlanner(plannerProvider, haPlanner)


  def getBasePlanner: SingleClusterPlanner = longTimeRangePlanner.getBasePlanner
  def materialize(rootLogicalPlan: LogicalPlan, options: QueryContext): ExecPlan = {
    multiClusterPlanner.materialize(rootLogicalPlan, options)
  }
}
