//package filodb.coordinator.queryplanner
//
//import filodb.core.query.QueryContext
//import filodb.query.LogicalPlan
//import filodb.query.exec.ExecPlan
//

//
//class MultiPartitionPlanner(plannerProvider: PlannerProvider,
//                            localPlanner: QueryPlanner) extends  QueryPlanner {
//
//  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
//    val routingKeys = LogicalPlanUtils.getRoutingKeys(logicalPlan)
//    // TODO get time for Non Periodic Series like LabelValues, SeriesKeysByFilters
//    val timeRange = LogicalPlanUtils.getPeriodicSeriesTimeFromLogicalPlan(logicalPlan)
//    val planners = routingKeys.flatMap(plannerProvider.getPlanners(_, timeRange))
//    localPlanner.materialize(logicalPlan, qContext)
//  }
//}
