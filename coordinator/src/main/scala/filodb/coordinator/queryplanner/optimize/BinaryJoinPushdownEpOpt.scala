package filodb.coordinator.queryplanner.optimize

import scala.collection.mutable

import filodb.coordinator.queryplanner.SingleClusterPlanner.findTargetSchema
import filodb.core.DatasetRef
import filodb.core.query.QueryContext
import filodb.query.exec.{BinaryJoinExec, DistConcatExec, EmptyResultExec, ExecPlan,
                          LocalPartitionDistConcatExec, MultiSchemaPartitionsExec,
                          ReduceAggregateExec, SetOperatorExec}


/**
 * Suppose we had the following ExecPlan:
 *
 * E~BinaryJoinExec(binaryOp=ADD)
 * -T~PeriodicSamplesMapper()
 * --E~MultiSchemaPartitionsExec(shard=0)  // lhs
 * -T~PeriodicSamplesMapper()
 * --E~MultiSchemaPartitionsExec(shard=1)  // lhs
 * -T~PeriodicSamplesMapper()
 * --E~MultiSchemaPartitionsExec(shard=0)  // rhs
 * -T~PeriodicSamplesMapper()
 * --E~MultiSchemaPartitionsExec(shard=1)  // rhs
 *
 * Data is pulled from two shards, sent to the BinaryJoin actor, then that single actor
 *   needs to process all of this data.
 *
 * When (1) a target-schema is defined and (2) every join-key fully-specifies the
 *   target-schema columns, we can relieve much of this single-actor pressure.
 *   Lhs/rhs values will never be joined across shards, so the following ExecPlan
 *   would yield the same result as the above plan:
 *
 * E~LocalPartitionDistConcatExec()
 * -E~BinaryJoinExec(binaryOp=ADD)
 * --T~PeriodicSamplesMapper()
 * ---E~MultiSchemaPartitionsExec(shard=0)
 * --T~PeriodicSamplesMapper()
 * ---E~MultiSchemaPartitionsExec(shard=0)
 * -E~BinaryJoinExec(binaryOp=ADD)
 * --T~PeriodicSamplesMapper()
 * ---E~MultiSchemaPartitionsExec(shard=1)
 * --T~PeriodicSamplesMapper()
 * ---E~MultiSchemaPartitionsExec(shard=1)
 *
 * Now, data is joined locally and in smaller batches.
 *
 * TODO(a_theimer): lots of missing description here. When can['t] this optimization be done?
 */
class BinaryJoinPushdownEpOpt extends ExecPlanOptimizer {

  /**
   * Describes an ExecPlan subtree.
   * @param targetSchemaColsOpt occupied with a set of target schema columns iff a
   *                            target schema is defined for every leaf of the subtree
   */
  private case class Subtree(root: ExecPlan,
                             targetSchemaColsOpt: Option[Set[String]]) {}

  /**
   * Describes the result of an optimization step.
   * @param shardToSubtrees mapping of shards to least-depth subtrees that span a single shard.
   *                        TODO(a_theimer): example
   */
  private case class Result(subtrees: Seq[Subtree],
                            shardToSubtrees: Map[Int, Seq[Subtree]]) {}

  /**
   * Returns a Result where:
   *   (1) each ExecPlan is individually optimized, and
   *   (2) all other Result attributes (i.e. not `subtrees`) aggregate the Results of each optimization.
   */
  private def optimizeAndAggregateResult(plans: Seq[ExecPlan]): Result = {
    val results = plans.map(optimizeWalker(_))
    val optimizedPlans = results.map(_.subtrees).flatten
    val shardToSubtrees = results.map(_.shardToSubtrees).foldLeft(
      new mutable.HashMap[Int, Seq[Subtree]]()){ case (acc, map) =>
      map.foreach{ case (k, v) =>
        val exist = acc.getOrElse(k, Nil)
        acc.put(k, v ++ exist)
      }
      acc
    }
    Result(optimizedPlans, shardToSubtrees.toMap)
  }

  private def optimizeAggregate(plan: ReduceAggregateExec): Result = {
    // for now: just end the optimization here
    val childPlans = optimizeAndAggregateResult(plan.children).subtrees.map(_.root)
    val subtree = Subtree(plan.withChildren(childPlans), None)
    Result(Seq(subtree), Map())
  }

  private def optimizeConcat(plan: DistConcatExec): Result = {
    // for now: just end the optimization here
    val childPlans = optimizeAndAggregateResult(plan.children).subtrees.map(_.root)
    val subtree = Subtree(plan.withChildren(childPlans), None)
    Result(Seq(subtree), Map())
  }

  /**
   * Creates the "pushed-down" join plans.
   * @param copyJoinPlanWithChildren accepts a lhs and rhs, then returns a copy of the
   *                                 join plan with these as children.
   */
  private def makePushdownJoins(lhsRes: Result,
                                rhsRes: Result,
                                queryContext: QueryContext,
                                dataset: DatasetRef,
                                copyJoinPlanWithChildren: (Seq[ExecPlan], Seq[ExecPlan]) => ExecPlan): Result = {

    // TODO(a_theimer): make this less confusing
    val joinPairs = lhsRes.shardToSubtrees.filter{ case (shard, _) =>
      // select only the single-shard subtrees that exist in both maps
      rhsRes.shardToSubtrees.contains(shard)
    }.flatMap { case (shard, lhsSubtrees) =>
      // make all possible combinations of subtrees (1) on the same shard, and (2) on separate lhs/rhs sides
      val rhsSubtrees = rhsRes.shardToSubtrees(shard)
      val pairs = new mutable.ArrayBuffer[(Int, (Subtree, Subtree))]
      for (lhsTree <- lhsSubtrees) {
        for (rhsTree <- rhsSubtrees) {
          pairs.append((shard, (lhsTree, rhsTree)))
        }
      }
      pairs
    }

    if (joinPairs.isEmpty) {
      return Result(Seq(Subtree(EmptyResultExec(queryContext, dataset), None)), Map())
    }

    // make the pushed-down join subtrees
    val shardToSubtrees = new mutable.HashMap[Int, Seq[Subtree]]
    val pushdownSubtrees = joinPairs.map{ case (shard, (lhsSubtree, rhsSubtree)) =>
      val pushdownJoinPlan = copyJoinPlanWithChildren(Seq(lhsSubtree.root), Seq(rhsSubtree.root))
      val targetSchemaColUnion = lhsSubtree.targetSchemaColsOpt.get.union(rhsSubtree.targetSchemaColsOpt.get)
      val pushdownJoinSubtree = Subtree(pushdownJoinPlan, Some(targetSchemaColUnion))
      shardToSubtrees(shard) = Seq(pushdownJoinSubtree)
      pushdownJoinSubtree
    }.toSeq
    Result(pushdownSubtrees, shardToSubtrees.toMap)
  }

  /**
   * Helper function for optimizeSetOp / optimizeBinaryJoin.
   * @param copyJoinPlanWithChildren accepts a lhs and rhs, then returns a copy of the
   *                                 join plan with these as children.
   */
  private def optimizeJoinPlan(lhsRes: Result,
                               rhsRes: Result,
                               on: Seq[String],
                               ignoring: Seq[String],
                               queryContext: QueryContext,
                               dataset: DatasetRef,
                               copyJoinPlanWithChildren: (Seq[ExecPlan], Seq[ExecPlan]) => ExecPlan): Result = {
    val childSubtrees = Seq(lhsRes, rhsRes).flatMap(_.subtrees)
    // make sure all child subtrees have all leaf-level target schema columns defined and present
    if (childSubtrees.forall(_.targetSchemaColsOpt.isDefined)) {
      // get union of all target schema columns
      val targetSchemaColsUnion = childSubtrees.map(_.targetSchemaColsOpt.get)
        .foldLeft(Set[String]()){ case (acc, nextCols) =>
          acc.union(nextCols)
        }
      // make sure all cols present in join keys (TODO(a_theimer): relax this?)
      // TODO(a_theimer): this is not technically correct; combines on-empty and both-empty cases
      val alltargetSchemaColsPresent = if (on.isEmpty) {
        // make sure no target schema strings are ignored
        ignoring.find(targetSchemaColsUnion.contains(_)).isEmpty
      } else {
        // make sure all target schema cols are included in on
        targetSchemaColsUnion.forall(on.toSet.contains(_))
      }
      if (alltargetSchemaColsPresent) {
        return makePushdownJoins(lhsRes, rhsRes, queryContext, dataset, copyJoinPlanWithChildren)
      }
    }
    // no pushdown and end optimization here
    val root = copyJoinPlanWithChildren(lhsRes.subtrees.map(_.root), rhsRes.subtrees.map(_.root))
    Result(Seq(Subtree(root, None)), Map())
  }

  private def optimizeSetOp(plan: SetOperatorExec): Result = {
    val lhsRes = optimizeAndAggregateResult(plan.lhs)
    val rhsRes = optimizeAndAggregateResult(plan.rhs)
    optimizeJoinPlan(lhsRes, rhsRes, plan.on, plan.ignoring, plan.queryContext, plan.dataset,
      (left: Seq[ExecPlan], right: Seq[ExecPlan]) => {
        val res = plan.copy(lhs = left, rhs = right)
        plan.copyStateInto(res)
        res
      })
  }

  private def optimizeBinaryJoin(plan: BinaryJoinExec): Result = {
    val lhsRes = optimizeAndAggregateResult(plan.lhs)
    val rhsRes = optimizeAndAggregateResult(plan.rhs)
    optimizeJoinPlan(lhsRes, rhsRes, plan.on, plan.ignoring, plan.queryContext, plan.dataset,
      (left: Seq[ExecPlan], right: Seq[ExecPlan]) => {
        val res = plan.copy(lhs = left, rhs = right)
        plan.copyStateInto(res)
        res
      })
  }

  private def optimizeMultiSchemaPartitionsExec(plan: MultiSchemaPartitionsExec): Result = {
    // get the target schema columns (if they exist)
    val targetSchemaColsOpt = plan.queryContext.plannerParams.targetSchemaProvider.map { provider =>
      val changes = provider.targetSchemaFunc(plan.filters)
      val startMs = plan.chunkMethod.startTime
      val endMs = plan.chunkMethod.endTime
      findTargetSchema(changes, startMs, endMs).map(_.schema.toSet)
    }.filter(_.isDefined).map(_.get)
    val subtree = Subtree(plan, targetSchemaColsOpt)
    Result(Seq(subtree), Map(plan.shard -> Seq(subtree)))
  }

  /**
   * Calls the optimizer function specific to the plan type.
   */
  private def optimizeWalker(plan: ExecPlan): Result = {
    plan match {
      case plan: BinaryJoinExec => optimizeBinaryJoin(plan)
      case plan: SetOperatorExec => optimizeSetOp(plan)
      case plan: ReduceAggregateExec => optimizeAggregate(plan)
      case plan: DistConcatExec => optimizeConcat(plan)
      case plan: MultiSchemaPartitionsExec => optimizeMultiSchemaPartitionsExec(plan)
      case plan => {
        // end the optimization here
        Result(Seq(Subtree(plan, None)), Map())
      }
    }
  }

  override def optimize(plan: ExecPlan): ExecPlan = {
    val res = optimizeWalker(plan)
    if (res.subtrees.size > 1) {
      LocalPartitionDistConcatExec(plan.queryContext, plan.dispatcher, res.subtrees.map(_.root))
    } else {
      res.subtrees.head.root
    }
  }
}
