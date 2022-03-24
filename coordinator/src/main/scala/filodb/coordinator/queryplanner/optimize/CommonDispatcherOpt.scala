package filodb.coordinator.queryplanner.optimize

import filodb.core.query.EmptyQueryConfig
import filodb.query.exec.{BinaryJoinExec, ExecPlan, InProcessPlanDispatcher, MultiSchemaPartitionsExec, NonLeafExecPlan, SetOperatorExec}

object CommonDispatcherOpt {

  private case class Result(plan: ExecPlan,
                            sameShard: Option[Int]) {}

  private case class ChildrenResult(children: Seq[Seq[ExecPlan]],
                                    sameShard: Option[Int])

  // TODO(a_theimer): cleanup
  private def optimizeChildren(children: Seq[Seq[ExecPlan]]): ChildrenResult = {
    val results = children.map(_.map(optimizeWalker(_)))
    val shards = results.flatten.map(_.sameShard)
    val sameShard = if (shards.forall(_.isDefined)) {
      val shardSet = shards.map(_.get).toSet
      if (shardSet.size == 1) {
        Some(shardSet.head)
      } else {
        None
      }
    } else {
      None
    }

    val plans = results.map(_.map(_.plan))
    val optChildren = if (sameShard.isDefined) {
      plans.map(_.map(_.withDispatcher(new InProcessPlanDispatcher(EmptyQueryConfig))))
    } else {
      plans
    }

    ChildrenResult(optChildren, sameShard)
  }

  private def optimizeNonLeaf(plan: NonLeafExecPlan): Result = {
    val optChildren = optimizeChildren(Seq(plan.children))
    assert(optChildren.children.size == 1)
    Result(plan.withChildren(optChildren.children.head), optChildren.sameShard)
  }

  private def optimizeBinaryJoinExec(plan: BinaryJoinExec): Result = {
    val optChildren = optimizeChildren(Seq(plan.lhs, plan.rhs))
    assert(optChildren.children.size == 2)

    val optPlan = plan.copy(lhs = optChildren.children(0), rhs = optChildren.children(1))
    Result(optPlan, optChildren.sameShard)
  }

  // TODO(a_theimer): get rid of all the copy-paste
  private def optimizeSetOperatorExec(plan: SetOperatorExec): Result = {
    val optChildren = optimizeChildren(Seq(plan.lhs, plan.rhs))
    assert(optChildren.children.size == 2)

    val optPlan = plan.copy(lhs = optChildren.children(0), rhs = optChildren.children(1))
    Result(optPlan, optChildren.sameShard)
  }

  private def optimizeMultiSchemaPartitionsExec(plan: MultiSchemaPartitionsExec): Result = {
    Result(plan, Some(plan.shard))
  }

  private def optimizeWalker(plan: ExecPlan): Result = {
    plan match {
      case plan: BinaryJoinExec => optimizeBinaryJoinExec(plan)
      case plan: SetOperatorExec => optimizeSetOperatorExec(plan)
      case plan: NonLeafExecPlan => optimizeNonLeaf(plan)
      case plan: MultiSchemaPartitionsExec => optimizeMultiSchemaPartitionsExec(plan)
      case plan => Result(plan, None)
    }
  }

  def optimize(plan: ExecPlan): ExecPlan = {
    optimizeWalker(plan).plan
  }

}
