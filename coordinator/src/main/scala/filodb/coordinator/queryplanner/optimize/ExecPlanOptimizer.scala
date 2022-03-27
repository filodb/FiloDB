package filodb.coordinator.queryplanner.optimize

import filodb.query.exec.ExecPlan

trait ExecPlanOptimizer {
  /**
   * Apply the optimization.
   */
  def optimize(plan: ExecPlan): ExecPlan
}
