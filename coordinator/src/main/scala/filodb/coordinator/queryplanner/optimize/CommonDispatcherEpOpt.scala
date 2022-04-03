package filodb.coordinator.queryplanner.optimize

import filodb.core.query.QueryConfig
import filodb.query.exec.{ExecPlan, InProcessPlanDispatcher, JoinExecPlan, MultiSchemaPartitionsExec, NonLeafExecPlan}

/**
 * Suppose we had the following ExecPlan:
 *
 * E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(actor=0)
 * -E~BinaryJoinExec(binaryOp=ADD) on ActorPlanDispatcher(actor=0)
 * --T~PeriodicSamplesMapper()
 * ---E~MultiSchemaPartitionsExec(shard=0) on ActorPlanDispatcher(actor=0)
 * --T~PeriodicSamplesMapper()
 * ---E~MultiSchemaPartitionsExec(shard=0) on ActorPlanDispatcher(actor=0)
 * -E~BinaryJoinExec(binaryOp=ADD) on ActorPlanDispatcher(actor=1)
 * --T~PeriodicSamplesMapper()
 * ---E~MultiSchemaPartitionsExec(shard=1) on ActorPlanDispatcher(actor=1)
 * --T~PeriodicSamplesMapper()
 * ---E~MultiSchemaPartitionsExec(shard=1) on ActorPlanDispatcher(actor=1)
 *
 * It would be inefficient to make actor asks between nodes of a subtree that dispatch to the same actor.
 *   Therefore, we can dispatch all children of a single-actor subtree with an InProcessPlanDispatcher:
 *
 * E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(actor=0)
 * -E~BinaryJoinExec(binaryOp=ADD) on ActorPlanDispatcher(actor=0)
 * --T~PeriodicSamplesMapper()
 * ---E~MultiSchemaPartitionsExec(shard=0) on InProcessPlanDispatcher
 * --T~PeriodicSamplesMapper()
 * ---E~MultiSchemaPartitionsExec(shard=0) on InProcessPlanDispatcher
 * -E~BinaryJoinExec(binaryOp=ADD) on ActorPlanDispatcher(actor=1)
 * --T~PeriodicSamplesMapper()
 * ---E~MultiSchemaPartitionsExec(shard=1) on InProcessPlanDispatcher
 * --T~PeriodicSamplesMapper()
 * ---E~MultiSchemaPartitionsExec(shard=1) on InProcessPlanDispatcher
 */
class CommonDispatcherEpOpt(queryConfig: QueryConfig) extends ExecPlanOptimizer {

  /**
   * Describes the result of an optimization step.
   * @param sameShard occupied with a shard iff the entire subtree's data is located on that shard.
   */
  private case class Result(plans: Seq[ExecPlan],
                            sameShard: Option[Int]) {}

  /**
   * Returns a Result where:
   *   (1) each ExecPlan is individually optimized, and
   *   (2) all other Result attributes (i.e. not `subtrees`) aggregate the Results of each optimization.
   */
  private def optimizePlans(plans: Seq[ExecPlan]): Result = {
    val results = plans.map(optimizeWalker(_))
    val shardOpts = results.map(_.sameShard).toSet
    val sameShard = if (shardOpts.size == 1) shardOpts.head else None
    val optPlans = results.flatMap(_.plans)
    val resPlans = if (sameShard.isDefined) {
      optPlans.map(_.withDispatcher(new InProcessPlanDispatcher(queryConfig)))
    } else {
      optPlans
    }
    Result(resPlans, sameShard)
  }

  /**
   * @param copyJoinPlanWithChildren accepts a lhs and rhs, then returns a copy of the
   *                                 join plan with these as children.
   */
  private def optimizeJoinPlan(plan: JoinExecPlan): Result = {
    val lhsOpt = optimizePlans(plan.lhs)
    val rhsOpt = optimizePlans(plan.rhs)
    val optPlan = plan.withChildrenBinary(lhsOpt.plans, rhsOpt.plans)
    val sameShard = if (lhsOpt.sameShard == rhsOpt.sameShard) lhsOpt.sameShard else None
    Result(Seq(optPlan), sameShard)
  }



  private def optimizeNonLeaf(plan: NonLeafExecPlan): Result = {
    val optChildren = optimizePlans(plan.children)
    Result(Seq(plan.withChildren(optChildren.plans)), optChildren.sameShard)
  }

  private def optimizeMultiSchemaPartitionsExec(plan: MultiSchemaPartitionsExec): Result = {
    Result(Seq(plan), Some(plan.shard))
  }

  /**
   * Calls the optimizer function specific to the plan type.
   */
  private def optimizeWalker(plan: ExecPlan): Result = {
    plan match {
      case plan: JoinExecPlan => optimizeJoinPlan(plan)
      case plan: NonLeafExecPlan => optimizeNonLeaf(plan)
      case plan: MultiSchemaPartitionsExec => optimizeMultiSchemaPartitionsExec(plan)
      case plan => {
        // end the optimization here
        // TODO(a_theimer): probably log this
        Result(Seq(plan), None)
      }
    }
  }

  override def optimize(plan: ExecPlan): ExecPlan = {
    val res = optimizeWalker(plan)
    assert(res.plans.size == 1, s"expected single plan but found ${res.plans.size}")
    res.plans.head
  }
}
