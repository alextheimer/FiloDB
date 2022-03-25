package filodb.coordinator.queryplanner.optimize

import filodb.coordinator.queryplanner.SingleClusterPlanner.findTargetSchema
import filodb.query.exec.{BinaryJoinExec, DistConcatExec, EmptyResultExec, ExecPlan, LocalPartitionDistConcatExec, MultiSchemaPartitionsExec, ReduceAggregateExec, SetOperatorExec}

import scala.collection.mutable

/**
 * Materialize a binary join with the target-schema pushdown optimization.
 * materializeBinaryJoin helper.
 *
 * When both children of a BinaryJoin are a PeriodicSeries, the result ExecPlan would
 *   typically be of the form:
 *
 * E~BinaryJoinExec(binaryOp=ADD) on ActorPlanDispatcher(actor=0)
 * -T~PeriodicSamplesMapper()
 * --E~MultiSchemaPartitionsExec(shard=0) on ActorPlanDispatcher(actor=0)  // lhs
 * -T~PeriodicSamplesMapper()
 * --E~MultiSchemaPartitionsExec(shard=0) on ActorPlanDispatcher(actor=0)  // rhs
 * -T~PeriodicSamplesMapper()
 * --E~MultiSchemaPartitionsExec(shard=1) on ActorPlanDispatcher(actor=1)  // lhs
 * -T~PeriodicSamplesMapper()
 * --E~MultiSchemaPartitionsExec(shard=1) on ActorPlanDispatcher(actor=1)  // rhs
 *
 * Data is pulled from each shard, sent to the BinaryJoin actor, then that single actor
 *   needs to process all of this data.
 *
 * When (1) a target-schema is defined and (2) every join-key fully-specifies the
 *   target-schema columns, we can relieve much of this single-actor pressure.
 *   Lhs/rhs values will never be joined across shards, so the following ExecPlan
 *   would yield the same result as the above plan:
 *
 * E~LocalPartitionDistConcatExec() on ActorPlanDispatcher(actor=0)
 * -E~BinaryJoinExec(binaryOp=ADD) on ActorPlanDispatcher(actor=0)
 * --T~PeriodicSamplesMapper()
 * ---E~MultiSchemaPartitionsExec(shard=0) on InProcessPlanDispatcher()
 * --T~PeriodicSamplesMapper()
 * ---E~MultiSchemaPartitionsExec(shard=0) on InProcessPlanDispatcher()
 * -E~BinaryJoinExec(binaryOp=ADD) on ActorPlanDispatcher(actor=1)
 * --T~PeriodicSamplesMapper()
 * ---E~MultiSchemaPartitionsExec(shard=1) on InProcessPlanDispatcher()
 * --T~PeriodicSamplesMapper()
 * ---E~MultiSchemaPartitionsExec(shard=1) on InProcessPlanDispatcher()
 *
 * Now, data is joined locally and in smaller batches.
 */
object BinaryJoinPushdownOpt {

  private case class Subtree(root: ExecPlan,
                             targetSchemaColsOpt: Option[Set[String]]) {}

  private case class Result(plans: Seq[Subtree],
                            shardToSubtrees: Map[Int, Seq[Subtree]]) {}

//  /**
//   * Given a BinaryJoin, returns true iff it would be valid to apply the "pushdown" optimization.
//   * See materializeBinaryJoinWithPushdown for more details about the optimization.
//   *
//   * It is valid to apply the optimization iff three conditions are met:
//   *   (1) each child has a target schema defined for its RawSeries filters
//   *   (2) when the eventual ExecPlan is executed, each child join-key must
//   *       constitute a superset of the target-schema columns.
//   */
//  private def canPushdownBinaryJoin(binJoin: BinaryJoinExec): Boolean = {
//    val targetSchemaProviderOpt = binJoin.queryContext.plannerParams.targetSchemaProvider
//    if (targetSchemaProviderOpt.isEmpty) {
//      return false
//    }
//    Seq(binJoin.lhs, binJoin.rhs).forall { case child =>
//      val targetSchemaOpt = {
//        val filters = getRawSeriesFilters(child)
//        if (filters.size != 1) {
//          logger.warn(s"expected a single set of column filters, but found $filters " +
//            s"from child $child")
//          return false
//        }
//        val targetSchemaChanges = targetSchemaProviderOpt.get.targetSchemaFunc(filters.head)
//        findTargetSchema(targetSchemaChanges, child.startMs, child.endMs)
//      }
//      if (targetSchemaOpt.isEmpty) {
//        return false
//      }
//      joinKeyIsTargetSchemaSuperset(binJoin, targetSchemaOpt.get.schema)
//    }
//  }

  private def optimizeChildren(plans: Seq[ExecPlan]): Result = {
    val results = plans.map(optimizeWalker(_))
    val optimizedChildren = results.map(_.plans).flatten
    val updatedMap = results.map(_.shardToSubtrees).foldLeft(
      new mutable.HashMap[Int, Seq[Subtree]]()){ case (acc, map) =>
      map.foreach{ case (k, v) =>
        val exist = acc.getOrElse(k, Nil)
        acc.put(k, v ++ exist)
      }
      acc
    }
    Result(optimizedChildren, updatedMap.toMap)
  }

  private def optimizeAggregate(plan: ReduceAggregateExec): Result = {
    // for now: just end the optimization here
    val childrenRes = optimizeChildren(plan.children)
    val updated = plan.withChildren(childrenRes.plans.map(_.root))
    Result(Seq(Subtree(updated, None)), Map())
  }

  private def optimizeConcat(plan: DistConcatExec): Result = {
    // for now: just end the optimization here
    val childrenRes = optimizeChildren(plan.children)
    val updated = plan.withChildren(childrenRes.plans.map(_.root))
    Result(Seq(Subtree(updated, None)), Map())
  }

  private def makePushdownsSet(exec: SetOperatorExec, lhsRes: Result, rhsRes: Result): Result = {

    // map each rhs plan to its same-shard lhs counterpart (s)
    val joinPairs = lhsRes.shardToSubtrees.filter{ case (shard, subtrees) => rhsRes.shardToSubtrees.contains(shard) }
      .flatMap { case (shard, lhsSubtrees) =>
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
      return Result(Seq(Subtree(EmptyResultExec(exec.queryContext, exec.dataset), None)), Map())
    }

    // build the pushed-down join subtrees
    val newMap = new mutable.HashMap[Int, Seq[Subtree]]
    val res = joinPairs.map{ case (shard, (lhsSt, rhsSt)) =>
      val subres = exec.copy(lhs = Seq(lhsSt.root), rhs = Seq(rhsSt.root))
      exec.copyStateInto(subres)
      newMap(shard) = Seq(Subtree(subres, Some(lhsSt.targetSchemaColsOpt.get.union(rhsSt.targetSchemaColsOpt.get))))
      Subtree(subres, Some(lhsSt.targetSchemaColsOpt.get.union(rhsSt.targetSchemaColsOpt.get)))
    }
    Result(res.toSeq, newMap.toMap)


    //    val execPlans = if (lp.operator.isInstanceOf[SetOperator]) {
    //      joinPairs.map{ case (lhs, rhs) =>
    //        exec.SetOperatorExec(qContext, lhs.dispatcher,
    //          Seq(lhs.withDispatcher(inProcessPlanDispatcher)),
    //          Seq(rhs.withDispatcher(inProcessPlanDispatcher)), lp.operator,
    //          LogicalPlanUtils.renameLabels(lp.on, dsOptions.metricColumn),
    //          LogicalPlanUtils.renameLabels(lp.ignoring, dsOptions.metricColumn), dsOptions.metricColumn,
    //          rvRangeFromPlan(lp))
    //      }
    //    } else {
    //
    //    }
  }

  private def makePushdownsBin(exec: BinaryJoinExec, lhsRes: Result, rhsRes: Result): Result = {

    // map each rhs plan to its same-shard lhs counterpart (s)
    val joinPairs = lhsRes.shardToSubtrees.filter{ case (shard, subtrees) => rhsRes.shardToSubtrees.contains(shard) }
      .flatMap { case (shard, lhsSubtrees) =>
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
      return Result(Seq(Subtree(EmptyResultExec(exec.queryContext, exec.dataset), None)), Map())
    }

    // build the pushed-down join subtrees
    val newMap = new mutable.HashMap[Int, Seq[Subtree]]
    val res = joinPairs.map{ case (shard, (lhsSt, rhsSt)) =>
      val subres = exec.copy(lhs = Seq(lhsSt.root), rhs = Seq(rhsSt.root))
      exec.copyStateInto(subres)
      newMap(shard) = Seq(Subtree(subres, Some(lhsSt.targetSchemaColsOpt.get.union(rhsSt.targetSchemaColsOpt.get))))
      Subtree(subres, Some(lhsSt.targetSchemaColsOpt.get.union(rhsSt.targetSchemaColsOpt.get)))
    }
    Result(res.toSeq, newMap.toMap)


//    val execPlans = if (lp.operator.isInstanceOf[SetOperator]) {
//      joinPairs.map{ case (lhs, rhs) =>
//        exec.SetOperatorExec(qContext, lhs.dispatcher,
//          Seq(lhs.withDispatcher(inProcessPlanDispatcher)),
//          Seq(rhs.withDispatcher(inProcessPlanDispatcher)), lp.operator,
//          LogicalPlanUtils.renameLabels(lp.on, dsOptions.metricColumn),
//          LogicalPlanUtils.renameLabels(lp.ignoring, dsOptions.metricColumn), dsOptions.metricColumn,
//          rvRangeFromPlan(lp))
//      }
//    } else {
//
//    }
  }

  private def optimizeSetOp(plan: SetOperatorExec): Result = {
    val lhsRes = optimizeChildren(plan.lhs)
    val rhsRes = optimizeChildren(plan.rhs)

    val targetSchemaColsPreserved = Seq(lhsRes, rhsRes).flatMap(_.plans).forall(_.targetSchemaColsOpt.isDefined)
    if (targetSchemaColsPreserved) {
      // get union of all target schema cols
      val targetSchemaColsUnion = Seq(lhsRes, rhsRes).flatMap(_.plans).map(_.targetSchemaColsOpt.get)
        .foldLeft(Set[String]()){ case (acc, next) =>
          acc.union(next)
        }
      // make sure all cols present in join keys (TODO(a_theimer): relax)
      // TODO(a_theimer): this is not technically correct; combines on-empty and both-empty cases
      val allPresent = if (plan.on.isEmpty) {
        // make sure no target schema strings are ignored
        plan.ignoring.find(targetSchemaColsUnion.contains(_)).isEmpty
      } else {
        // make sure all target schema cols are included in on
        targetSchemaColsUnion.forall(plan.on.toSet.contains(_))
      }
      if (allPresent) {
        // do the pushdown
        return makePushdownsSet(plan, lhsRes, rhsRes)
      }
    }
    // no pushdown and kill optimization here
    val root = plan.copy(lhs = lhsRes.plans.map(_.root),
      rhs = rhsRes.plans.map(_.root))
    plan.copyStateInto(root)
    Result(Seq(Subtree(root, None)), Map())
  }

  private def optimizeBinaryJoin(plan: BinaryJoinExec): Result = {
    val lhsRes = optimizeChildren(plan.lhs)
    val rhsRes = optimizeChildren(plan.rhs)

    val targetSchemaColsPreserved = Seq(lhsRes, rhsRes).flatMap(_.plans).forall(_.targetSchemaColsOpt.isDefined)
    if (targetSchemaColsPreserved) {
      // get union of all target schema cols
      val targetSchemaColsUnion = Seq(lhsRes, rhsRes).flatMap(_.plans).map(_.targetSchemaColsOpt.get)
        .foldLeft(Set[String]()){ case (acc, next) =>
        acc.union(next)
      }
      // make sure all cols present in join keys (TODO(a_theimer): relax)
      // TODO(a_theimer): this is not technically correct; combines on-empty and both-empty cases
      val allPresent = if (plan.on.isEmpty) {
        // make sure no target schema strings are ignored
        plan.ignoring.find(targetSchemaColsUnion.contains(_)).isEmpty
      } else {
        // make sure all target schema cols are included in on
        targetSchemaColsUnion.forall(plan.on.toSet.contains(_))
      }
      if (allPresent) {
        // do the pushdown
        return makePushdownsBin(plan, lhsRes, rhsRes)
      }
    }
    // no pushdown and kill optimization here
    val root = plan.copy(lhs = lhsRes.plans.map(_.root),
                         rhs = rhsRes.plans.map(_.root))
    plan.copyStateInto(root)
    Result(Seq(Subtree(root, None)), Map())
  }

//  private def optimizeBinaryJoin(plan: BinaryJoinExec): Result = {
//    // for the lhs, map shards to plans
//    val lhsShardToPlanMap = lhs.plans.map{ p =>
//      val mspe = p.asInstanceOf[MultiSchemaPartitionsExec]
//      mspe.shard -> mspe
//    }.toMap
//
//    // map each rhs plan to its same-shard lhs counterpart
//    val joinPairs = rhs.plans
//      .map(_.asInstanceOf[MultiSchemaPartitionsExec])
//      .filter(r => lhsShardToPlanMap.contains(r.shard))
//      .map(r => (lhsShardToPlanMap.apply(r.shard), r))
//
//    if (joinPairs.isEmpty) {
//      return PlanResult(Seq(EmptyResultExec(qContext, dataset.ref)))
//    }
//
//    // build the pushed-down join subtrees, where each child plan will be executed in-process.
//    val execPlans = if (lp.operator.isInstanceOf[SetOperator]) {
//      joinPairs.map{ case (lhs, rhs) =>
//        exec.SetOperatorExec(qContext, lhs.dispatcher,
//          Seq(lhs.withDispatcher(inProcessPlanDispatcher)),
//          Seq(rhs.withDispatcher(inProcessPlanDispatcher)), lp.operator,
//          LogicalPlanUtils.renameLabels(lp.on, dsOptions.metricColumn),
//          LogicalPlanUtils.renameLabels(lp.ignoring, dsOptions.metricColumn), dsOptions.metricColumn,
//          rvRangeFromPlan(lp))
//      }
//    } else {
//      joinPairs.map{ case (lhs, rhs) =>
//        BinaryJoinExec(qContext, lhs.dispatcher,
//          Seq(lhs.withDispatcher(inProcessPlanDispatcher)),
//          Seq(rhs.withDispatcher(inProcessPlanDispatcher)), lp.operator, lp.cardinality,
//          LogicalPlanUtils.renameLabels(lp.on, dsOptions.metricColumn),
//          LogicalPlanUtils.renameLabels(lp.ignoring, dsOptions.metricColumn),
//          LogicalPlanUtils.renameLabels(lp.include, dsOptions.metricColumn), dsOptions.metricColumn,
//          rvRangeFromPlan(lp))
//      }
//    }
//  }

  private def optimizeMultiSchemaPartitionsExec(plan: MultiSchemaPartitionsExec): Result = {
    val targetSchemaColsOpt = plan.queryContext.plannerParams.targetSchemaProvider.map { provider =>
      val changes = provider.targetSchemaFunc(plan.filters)
      val startms = plan.chunkMethod.startTime
      val endms = plan.chunkMethod.startTime
      findTargetSchema(changes, startms, endms).map(_.schema.toSet)
    }.filter(_.isDefined).map(_.get)
    val subtree = Subtree(plan, targetSchemaColsOpt)
    Result(Seq(subtree), Map(plan.shard -> Seq(subtree)))
  }

  private def optimizeWalker(plan: ExecPlan): Result = {
    plan match {
      case plan: BinaryJoinExec => optimizeBinaryJoin(plan)
      case plan: SetOperatorExec => optimizeSetOp(plan)
      case plan: ReduceAggregateExec => optimizeAggregate(plan)
      case plan: DistConcatExec => optimizeConcat(plan)
      case plan: MultiSchemaPartitionsExec => optimizeMultiSchemaPartitionsExec(plan)
      case plan => Result(Seq(Subtree(plan, None)), Map())
    }
  }

  def optimize(plan: ExecPlan): ExecPlan = {
    val res = optimizeWalker(plan)
    if (res.plans.size > 1) {
      LocalPartitionDistConcatExec(plan.queryContext, plan.dispatcher, res.plans.map(_.root))
    } else {
      res.plans.head.root
    }
  }
}
