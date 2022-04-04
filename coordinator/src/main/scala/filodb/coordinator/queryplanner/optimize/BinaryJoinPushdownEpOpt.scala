package filodb.coordinator.queryplanner.optimize

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import filodb.coordinator.queryplanner.SingleClusterPlanner.findTargetSchema
import filodb.coordinator.queryplanner.optimize.BinaryJoinPushdownEpOpt.JoinSide
import filodb.coordinator.queryplanner.optimize.BinaryJoinPushdownEpOpt.JoinSide.JoinSide
import filodb.core.DatasetRef
import filodb.core.query.QueryContext
import filodb.query.exec.{DistConcatExec, EmptyResultExec, ExecPlan, JoinExecPlan, LabelCardinalityReduceExec,
  LocalPartitionDistConcatExec, MultiSchemaPartitionsExec, NonLeafExecPlan, ReduceAggregateExec,
  SplitLocalPartitionDistConcatExec}

object BinaryJoinPushdownEpOpt {
  /**
   * Specifies the left/right childSide of a join.
   */
  object JoinSide extends Enumeration {
    type JoinSide = Value
    val LHS, RHS = Value
  }
}
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
 *
 */
class BinaryJoinPushdownEpOpt extends ExecPlanOptimizer {

  /**
   * Subtree metadata.
   * @param sameShardOpt occupied with a shard ID iff all of the subtree's data is derived from the same shard.
   * @param targetSchemaColsOpt occupied with a set of target schema columns iff a
   *                            target schema is defined for every leaf of the subtree
   *                            and all subtree join keys are supersets of these columns
   */
  private case class TreeStats(sameShardOpt: Option[Int],
                               targetSchemaColsOpt: Option[Set[String]]) {}

  /**
   * Describes an ExecPlan subtree.
   * @param stats: Immutable **even though ExecPlan updates are possible**.
   *               See propagatePlanUpdate for details.
   * @param parent: Used to propagate grandchildren updates to the root.
   */
  private class Subtree(var root: ExecPlan,
                        val stats: TreeStats,
                        var parent: Option[Subtree] = None) {}

  /**
   * Describes the result of an optimization step.
   * @param aggrStats aggregated TreeStats data (as if all subtrees were children of a no-RVT DistConcat node).
   * @param shardToSubtrees mapping of shards to least-depth subtrees that span a single shard.
   *                        TODO(a_theimer): example
   */
  private case class Result(subtrees: Seq[Subtree],
                            aggrStats: TreeStats,
                            shardToSubtrees: Map[Int, Seq[Subtree]]) {}

  /**
   * Replaces a parent's child plan with another.
   * @param replace the child to replace
   * @param replaceWith the child to use as replacement
   */
  private def replaceChild(parent: ExecPlan, replace: ExecPlan, replaceWith: ExecPlan): ExecPlan = {
    parent match {
      case join: JoinExecPlan => {
        getChildJoinSide(join, replace) match {
          case JoinSide.LHS => {
            val lhsUpd = join.lhs.filterNot(_ eq replace) ++ Seq(replaceWith)
            join.withChildrenBinary(lhsUpd, join.rhs)
          }
          case JoinSide.RHS => {
            val rhsUpd = join.rhs.filterNot(_ eq replace) ++ Seq(replaceWith)
            join.withChildrenBinary(join.lhs, rhsUpd)
          }
        }
      }
      case nl: NonLeafExecPlan => {
        val childrenUpd = nl.children.filterNot(_ eq replace) ++ Seq(replaceWith)
        nl.withChildren(childrenUpd)
      }
      case _ => throw new IllegalArgumentException(s"unexpected parent type: ${parent.getClass}")
    }
  }

  /**
   * Propagates an ExecPlan update up the tree.
   * This does **not** update Subtree TreeStats. Unclear yet whether-or-not this is necessary.
   */
  private def propagatePlanUpdate(subtreeWithOldPlan: Subtree, updPlan: ExecPlan): Unit = {
    var oldPlan = subtreeWithOldPlan.root
    var child = subtreeWithOldPlan
    // set the new plan
    subtreeWithOldPlan.root = updPlan
    // walk up the tree and update plans until the parent is also updated
    while (child.parent != None) {
      val parent = child.parent.get
      val newParentPlan = replaceChild(parent.root, oldPlan, child.root)
      oldPlan = parent.root
      parent.root = newParentPlan
      child = parent
    }
  }

  /**
   * Returns the side of the join occupied by `child`.
   * @param child: immediate child of `parent`
   */
  private def getChildJoinSide(parent: JoinExecPlan, child: ExecPlan): JoinSide = {
    if (parent.lhs.find(_ eq child).isDefined) {
      JoinSide.LHS
    } else if (parent.rhs.find(_ eq child).isDefined) {
      JoinSide.RHS
    } else {
      throw new IllegalArgumentException(s"argument child not found in parent's children")
    }
  }

  // TODO(a_theimer): make JoinInfo class
  /**
   * Returns a sequence of JoinInfos that describe, in order, any join plans encountered along the
   *   path from the child until the parent (non-inclusive of `child`; inclusive of `parent`).
   *
   * @param parent: cannot be same ExecPlan instance wrapped by `child`
   */
  private def getParentJoinInfos(parent: ExecPlan, child: Subtree): Seq[(Subtree, JoinSide)] = {
    // TODO(a_theimer): remove this; this is really just to prevent parent=child ambiguity
    require(!(parent eq child.root))
    var childPtr = child
    var parentPtrOpt = child.parent
    val infos = new mutable.ArrayBuffer[(Subtree, JoinSide)]
    while (parentPtrOpt.isDefined && !(childPtr.root eq parent)) {
      parentPtrOpt.get.root match {
        case plan: JoinExecPlan => infos.append((parentPtrOpt.get, getChildJoinSide(plan, childPtr.root)))
      }
      childPtr = parentPtrOpt.get
      parentPtrOpt = parentPtrOpt.get.parent
    }
    infos
  }

  /**
   * TODO(a_theimer)
   */
  private def canPushdownToLeftGrandchild(join: JoinExecPlan, left: Subtree, right: Subtree): Boolean = {
    val leftJoinInfos = getParentJoinInfos(join, left)
    // TODO(a_theimer): fill in this logic
    false
  }

  /**
   * TODO(a_theimer)
   */
  private def canPushdownToRightGrandchild(join: JoinExecPlan, left: Subtree, right: Subtree): Boolean = {
    val rightJoinInfos = getParentJoinInfos(join, right)
    // TODO(a_theimer): fill in this logic
    false
  }

  // scalastyle:off method.length
  /**
   * TODO(a_theimer): document. Also-- just accept Result args?
   *   Also also: need to review what is/isn't mutable
   *
   * *** Below code is ultra-WIP ***
   */
  private def pushdownToJoinGrandchildren(plan: JoinExecPlan,
                                          lhsRes: Result,
                                          rhsRes: Result): Option[Result] = {
    // TODO(a_theimer): documentation / renames needed down here
    val pushedDown = new mutable.HashSet[Subtree]()
    // make a mutable copy of the shard->subtree maps
    val updMap = new mutable.HashMap[Int, mutable.ArrayBuffer[Subtree]]()
    Seq(lhsRes.shardToSubtrees, rhsRes.shardToSubtrees).flatten.map{ case (shard, subtrees) =>
      val mappedSubtrees = updMap.getOrElseUpdate(shard, new ArrayBuffer[Subtree]())
      mappedSubtrees.appendAll(subtrees)
    }

    val pushToLeftCandidates = lhsRes.subtrees.filter{ leftSubtree =>
      leftSubtree.stats.sameShardOpt.isDefined && rhsRes.shardToSubtrees.contains(leftSubtree.stats.sameShardOpt.get)
    }.flatMap{ leftSubtree =>
      rhsRes.shardToSubtrees(leftSubtree.stats.sameShardOpt.get).map{ rightSubtree =>
        (leftSubtree, rightSubtree)
      }
    }
    val pushToRightCandidates = rhsRes.subtrees.filter{ rightSubtree =>
      rightSubtree.stats.sameShardOpt.isDefined && lhsRes.shardToSubtrees.contains(rightSubtree.stats.sameShardOpt.get)
    }.filter(_.stats.sameShardOpt.isDefined).flatMap{ rightSubtree =>
      lhsRes.shardToSubtrees(rightSubtree.stats.sameShardOpt.get).map{ leftSubtree =>
        (leftSubtree, rightSubtree)
      }
    }

    // TODO(a_theimer): order the pushdowns?
    // TODO(a_theimer): lots of repeat code
    pushToLeftCandidates.foreach{ case (left, right) =>
      if (!pushedDown.contains(right) && canPushdownToLeftGrandchild(plan, left, right)) {
        // make the pushed-down node
        val pushdown = plan.withChildrenBinary(Seq(left.root), Seq(right.root))
        // replace the left node with the pushdown
        val leftParent = left.parent
        val leftParentUpd = replaceChild(leftParent.get.root, left.root, pushdown)
        // update the subtree nodes
        leftParent.get.root = leftParentUpd
        left.root = pushdown
        // remove the right subtree as a pushdown candidate
        pushedDown.add(right)
        // TODO(a_theimer): worth maybe just keeping sets?
        val shardSeq = updMap(right.stats.sameShardOpt.get)
        val indexToRemove = shardSeq.indexOf(right)
        shardSeq(indexToRemove) = shardSeq.last
        shardSeq.dropRight(1)
      }
    }
    pushToRightCandidates.foreach{ case (left, right) =>
      if (!pushedDown.contains(left) && canPushdownToRightGrandchild(plan, left, right)) {
        // make the pushed-down node
        val pushdown = plan.withChildrenBinary(Seq(left.root), Seq(right.root))
        // replace the right node with the pushdown
        val rightParent = right.parent
        val rightParentUpd = replaceChild(rightParent.get.root, left.root, pushdown)
        // update the subtree nodes
        rightParent.get.root = rightParentUpd
        right.root = pushdown
        // remove the right subtree as a pushdown candidate
        pushedDown.add(left)
        val shardSeq = updMap(left.stats.sameShardOpt.get)
        val indexToRemove = shardSeq.indexOf(left)
        shardSeq(indexToRemove) = shardSeq.last
        shardSeq.dropRight(1)
      }
    }

    val resultMap = updMap.map{ case (shard, subtrees) =>
      shard -> subtrees.toSeq
    }.toMap

    // TODO(a_theimer): what if one subtree is exhausted?

    // TODO(a_theimer): these might have changed
    val sameShardOpt = {
      val leftShardOpt = lhsRes.aggrStats.sameShardOpt
      val rightShardOpt = rhsRes.aggrStats.sameShardOpt
      if (leftShardOpt.isDefined && rightShardOpt.isDefined && leftShardOpt.get == rightShardOpt.get) {
        leftShardOpt
      } else {
        None
      }
    }
    val targetSchemaColsOpt = {
      val leftTsOpt = lhsRes.aggrStats.targetSchemaColsOpt
      val rightTsOpt = rhsRes.aggrStats.targetSchemaColsOpt
      if (leftTsOpt.isDefined && rightTsOpt.isDefined) {
        Some(Seq(leftTsOpt, rightTsOpt).flatMap(_.get).toSet)
      } else {
        None
      }
    }
    val stats = TreeStats(sameShardOpt, targetSchemaColsOpt)
    Some(Result(Seq(new Subtree(plan, stats)), stats, resultMap))
  }
  // scalastyle:on method.length

  private def pushdownToJoinChildren(plan: JoinExecPlan,
                                     lhsRes: Result,
                                     rhsRes: Result): Option[Result] = {
    val childSubtrees = Seq(lhsRes, rhsRes).flatMap(_.subtrees)
    // make sure all child subtrees have all leaf-level target schema columns defined and present
    if (childSubtrees.forall(_.stats.targetSchemaColsOpt.isDefined)) {
      val targetSchemaColsUnion = childSubtrees.flatMap(_.stats.targetSchemaColsOpt.get).toSet
      // make sure all target schema cols present in join keys
      // TODO(a_theimer): this is not technically correct; combines on-empty and both-empty cases
      val alltargetSchemaColsPresent = if (plan.on.isEmpty) {
        // make sure no target schema strings are ignored
        plan.ignoring.find(targetSchemaColsUnion.contains(_)).isEmpty
      } else {
        // make sure all target schema cols are included in on
        targetSchemaColsUnion.forall(plan.on.toSet.contains(_))
      }
      if (alltargetSchemaColsPresent) {
        return makePushdownJoins(plan, lhsRes, rhsRes, plan.queryContext, plan.dataset)
      }
    }
    endOptimization(plan.withChildrenBinary(lhsRes.subtrees.map(_.root), rhsRes.subtrees.map(_.root)))
  }

  /**
   * Returns a Result where each argument ExecPlan is individually optimized.
   */
  private def optimizePlans(plans: Seq[ExecPlan]): Option[Result] = {
    // ignore any empty results
    val results = plans.map(optimizeWalker(_)).filter(_.isDefined).map(_.get)
    if (results.isEmpty) {
      return None
    }
    val optimizedPlans = results.map(_.subtrees).flatten
    val shardToSubtrees = results.map(_.shardToSubtrees).foldLeft(
      new mutable.HashMap[Int, Seq[Subtree]]()){ case (acc, map) =>
      map.foreach{ case (k, v) =>
        val exist = acc.getOrElse(k, Nil)
        acc.put(k, v ++ exist)
      }
      acc
    }
    val sameShardOpt = {
      val shardSet = results.map(_.aggrStats.sameShardOpt).toSet
      if (shardSet.size == 1) {
        shardSet.head
      } else {
        None
      }
    }
    val targetSchemaColsOpt = {
      val opts = results.map(_.aggrStats.targetSchemaColsOpt)
      if (opts.forall(_.isDefined)) {
        Some(opts.flatMap(_.get).toSet)
      } else {
        None
      }
    }
    Some(Result(optimizedPlans, TreeStats(sameShardOpt, targetSchemaColsOpt), shardToSubtrees.toMap))
  }

  // TODO(a_theimer): cleanup
  private def optimizeAggregate(plan: ReduceAggregateExec): Option[Result] = {
    val childrenResultOpt = optimizePlans(plan.children)
    if (childrenResultOpt.isEmpty) {
      return None
    }
    val childrenResult = childrenResultOpt.get
    val optimizedPlan = plan.withChildren(childrenResult.subtrees.map(_.root))
    if (childrenResult.aggrStats.sameShardOpt.isDefined) {
      val stats = TreeStats(childrenResult.aggrStats.sameShardOpt,
                            childrenResult.aggrStats.targetSchemaColsOpt)
      val subtree = new Subtree(optimizedPlan, stats)
      val shardToSubtrees = Map(childrenResult.aggrStats.sameShardOpt.get -> Seq(subtree))
      Some(Result(Seq(subtree), stats, shardToSubtrees))
    } else {
      // TODO(a_theimer): double-check this
      endOptimization(optimizedPlan)
    }
  }

  private def optimizeConcat(plan: DistConcatExec): Option[Result] = {
    val childrenResultOpt = optimizePlans(plan.children)
    if (childrenResultOpt.isEmpty) {
      return None
    }
    val childrenResult = childrenResultOpt.get
    if (plan.rangeVectorTransformers.isEmpty ) {
      childrenResultOpt
    } else {
      // TODO: continue optimization when RVT's don't combine data across shards
      // end optimization for this subtree
      endOptimization(plan.withChildren(childrenResult.subtrees.map(_.root)))
    }
  }

  // scalastyle:off method.length
  /**
   * Creates the "pushed-down" join plans.
   */
  private def makePushdownJoins(originalPlan: JoinExecPlan,
                                lhsRes: Result,
                                rhsRes: Result,
                                queryContext: QueryContext,
                                dataset: DatasetRef): Option[Result] = {
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
      return None
    }

    // make the pushed-down join subtrees
    val shardToSubtrees = new mutable.HashMap[Int, Seq[Subtree]]
    val pushdownSubtrees = joinPairs.map{ case (shard, (lhsSubtree, rhsSubtree)) =>
      val pushdownJoinPlan = originalPlan.withChildrenBinary(Seq(lhsSubtree.root), Seq(rhsSubtree.root))
      val targetSchemaColUnion =
        lhsSubtree.stats.targetSchemaColsOpt.get.union(rhsSubtree.stats.targetSchemaColsOpt.get)
      val stats = TreeStats(Some(shard), Some(targetSchemaColUnion))
      val pushdownJoinSubtree = new Subtree(pushdownJoinPlan, stats)
      shardToSubtrees(shard) = Seq(pushdownJoinSubtree)
      pushdownJoinSubtree
    }.toSeq

    // TODO(a_theimer): have Result automatically aggregate over its subtrees?
    val sameShardOpt = if (shardToSubtrees.size == 1) {
      Some(shardToSubtrees.head._1)
    } else {
      None
    }
    val targetSchemaColsOpt = if (pushdownSubtrees.forall(_.stats.targetSchemaColsOpt.isDefined)) {
      Some(pushdownSubtrees.flatMap(_.stats.targetSchemaColsOpt.get).toSet)
    } else {
      None
    }
    Some(Result(pushdownSubtrees, TreeStats(sameShardOpt, targetSchemaColsOpt), shardToSubtrees.toMap))
  }
  // scalastyle:on method.length

  /**
   * TODO(a_theimer): document
   * @param copyJoinPlanWithChildren accepts a lhs and rhs, then returns a copy of the
   *                                 join plan with these as children.
   */
  private def optimizeJoin(plan: JoinExecPlan): Option[Result] = {
    val lhsResOpt = optimizePlans(plan.lhs)
    val rhsResOpt = optimizePlans(plan.rhs)
    if (lhsResOpt.isEmpty || rhsResOpt.isEmpty) {
      // TODO(a_theimer): invalid for 'unless'?
      return None
    }
    val lhsRes = lhsResOpt.get
    val rhsRes = rhsResOpt.get
    val childrenPushdownOpt = pushdownToJoinChildren(plan, lhsRes, rhsRes)
    if (childrenPushdownOpt.isEmpty || !(childrenPushdownOpt.get.subtrees.head.root eq plan)) {
      // can't do any better than this-- return this result
      return childrenPushdownOpt
    }
    // otherwise, see if there are any possible pushdowns beyond immediate children (more complicated and less optimal)
    pushdownToJoinGrandchildren(plan, lhsRes, rhsRes)
  }

  private def optimizeMultiSchemaPartitionsExec(plan: MultiSchemaPartitionsExec): Option[Result] = {
    // get the target schema columns (if they exist)
    val targetSchemaColsOpt = plan.queryContext.plannerParams.targetSchemaProvider.map { provider =>
      val changes = provider.targetSchemaFunc(plan.filters)
      val startMs = plan.chunkMethod.startTime
      val endMs = plan.chunkMethod.endTime
      findTargetSchema(changes, startMs, endMs).map(_.schema.toSet)
    }.filter(_.isDefined).map(_.get)
    val stats = TreeStats(Some(plan.shard), targetSchemaColsOpt)
    val subtree = new Subtree(plan, stats)
    Some(Result(Seq(subtree), stats, Map(plan.shard -> Seq(subtree))))
  }

  /**
   * Ends all optimization for the subtree.
   */
  private def endOptimization(plan: ExecPlan): Option[Result] = {
    val stats = TreeStats(None, None)
    Some(Result(Seq(new Subtree(plan, stats)), stats, Map()))
  }

  /**
   * Calls the optimizer function specific to the plan type.
   */
  private def optimizeWalker(plan: ExecPlan): Option[Result] = {
    plan match {
      // TODO(a_theimer): need to handle these vvvvvvvvvvvvvvvvvvvv
      case plan: SplitLocalPartitionDistConcatExec => endOptimization(plan)
      case plan: LabelCardinalityReduceExec => endOptimization(plan)
      // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      case plan: JoinExecPlan => optimizeJoin(plan)
      case plan: ReduceAggregateExec => optimizeAggregate(plan)
      case plan: DistConcatExec => optimizeConcat(plan)
      case plan: MultiSchemaPartitionsExec => optimizeMultiSchemaPartitionsExec(plan)
      case plan => endOptimization(plan)
    }
  }

  override def optimize(plan: ExecPlan): ExecPlan = {
    val resOpt = optimizeWalker(plan)
    if (resOpt.isDefined) {
      val res = resOpt.get
      res.subtrees.size match {
        case 0 => EmptyResultExec(plan.queryContext, plan.dataset)
        case 1 => res.subtrees.head.root
        case _ => LocalPartitionDistConcatExec(plan.queryContext, plan.dispatcher, res.subtrees.map(_.root))
      }
    } else {
      EmptyResultExec(plan.queryContext, plan.dataset)
    }
  }
}
