package filodb.coordinator.queryplanner

import scala.concurrent.duration._
import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.core.{SpreadProvider, StaticTargetSchemaProvider, TargetSchemaChange, TargetSchemaProvider}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.{Dataset, DatasetOptions, Schemas}
import filodb.core.query._
import filodb.core.query.Filter.Equals
import filodb.core.store.{AllChunkScan, ChunkScanMethod, InMemoryChunkScan, TimeRangeChunkScan, WriteBufferChunkScan}
import filodb.prometheus.ast.Vectors.{PromMetricLabel, TypeLabel}
import filodb.prometheus.ast.WindowConstants
import filodb.query.{exec, _}
import filodb.query.InstantFunctionId.HistogramBucket
import filodb.query.LogicalPlan._
import filodb.query.exec.{LocalPartitionDistConcatExec, _}
import filodb.query.exec.InternalRangeFunction.Last

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SingleClusterPlanner {
  private val mdNoShardKeyFilterRequests = Kamon.counter("queryengine-metadata-no-shardkey-requests").withoutTags
}

/**
  * Responsible for query planning within single FiloDB cluster
  * @param schemas          schema instance, used to extract partKey schema
  * @param spreadProvider  used to get spread
  * @param shardMapperFunc used to get shard locality
  * @param timeSplitEnabled split based on longer time range
  * @param minTimeRangeForSplitMs if time range is longer than this, zplan will be split into multiple plans
  * @param splitSizeMs time range for each split, if plan needed to be split
  */

class SingleClusterPlanner(val dataset: Dataset,
                           val schemas: Schemas,
                           shardMapperFunc: => ShardMapper,
                           earliestRetainedTimestampFn: => Long,
                           val queryConfig: QueryConfig,
                           clusterName: String,
                           spreadProvider: SpreadProvider = StaticSpreadProvider(),
                           targetSchemaProvider: TargetSchemaProvider = StaticTargetSchemaProvider(),
                           timeSplitEnabled: Boolean = false,
                           minTimeRangeForSplitMs: => Long = 1.day.toMillis,
                           splitSizeMs: => Long = 1.day.toMillis)
                           extends QueryPlanner with StrictLogging with DefaultPlanner {
  override val dsOptions: DatasetOptions = schemas.part.options
  private val shardColumns = dsOptions.shardKeyColumns.sorted
  private val dsRef = dataset.ref

  import SingleClusterPlanner._

  private def dispatcherForShard(shard: Int, forceInProcess: Boolean): PlanDispatcher = {
    if (forceInProcess) {
      return inProcessPlanDispatcher
    }
    val targetActor = shardMapperFunc.coordForShard(shard)
    if (targetActor == ActorRef.noSender) {
      logger.debug(s"ShardMapper: $shardMapperFunc")
      throw new RuntimeException(s"Shard: $shard is not available") // TODO fix this
    }
    ActorPlanDispatcher(targetActor, clusterName)
  }

  /**
   * Change start time of logical plan according to earliestRetainedTimestampFn
   */
  private def updateStartTime(logicalPlan: LogicalPlan): Option[LogicalPlan] = {
    val periodicSeriesPlan = LogicalPlanUtils.getPeriodicSeriesPlan(logicalPlan)
    if (periodicSeriesPlan.isEmpty) Some(logicalPlan)
    else {
      //For binary join LHS & RHS should have same times
      val boundParams = periodicSeriesPlan.get.head match {
        case p: PeriodicSeries => (p.startMs, p.stepMs, WindowConstants.staleDataLookbackMillis,
          p.offsetMs.getOrElse(0L), p.endMs)
        case w: PeriodicSeriesWithWindowing => (w.startMs, w.stepMs, w.window, w.offsetMs.getOrElse(0L), w.endMs)
        case _  => throw new UnsupportedOperationException(s"Invalid plan: ${periodicSeriesPlan.get.head}")
      }

      val newStartMs = boundToStartTimeToEarliestRetained(boundParams._1, boundParams._2, boundParams._3,
        boundParams._4)
      if (newStartMs <= boundParams._5) { // if there is an overlap between query and retention ranges
        if (newStartMs != boundParams._1)
          Some(LogicalPlanUtils.copyLogicalPlanWithUpdatedTimeRange(logicalPlan, TimeRange(newStartMs, boundParams._5)))
        else Some(logicalPlan)
      } else { // query is outside retention period, simply return empty result
        None
      }
    }
  }

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    val plannerParams = qContext.plannerParams
    val updatedPlan = updateStartTime(logicalPlan)
    if (updatedPlan.isEmpty) EmptyResultExec(qContext, dsRef)
    else {
     val logicalPlan = updatedPlan.get
      val timeSplitConfig = if (plannerParams.timeSplitEnabled)
        (plannerParams.timeSplitEnabled, plannerParams.minTimeRangeForSplitMs, plannerParams.splitSizeMs)
      else (timeSplitEnabled, minTimeRangeForSplitMs, splitSizeMs)

      if (shardMapperFunc.numShards <= 0) throw new IllegalStateException("No shards available")
      val logicalPlans = if (updatedPlan.get.isInstanceOf[PeriodicSeriesPlan])
        LogicalPlanUtils.splitPlans(updatedPlan.get, qContext, timeSplitConfig._1,
          timeSplitConfig._2, timeSplitConfig._3)
      else
        Seq(logicalPlan)
      val materialized = logicalPlans match {
        case Seq(one) => materializeTimeSplitPlan(one, qContext, false)
        case many =>
          val materializedPlans = many.map(materializeTimeSplitPlan(_, qContext, false))
          val targetActor = PlannerUtil.pickDispatcher(materializedPlans)

          // create SplitLocalPartitionDistConcatExec that will execute child execplanss sequentially and stitches
          // results back with StitchRvsMapper transformer.
          val stitchPlan = SplitLocalPartitionDistConcatExec(qContext, targetActor, materializedPlans,
            rvRangeFromPlan(logicalPlan))
          stitchPlan
      }
      logger.debug(s"Materialized logical plan for dataset=$dsRef :" +
        s" $logicalPlan to \n${materialized.printTree()}")
      materialized
    }
  }

  private def materializeTimeSplitPlan(logicalPlan: LogicalPlan,
                                       qContext: QueryContext,
                                       forceInProcess: Boolean): ExecPlan = {
    val materialized = walkLogicalPlanTree(logicalPlan, qContext, forceInProcess)
    match {
      case PlanResult(Seq(justOne), stitch) =>
        if (stitch) justOne.addRangeVectorTransformer(StitchRvsMapper(rvRangeFromPlan(logicalPlan)))
        justOne
      case PlanResult(many, stitch) =>
        val targetActor = PlannerUtil.pickDispatcher(many)
        many.head match {
          case _: LabelValuesExec => LabelValuesDistConcatExec(qContext, targetActor, many)
          case _: LabelNamesExec => LabelNamesDistConcatExec(qContext, targetActor, many)
          case _: TsCardExec => TsCardReduceExec(qContext, targetActor, many)
          case _: LabelCardinalityExec =>
            val reduceExec = LabelCardinalityReduceExec(qContext, targetActor, many)
            // Presenter here is added separately which use the bytes representing the sketch to get an estimate
            // of cardinality. The presenter is kept separate from DistConcatExec to enable multi partition queries
            // later if needed. The DistConcatExec's from multiple partitions can still return the map of label names
            // and bytes and they can be merged to create a new sketch. Only the top level exec needs to then add the
            // presenter to display the final mapping of label name and the count based on the sketch bytes.
            if (!qContext.plannerParams.skipAggregatePresent) {
              reduceExec.addRangeVectorTransformer(new LabelCardinalityPresenter())
            }
            reduceExec
          case _: PartKeysExec => PartKeysDistConcatExec(qContext, targetActor, many)
          case _: ExecPlan =>
            val topPlan = LocalPartitionDistConcatExec(qContext, targetActor, many)
            if (stitch) topPlan.addRangeVectorTransformer(StitchRvsMapper(rvRangeFromPlan(logicalPlan)))
            topPlan
        }
    }
    logger.debug(s"Materialized logical plan for dataset=$dsRef :" +
      s" $logicalPlan to \n${materialized.printTree()}")
    materialized
  }

  def shardsFromFilters(filters: Seq[ColumnFilter],
                        qContext: QueryContext,
                        useTargetSchemaForShards: Boolean = false): Seq[Int] = {

    val spreadProvToUse = qContext.plannerParams.spreadOverride.getOrElse(spreadProvider)

    require(shardColumns.nonEmpty || qContext.plannerParams.shardOverrides.nonEmpty,
      s"Dataset $dsRef does not have shard columns defined, and shard overrides were not mentioned")

    qContext.plannerParams.shardOverrides.getOrElse {
      val shardVals = shardColumns.map { shardCol =>
        // So to compute the shard hash we need shardCol == value filter (exact equals) for each shardColumn
        filters.find(f => f.column == shardCol) match {
          case Some(ColumnFilter(_, Filter.Equals(filtVal: String))) =>
            shardCol -> RecordBuilder.trimShardColumn(dsOptions, shardCol, filtVal)
          case Some(ColumnFilter(_, filter)) =>
            throw new BadQueryException(s"Found filter for shard column $shardCol but " +
              s"$filter cannot be used for shard key routing")
          case _ =>
            throw new BadQueryException(s"Could not find filter for shard key column " +
              s"$shardCol, shard key hashing disabled")
        }
      }
      val metric = shardVals.find(_._1 == dsOptions.metricColumn)
        .map(_._2)
        .getOrElse(throw new BadQueryException(s"Could not find metric value"))
      val shardValues = shardVals.filterNot(_._1 == dsOptions.metricColumn).map(_._2)
      logger.debug(s"For shardColumns $shardColumns, extracted metric $metric and shard values $shardValues")
      val targetSchemaChange = qContext.plannerParams.targetSchema
        .getOrElse(targetSchemaProvider)
        .targetSchemaFunc(filters)
      val targetSchema = if (targetSchemaChange.nonEmpty) targetSchemaChange.last.schema else Seq.empty
      val shardHash = RecordBuilder.shardKeyHash(shardValues, dsOptions.metricColumn, metric, targetSchema)
      if(useTargetSchemaForShards) {
        val nonShardKeyLabelPairs = filters.filter(f => !shardColumns.contains(f.column)
                                                          && f.filter.isInstanceOf[Filter.Equals])
                                            .map(cf => cf.column ->
                                              cf.filter.asInstanceOf[Filter.Equals].value.toString).toMap
        val partitionHash = RecordBuilder.partitionKeyHash(nonShardKeyLabelPairs, shardVals.toMap, targetSchema,
          dsOptions.metricColumn, metric)
        // since target-schema filter is provided in the query, ingestionShard can be used to find the single shard
        // that can answer the query.
        Seq(shardMapperFunc.ingestionShard(shardHash, partitionHash, spreadProvToUse.spreadFunc(filters).last.spread))
      } else {
        shardMapperFunc.queryShards(shardHash, spreadProvToUse.spreadFunc(filters).last.spread)
      }
    }
  }

  // Is TargetSchema changing during query window.
  private def isTargetSchemaChanging(targetSchemaChanges: Seq[TargetSchemaChange],
                                     startMs: Long, endMs: Long): Boolean =
    targetSchemaChanges.nonEmpty && targetSchemaChanges.exists(c => c.time >= startMs && c.time <= endMs)

  // Find the TargetSchema that is applicable i.e effective for the current query window
  private def findTargetSchema(targetSchemaChanges: Seq[TargetSchemaChange],
                               startMs: Long, endMs: Long): Option[TargetSchemaChange] = {
    val tsIndex = targetSchemaChanges.lastIndexWhere(t => t.time <= startMs)
    if(tsIndex > -1)
      Some(targetSchemaChanges(tsIndex))
    else
      None
  }

  /**
   * If TargetSchema exists and all of the target-schema label filters (equals) are provided in the query,
   * then return true.
   * @param filters Query Column Filters
   * @param targetSchema TargetSchema
   * @return useTargetSchema - use target-schema to calculate query shards
   */
  private def useTargetSchemaForShards(filters: Seq[ColumnFilter], targetSchema: Option[TargetSchemaChange]): Boolean =
    targetSchema match {
      case Some(ts) if ts.schema.nonEmpty => ts.schema
        .forall(s => filters.exists(cf => cf.column == s && cf.filter.isInstanceOf[Filter.Equals]))
      case _ => false
    }


  private def toChunkScanMethod(rangeSelector: RangeSelector): ChunkScanMethod = {
    rangeSelector match {
      case IntervalSelector(from, to) => TimeRangeChunkScan(from, to)
      case AllChunksSelector          => AllChunkScan
      case WriteBufferSelector        => WriteBufferChunkScan
      case InMemoryChunksSelector     => InMemoryChunkScan
      case x @ _                      => throw new IllegalArgumentException(s"Unsupported range selector '$x' found")
    }
  }

  /**
    * Renames Prom AST __name__ metric name filters to one based on the actual metric column of the dataset,
    * if it is not the prometheus standard
    */
  def renameMetricFilter(filters: Seq[ColumnFilter]): Seq[ColumnFilter] =
    if (dsOptions.metricColumn != PromMetricLabel) {
      filters map {
        case ColumnFilter(PromMetricLabel, filt) => ColumnFilter(dsOptions.metricColumn, filt)
        case other: ColumnFilter                 => other
      }
    } else {
      filters
    }

  /**
    * Walk logical plan tree depth-first and generate execution plans starting from the bottom
    *
    * @return ExecPlans that answer the logical plan provided
    */
  // scalastyle:off cyclomatic.complexity
  override def walkLogicalPlanTree(logicalPlan: LogicalPlan,
                                  qContext: QueryContext,
                                   forceInProcess: Boolean): PlanResult = {
     logicalPlan match {
      case lp: RawSeries                   => materializeRawSeries(qContext, lp, forceInProcess)
      case lp: RawChunkMeta                => materializeRawChunkMeta(qContext, lp, forceInProcess)
      case lp: PeriodicSeries              => materializePeriodicSeries(qContext, lp, forceInProcess)
      case lp: PeriodicSeriesWithWindowing => materializePeriodicSeriesWithWindowing(qContext, lp, forceInProcess)
      case lp: ApplyInstantFunction        => materializeApplyInstantFunction(qContext, lp, forceInProcess)
      case lp: ApplyInstantFunctionRaw     => materializeApplyInstantFunctionRaw(qContext, lp, forceInProcess)
      case lp: Aggregate                   => materializeAggregate(qContext, lp, forceInProcess)
      case lp: BinaryJoin                  => materializeBinaryJoin(qContext, lp, forceInProcess)
      case lp: ScalarVectorBinaryOperation => materializeScalarVectorBinOp(qContext, lp, forceInProcess)
      case lp: LabelValues                 => materializeLabelValues(qContext, lp, forceInProcess)
      case lp: LabelNames                  => materializeLabelNames(qContext, lp, forceInProcess)
      case lp: TsCardinalities             => materializeTsCardinalities(qContext, lp, forceInProcess)
      case lp: SeriesKeysByFilters         => materializeSeriesKeysByFilters(qContext, lp, forceInProcess)
      case lp: ApplyMiscellaneousFunction  => materializeApplyMiscellaneousFunction(qContext, lp, forceInProcess)
      case lp: ApplySortFunction           => materializeApplySortFunction(qContext, lp, forceInProcess)
      case lp: ScalarVaryingDoublePlan     => materializeScalarPlan(qContext, lp, forceInProcess)
      case lp: ScalarTimeBasedPlan         => materializeScalarTimeBased(qContext, lp)
      case lp: VectorPlan                  => materializeVectorPlan(qContext, lp, forceInProcess)
      case lp: ScalarFixedDoublePlan       => materializeFixedScalar(qContext, lp)
      case lp: ApplyAbsentFunction         => materializeAbsentFunction(qContext, lp, forceInProcess)
      case lp: ApplyLimitFunction          => materializeLimitFunction(qContext, lp, forceInProcess)
      case lp: ScalarBinaryOperation       => materializeScalarBinaryOperation(qContext, lp, forceInProcess)
      case lp: SubqueryWithWindowing       => materializeSubqueryWithWindowing(qContext, lp, forceInProcess)
      case lp: TopLevelSubquery            => materializeTopLevelSubquery(qContext, lp, forceInProcess)
      case lp: LabelCardinality            => materializeLabelCardinality(qContext, lp, forceInProcess)
      //case _                               => throw new BadQueryException("Invalid logical plan")
    }
  }
  // scalastyle:on cyclomatic.complexity

  // TODO(a_theimer): rename
  private def materializeBinaryJoinOld(qContext: QueryContext,
                                     lp: BinaryJoin,
                                       forceInProcess: Boolean): PlanResult = {
    val lhs = walkLogicalPlanTree(lp.lhs, qContext, forceInProcess)
    val stitchedLhs = if (lhs.needsStitch) Seq(StitchRvsExec(qContext,
      PlannerUtil.pickDispatcher(lhs.plans), rvRangeFromPlan(lp), lhs.plans))
    else lhs.plans
    val rhs = walkLogicalPlanTree(lp.rhs, qContext, forceInProcess)
    val stitchedRhs = if (rhs.needsStitch) Seq(StitchRvsExec(qContext,
      PlannerUtil.pickDispatcher(rhs.plans), rvRangeFromPlan(lp), rhs.plans))
    else rhs.plans

    // TODO Currently we create separate exec plan node for stitching.
    // Ideally, we can go one step further and add capability to NonLeafNode plans to pre-process
    // and transform child results individually before composing child results together.
    // In theory, more efficient to use transformer than to have separate exec plan node to avoid IO.
    // In the interest of keeping it simple, deferring decorations to the ExecPlan. Add only if needed after measuring.

    val targetActor = PlannerUtil.pickDispatcher(stitchedLhs ++ stitchedRhs)
    val joined = if (lp.operator.isInstanceOf[SetOperator])
      Seq(exec.SetOperatorExec(qContext, targetActor, stitchedLhs, stitchedRhs, lp.operator,
        LogicalPlanUtils.renameLabels(lp.on, dsOptions.metricColumn),
        LogicalPlanUtils.renameLabels(lp.ignoring, dsOptions.metricColumn), dsOptions.metricColumn,
        rvRangeFromPlan(lp)))
    else
      Seq(BinaryJoinExec(qContext, targetActor, stitchedLhs, stitchedRhs, lp.operator, lp.cardinality,
        LogicalPlanUtils.renameLabels(lp.on, dsOptions.metricColumn),
        LogicalPlanUtils.renameLabels(lp.ignoring, dsOptions.metricColumn),
        LogicalPlanUtils.renameLabels(lp.include, dsOptions.metricColumn), dsOptions.metricColumn, rvRangeFromPlan(lp)))

    PlanResult(joined)
  }

  // TODO(a_theimer): refactor/rename/move
  private def getRawSeriesTargetSchemaCols(lp: RawSeries,
                                           tsp: TargetSchemaProvider): Seq[String] = {
    // TODO(a_theimer): declaration required?
    val opt: Option[(Long, Long)] = lp.rangeSelector match {
      // TODO(a_theimer): other selectors
      // TODO(a_theimer): label and double-check units
      case IntervalSelector(from, to) => Some((from, to))
      case _ => None
    }
    opt.map{ case (from, to) =>
      val changes = tsp.targetSchemaFunc(lp.filters)
      findTargetSchema(changes, from, to).map(_.schema).getOrElse(Nil)
    }.getOrElse(Nil)
  }

  // TODO(a_theimer): tsp should be removed (and field/arg tsp should be reconciled)
  private def getTargetSchemaCols(lp: LogicalPlan,
                                  tsp: TargetSchemaProvider): Seq[Seq[String]] = {
    lp match {
      case rs: RawSeries => Seq(getRawSeriesTargetSchemaCols(rs, tsp))
      case nl: NonLeafLogicalPlan => nl.children.flatMap(getTargetSchemaCols(_, tsp))
      case _ => throw new RuntimeException("TODO(a_theimer)")
    }
  }

  // TODO(a_theimer): move/rename/refactor
  private def joinColsPreserveTargetSchema(lp: LogicalPlan,
                                           tsCols: Seq[String]): Boolean = {
    lp match {
      case join: BinaryJoin => {
        // TODO(a_theimer): probably pass sets around instead of seqs
        val noneIgnored = tsCols.toSet.intersect(join.ignoring.toSet).isEmpty
        // TODO(a_theimer): included/on?
        val allIncluded = tsCols.toSet.subsetOf(join.on.toSet)
        if (noneIgnored && allIncluded) {
          joinColsPreserveTargetSchema(join.lhs, tsCols) && joinColsPreserveTargetSchema(join.rhs, tsCols)
        } else false
      }
      case nl: NonLeafLogicalPlan => {
        nl.children.forall(joinColsPreserveTargetSchema(_, tsCols))
      }
      case _ => {
        // TODO(a_theimer): add LeafLogicalPlan type?
        // must be a leaf plan, since above case did not trip
        true
      }
    }
  }


  // TODO(a_theimer): rename/refactor/document
  private def materializeBinaryJoinPushdown(qContext: QueryContext,
                                            lp: BinaryJoin,
                                            forceInProcess: Boolean): PlanResult = {
    // TODO(a_theimer): probably a bunch of assertions here
    val forceInProcessUpd = forceInProcess || sameShardLP(lp, qContext, qContext.plannerParams.targetSchema).isDefined
    val lhs = walkLogicalPlanTree(lp.lhs, qContext, forceInProcessUpd)
    val rhs = walkLogicalPlanTree(lp.rhs, qContext, forceInProcessUpd)

    // TODO(a_theimer): remove this and/or handle stitching here
    assert(!lhs.needsStitch && !rhs.needsStitch)

    // TODO(a_theimer): stitch? (also probably doesn't need separate method)
    PlanResult(makePushdownJoins(lp, lhs.plans, rhs.plans, qContext))
  }

  // TODO(a_theimer): rename/reconcile/refactor
  private def sameShardLP(lp: LogicalPlan,
                          qContext: QueryContext,
                          tsProvider: Option[TargetSchemaProvider]): Option[Int] = {
    val leafFilterPairs = findLeafLogicalPlans(lp).map{ leaf =>
      // TODO(a_theimer): rename these?
      val filters = getColumnFilterGroup(leaf)
      assert(filters.size == 1)
      (leaf, filters.head)
    }

    lazy val canGetShardsFromFilts = leafFilterPairs.forall{ case (_, filters) =>
      canGetShardsFromFilters(filters.toSeq, qContext)
    }

    // TODO(a_theimer): rename / reconcile expr
    lazy val useTargetSchemaFS = tsProvider.isDefined && leafFilterPairs.forall{ case (leaf, filters) =>
      val targetSchema = tsProvider.get.targetSchemaFunc(filters.toSeq)
      val (start, stop) = leaf match {
        case rs: RawSeries => rs.rangeSelector match {
          case is: IntervalSelector => (is.from, is.to)
          case _ => throw new IllegalArgumentException("TODO(a_theimer)")
        }
        case _ => throw new IllegalArgumentException("TODO(a_theimer)")
      }
      findTargetSchema(targetSchema, start, stop).isDefined
    }

    if (!canGetShardsFromFilts || !useTargetSchemaFS) {
      return None
    }

    val shards = leafFilterPairs.flatMap{case (leaf, filters) =>
      shardsFromFilters(filters.toSeq, qContext, useTargetSchemaFS)
    }

    if (shards.drop(1).forall(_ == shards.head)) {
      Some(shards.head)
    } else None
  }

  // TODO(a_theimer): rename/refactor/document
  private def sameShard(ep: ExecPlan): Option[Int] = {
    ep match {
      case mspe: MultiSchemaPartitionsExec => Some(mspe.shard)
      case nl: NonLeafExecPlan => {
        val refShard = sameShard(nl.children.head)
        if (refShard.isDefined){
          val allChildrenSame = nl.children.drop(1).map(sameShard(_)).find(_ != refShard).isEmpty
          if (allChildrenSame){
            return refShard
          }
        }
        None
      }
      case _ => throw new RuntimeException("TODO(a_theimer)")
    }
  }

  // TODO(a_theimer)
  // scalastyle:off
  private def makePushdownJoins(plan: BinaryJoin,
                                lhs: Seq[ExecPlan],
                                rhs: Seq[ExecPlan],
                                queryContext: QueryContext): Seq[ExecPlan] = {

    val lhsShardMap = new mutable.HashMap[Int, mutable.ArrayBuffer[ExecPlan]]
    val rhsShardMap = new mutable.HashMap[Int, mutable.ArrayBuffer[ExecPlan]]
    lhs.map(ep => (ep, sameShard(ep)))
      .filter(_._2.isDefined)
      .foreach{ case (plan, shard) =>
        lhsShardMap.getOrElseUpdate(shard.get, new ArrayBuffer[ExecPlan]()).append(plan)
      }
    rhs.map(ep => (ep, sameShard(ep)))
      .filter(_._2.isDefined)
      .foreach{ case (plan, shard) =>
        rhsShardMap.getOrElseUpdate(shard.get, new ArrayBuffer[ExecPlan]()).append(plan)
      }

    val joinPairs = lhsShardMap.keySet.intersect(rhsShardMap.keySet).map{ shard =>
      (lhsShardMap(shard), rhsShardMap(shard))
    }.toSeq

    // TODO(a_theimer): stitch lhs/rhs?
    joinPairs.map{ case (lhsPlans, rhsPlans) =>
      val targetActor = PlannerUtil.pickDispatcher(lhsPlans ++ rhsPlans)
      plan.operator match {
        case _: SetOperator => exec.SetOperatorExec(queryContext, targetActor, lhsPlans, rhsPlans, plan.operator,
          LogicalPlanUtils.renameLabels(plan.on, dsOptions.metricColumn),
          LogicalPlanUtils.renameLabels(plan.ignoring, dsOptions.metricColumn), dsOptions.metricColumn,
          rvRangeFromPlan(plan))
        case _ => BinaryJoinExec(queryContext, targetActor, lhsPlans, rhsPlans, plan.operator, plan.cardinality,
          LogicalPlanUtils.renameLabels(plan.on, dsOptions.metricColumn),
          LogicalPlanUtils.renameLabels(plan.ignoring, dsOptions.metricColumn),
          LogicalPlanUtils.renameLabels(plan.include, dsOptions.metricColumn), dsOptions.metricColumn, rvRangeFromPlan(plan))
      }
    }
  }


  override def materializeBinaryJoin(qContext: QueryContext,
                                     lp: BinaryJoin,
                                     forceInProcess: Boolean): PlanResult = {
    // (1) get target schemas for all leaves. If all match, proceed.
    if (qContext.plannerParams.targetSchema.isDefined) {
      val targetSchemas = getTargetSchemaCols(lp, qContext.plannerParams.targetSchema.get)
      lazy val allSame = {
        val refColSet = targetSchemas.head.toSet
        // TODO(a_theimer): does column order matter?
        targetSchemas.drop(1).find(_.toSet != refColSet).isDefined
      }
      // (2) make sure all child joins preserve ts columns. If yes, proceed.
      // (3) [after 1 and 2]: figure out how to implement CommonDispatcherOpt TODO(a_theimer)
      lazy val colsPreserved = joinColsPreserveTargetSchema(lp, targetSchemas.head)
      if (allSame && colsPreserved) {
        return materializeBinaryJoinPushdown(qContext, lp, forceInProcess)
      }
    }
    materializeBinaryJoinOld(qContext, lp, forceInProcess)
  }

  private def materializePeriodicSeriesWithWindowing(qContext: QueryContext,
                                                     lp: PeriodicSeriesWithWindowing,
                                                     forceInProcess: Boolean): PlanResult = {
    val logicalPlanWithoutBucket = if (queryConfig.translatePromToFilodbHistogram) {
       removeBucket(Right(lp))._3.right.get
    } else lp

    val series = walkLogicalPlanTree(logicalPlanWithoutBucket.series, qContext, forceInProcess)
    val rawSource = logicalPlanWithoutBucket.series.isRaw

    /* Last function is used to get the latest value in the window for absent_over_time
    If no data is present AbsentFunctionMapper will return range vector with value 1 */

    val execRangeFn = if (logicalPlanWithoutBucket.function == RangeFunctionId.AbsentOverTime) Last
                      else InternalRangeFunction.lpToInternalFunc(logicalPlanWithoutBucket.function)

    val paramsExec = materializeFunctionArgs(logicalPlanWithoutBucket.functionArgs, qContext)
    val window = if (execRangeFn == InternalRangeFunction.Timestamp) None else Some(logicalPlanWithoutBucket.window)
    series.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(logicalPlanWithoutBucket.startMs,
      logicalPlanWithoutBucket.stepMs, logicalPlanWithoutBucket.endMs, window, Some(execRangeFn), qContext,
      logicalPlanWithoutBucket.stepMultipleNotationUsed,
      paramsExec, logicalPlanWithoutBucket.offsetMs, rawSource)))
    if (logicalPlanWithoutBucket.function == RangeFunctionId.AbsentOverTime) {
      val aggregate = Aggregate(AggregationOperator.Sum, logicalPlanWithoutBucket, Nil,
                                AggregateClause.byOpt(Seq("job")))
      // Add sum to aggregate all child responses
      // If all children have NaN value, sum will yield NaN and AbsentFunctionMapper will yield 1
      val aggregatePlanResult = PlanResult(Seq(addAggregator(aggregate, qContext.copy(plannerParams =
        qContext.plannerParams.copy(skipAggregatePresent = true)), series)))
      addAbsentFunctionMapper(aggregatePlanResult, logicalPlanWithoutBucket.columnFilters,
        RangeParams(logicalPlanWithoutBucket.startMs / 1000, logicalPlanWithoutBucket.stepMs / 1000,
          logicalPlanWithoutBucket.endMs / 1000), qContext)
    } else series
  }

  private def removeBucket(lp: Either[PeriodicSeries, PeriodicSeriesWithWindowing]) = {
    val rawSeries = lp match {
      case Right(value) => value.series
      case Left(value)  => value.rawSeries
    }

    rawSeries match {
      case rawSeriesLp: RawSeries =>

        val nameFilter = rawSeriesLp.filters.find(_.column.equals(PromMetricLabel)).
          map(_.filter.valuesStrings.head.toString)
        val leFilter = rawSeriesLp.filters.find(_.column == "le").map(_.filter.valuesStrings.head.toString)

        if (nameFilter.isEmpty) (nameFilter, leFilter, lp)
        else {
          val filtersWithoutBucket = rawSeriesLp.filters.filterNot(_.column.equals(PromMetricLabel)).
            filterNot(_.column == "le") :+ ColumnFilter(PromMetricLabel,
            Equals(nameFilter.get.replace("_bucket", "")))
          val newLp =
            if (lp.isLeft)
             Left(lp.left.get.copy(rawSeries = rawSeriesLp.copy(filters = filtersWithoutBucket)))
            else
             Right(lp.right.get.copy(series = rawSeriesLp.copy(filters = filtersWithoutBucket)))
          (nameFilter, leFilter, newLp)
        }
      case _ => (None, None, lp)
    }
  }
  private def materializePeriodicSeries(qContext: QueryContext,
                                        lp: PeriodicSeries,
                                        forceInProcess: Boolean): PlanResult = {

   // Convert to FiloDB histogram by removing le label and bucket prefix
   // _sum and _count are removed in MultiSchemaPartitionsExec since we need to check whether there is a metric name
   // with _sum/_count as suffix
    val (nameFilter: Option[String], leFilter: Option[String], lpWithoutBucket: PeriodicSeries) =
    if (queryConfig.translatePromToFilodbHistogram) {
     val result = removeBucket(Left(lp))
      (result._1, result._2, result._3.left.get)

    } else (None, None, lp)

    val rawSeries = walkLogicalPlanTree(lpWithoutBucket.rawSeries, qContext, forceInProcess)
    rawSeries.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(lp.startMs, lp.stepMs, lp.endMs,
      None, None, qContext, stepMultipleNotationUsed = false, Nil, lp.offsetMs)))

    if (nameFilter.isDefined && nameFilter.head.endsWith("_bucket") && leFilter.isDefined) {
      val paramsExec = StaticFuncArgs(leFilter.head.toDouble, RangeParams(lp.startMs/1000, lp.stepMs/1000,
        lp.endMs/1000))
      rawSeries.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(HistogramBucket,
        Seq(paramsExec))))
    }
   rawSeries
  }

  /**
    * Calculates the earliest startTime possible given the query's start/window/step/offset.
    * This is used to bound the startTime of queries so we dont create possibility of aggregating
    * partially expired data and return incomplete results.
    *
    * @return new startTime to be used for query in ms. If original startTime is within retention
    *         period, returns it as is.
    */
  private def boundToStartTimeToEarliestRetained(startMs: Long, stepMs: Long,
                                                 windowMs: Long, offsetMs: Long): Long = {
    // In case query is earlier than earliestRetainedTimestamp then we need to drop the first few instants
    // to prevent inaccurate results being served. Inaccuracy creeps in because data can be in memory for which
    // equivalent data may not be in cassandra. Aggregations cannot be guaranteed to be complete.
    // Also if startMs < 500000000000 (before 1985), it is usually a unit test case with synthetic data, so don't fiddle
    // around with those queries
    val earliestRetainedTimestamp = earliestRetainedTimestampFn
    if (startMs - windowMs - offsetMs < earliestRetainedTimestamp && startMs > 500000000000L) {
      // We calculate below number of steps/instants to drop. We drop instant if data required for that instant
      // doesnt fully fall into the retention period. Data required for that instant involves
      // going backwards from that instant up to windowMs + offsetMs milli-seconds.
      val numStepsBeforeRetention = (earliestRetainedTimestamp - startMs + windowMs + offsetMs) / stepMs
      val lastInstantBeforeRetention = startMs + numStepsBeforeRetention * stepMs
      lastInstantBeforeRetention + stepMs
    } else {
      startMs
    }
  }

  /**
    * If there is a _type_ filter, remove it and populate the schema name string.  This is because while
    * the _type_ filter is a real filter on time series, we don't index it using Lucene at the moment.
    */
  private def extractSchemaFilter(filters: Seq[ColumnFilter]): (Seq[ColumnFilter], Option[String]) = {
    var schemaOpt: Option[String] = None
    val newFilters = filters.filterNot { case ColumnFilter(label, filt) =>
      val isTypeFilt = label == TypeLabel
      if (isTypeFilt) filt match {
        case Filter.Equals(schema) => schemaOpt = Some(schema.asInstanceOf[String])
        case x: Any                 => throw new IllegalArgumentException(s"Illegal filter $x on _type_")
      }
      isTypeFilt
    }
    (newFilters, schemaOpt)
  }

  private def materializeRawSeries(qContext: QueryContext,
                                   lp: RawSeries,
                                   forceInProcess: Boolean): PlanResult = {
    val spreadProvToUse = qContext.plannerParams.spreadOverride.getOrElse(spreadProvider)
    val offsetMillis: Long = lp.offsetMs.getOrElse(0)
    val colName = lp.columns.headOption
    val (renamedFilters, schemaOpt) = extractSchemaFilter(renameMetricFilter(lp.filters))
    val spreadChanges = spreadProvToUse.spreadFunc(renamedFilters)

    val targetSchemaChanges = qContext.plannerParams.targetSchema
      .getOrElse(targetSchemaProvider)
      .targetSchemaFunc(renamedFilters)
    val rangeSelectorWithOffset = lp.rangeSelector match {
      case IntervalSelector(fromMs, toMs) => IntervalSelector(fromMs - offsetMillis - lp.lookbackMs.getOrElse(
                                             queryConfig.staleSampleAfterMs), toMs - offsetMillis)
      case _                              => lp.rangeSelector
    }
    val needsStitch = rangeSelectorWithOffset match {
      case IntervalSelector(from, to) => spreadChanges.exists(c => c.time >= from && c.time <= to)
      case _                          => false
    }

    // Change in Target Schema in query window, do not use target schema to find query shards
    val tsChangeExists = (rangeSelectorWithOffset match {
      case IntervalSelector(from, to) => isTargetSchemaChanging(targetSchemaChanges, from, to)
      case _                          => false
    })
    val targetSchemaOpt = (rangeSelectorWithOffset match {
      case IntervalSelector(from, to) => findTargetSchema(targetSchemaChanges, from, to)
      case _                          => None
    })
    val allTSLabelsPresent = useTargetSchemaForShards(renamedFilters, targetSchemaOpt)

    // Whether to use target-schema for calculating the target shards.
    // If there is change in target-schema or target-schema not defined or query doesn't have all the labels of
    // target-schema, use shardKeyHash to find the target shards.
    // If a target-schema is defined and is not changing during the query window and all the target-schema labels are
    // provided in query filters (Equals), then find the target shard to route to using ingestionShard helper method.
    val useTSForQueryShards = !tsChangeExists && allTSLabelsPresent

    val execPlans = shardsFromFilters(renamedFilters, qContext, useTSForQueryShards).map { shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess)
      MultiSchemaPartitionsExec(qContext, dispatcher, dsRef, shard, renamedFilters,
        toChunkScanMethod(rangeSelectorWithOffset), dsOptions.metricColumn, schemaOpt, colName)
    }
    // Stitch only if spread and/or target-schema changes during the query-window.
    // when target-schema changes during query window, data might be ingested in different shards after the change.
    PlanResult(execPlans, needsStitch || tsChangeExists)
  }

  private def materializeLabelValues(qContext: QueryContext,
                                     lp: LabelValues,
                                     forceInProcess: Boolean): PlanResult = {
    // If the label is PromMetricLabel and is different than dataset's metric name,
    // replace it with dataset's metric name. (needed for prometheus plugins)
    val metricLabelIndex = lp.labelNames.indexOf(PromMetricLabel)
    val labelNames = if (metricLabelIndex > -1 && dsOptions.metricColumn != PromMetricLabel)
      lp.labelNames.updated(metricLabelIndex, dsOptions.metricColumn) else lp.labelNames

    val renamedFilters = renameMetricFilter(lp.filters)
    val shardsToHit = if (canGetShardsFromFilters(renamedFilters, qContext)) {
      shardsFromFilters(renamedFilters, qContext)
    } else {
      mdNoShardKeyFilterRequests.increment()
      shardMapperFunc.assignedShards
    }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess)
      exec.LabelValuesExec(qContext, dispatcher, dsRef, shard, renamedFilters, labelNames, lp.startMs, lp.endMs)
    }
    PlanResult(metaExec)
  }

  // allow metadataQueries to get list of shards from shardKeyFilters only if all shardCols have Equals filter
  private def canGetShardsFromFilters(renamedFilters: Seq[ColumnFilter],
                                      qContext: QueryContext): Boolean = {
    if (qContext.plannerParams.shardOverrides.isEmpty && shardColumns.nonEmpty) {
      shardColumns.toSet.subsetOf(renamedFilters.map(_.column).toSet) &&
        shardColumns.forall { shardCol =>
          // So to compute the shard hash we need shardCol == value filter (exact equals) for each shardColumn
          renamedFilters.find(f => f.column == shardCol) match {
            case Some(ColumnFilter(_, Filter.Equals(_: String))) => true
            case _ => false
          }
        }
    } else false
  }

  private def materializeLabelNames(qContext: QueryContext,
                                    lp: LabelNames,
                                    forceInProcess: Boolean): PlanResult = {
    val renamedFilters = renameMetricFilter(lp.filters)
    val shardsToHit = if (canGetShardsFromFilters(renamedFilters, qContext)) {
      shardsFromFilters(renamedFilters, qContext)
    } else {
      mdNoShardKeyFilterRequests.increment()
      shardMapperFunc.assignedShards
    }

    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess)
      exec.LabelNamesExec(qContext, dispatcher, dsRef, shard, renamedFilters, lp.startMs, lp.endMs)
    }
    PlanResult(metaExec)
  }

  private def materializeLabelCardinality(qContext: QueryContext,
                                    lp: LabelCardinality,
                                          forceInProcess: Boolean): PlanResult = {
    val renamedFilters = renameMetricFilter(lp.filters)
    val shardsToHit = shardsFromFilters(renamedFilters, qContext)

    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess)
      exec.LabelCardinalityExec(qContext, dispatcher, dsRef, shard, renamedFilters, lp.startMs, lp.endMs)
    }
    PlanResult(metaExec)
  }

  private def materializeTsCardinalities(qContext: QueryContext,
                                         lp: TsCardinalities,
                                         forceInProcess: Boolean): PlanResult = {
    val metaExec = shardMapperFunc.assignedShards.map{ shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess)
      exec.TsCardExec(qContext, dispatcher, dsRef, shard, lp.shardKeyPrefix, lp.numGroupByFields)
    }
    PlanResult(metaExec)
  }

  private def materializeSeriesKeysByFilters(qContext: QueryContext,
                                             lp: SeriesKeysByFilters,
                                             forceInProcess: Boolean): PlanResult = {
    // NOTE: _type_ filter support currently isn't there in series keys queries
    val (renamedFilters, _) = extractSchemaFilter(renameMetricFilter(lp.filters))

    val targetSchemaChanges = qContext.plannerParams.targetSchema
      .getOrElse(targetSchemaProvider)
      .targetSchemaFunc(renamedFilters)
    // Change in Target Schema in query window, do not use target schema to find query shards
    val tsChangeExists = isTargetSchemaChanging(targetSchemaChanges, lp.startMs, lp.endMs)
    val targetSchemaOpt = findTargetSchema(targetSchemaChanges, lp.startMs, lp.endMs)
    val allTSLabelsPresent = useTargetSchemaForShards(renamedFilters, targetSchemaOpt)

    // No change in TargetSchema or not all target-schema labels are provided in the query
    val useTSForQueryShards = !tsChangeExists && allTSLabelsPresent
    val shardsToHit = if (useTSForQueryShards || canGetShardsFromFilters(renamedFilters, qContext)) {
      shardsFromFilters(renamedFilters, qContext, useTSForQueryShards)
    } else {
      mdNoShardKeyFilterRequests.increment()
      shardMapperFunc.assignedShards
    }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess)
      PartKeysExec(qContext, dispatcher, dsRef, shard, renamedFilters,
                   lp.fetchFirstLastSampleTimes, lp.startMs, lp.endMs)
    }
    PlanResult(metaExec)
  }

  private def materializeRawChunkMeta(qContext: QueryContext,
                                      lp: RawChunkMeta,
                                      forceInProcess: Boolean): PlanResult = {
    // Translate column name to ID and validate here
    val colName = if (lp.column.isEmpty) None else Some(lp.column)
    val (renamedFilters, schemaOpt) = extractSchemaFilter(renameMetricFilter(lp.filters))
    val metaExec = shardsFromFilters(renamedFilters, qContext).map { shard =>
      val dispatcher = dispatcherForShard(shard, forceInProcess)
      SelectChunkInfosExec(qContext, dispatcher, dsRef, shard, renamedFilters, toChunkScanMethod(lp.rangeSelector),
        schemaOpt, colName)
    }
    PlanResult(metaExec)
  }
}
