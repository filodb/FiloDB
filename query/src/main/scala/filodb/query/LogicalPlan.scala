package filodb.query

import filodb.core.GlobalConfig
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, RangeParams, RvRange}
import filodb.core.query.Filter.Equals
import filodb.query.MiscellaneousFunctionId.{NoOptimize, OptimizeWithAgg}
import filodb.query.lpopt.{AggLpOptimization, AggRuleProvider}

//scalastyle:off number.of.types
//scalastyle:off file.size.limit
sealed trait LogicalPlan {
  /**
    * Execute failure routing
    * Override for Queries which should not be routed e.g time(), month()
    * It is false for RawSeriesLikePlan, MetadataQueryPlan, RawChunkMeta, ScalarTimeBasedPlan and ScalarFixedDoublePlan
    */
  def isRoutable: Boolean = true

  /**
    * Whether to Time-Split queries into smaller range queries if the range exceeds configured limit.
    * This flag will be overridden by plans, which either do not support splitting or will not help in improving
    * performance. For e.g. metadata query plans.
    */
  def isTimeSplittable: Boolean = true

  // scalastyle:off cyclomatic.complexity
  def planColumnFilters(): Seq[ColumnFilter] = this match {
    case lp: ApplyInstantFunction        => lp.vectors.planColumnFilters()
    case lp: ApplyInstantFunctionRaw     => lp.rawSeriesFilters()
    case lp: Aggregate                   => lp.vectors.planColumnFilters()
    case lp: BinaryJoin                  => lp.rhs.planColumnFilters() ++ lp.lhs.planColumnFilters()
    case lp: ScalarVectorBinaryOperation => lp.vector.planColumnFilters()
    case lp: ApplyMiscellaneousFunction  => lp.vectors.planColumnFilters()
    case lp: ApplySortFunction           => lp.vectors.planColumnFilters()
    case lp: ScalarVaryingDoublePlan     => lp.vectors.planColumnFilters()
    case _: ScalarTimeBasedPlan          => Seq.empty
    case lp: VectorPlan                  => lp.scalars.planColumnFilters()
    case _: ScalarFixedDoublePlan        => Seq.empty
    case lp: ApplyAbsentFunction         => lp.vectors.planColumnFilters() ++ lp.columnFilters
    case lp: ApplyLimitFunction          => lp.vectors.planColumnFilters() ++ lp.columnFilters
    case _: ScalarBinaryOperation        => Seq.empty
    case lp: SubqueryWithWindowing       => lp.innerPeriodicSeries.planColumnFilters()
    case lp: TopLevelSubquery            => lp.innerPeriodicSeries.planColumnFilters()
    case lp: PeriodicSeries              => lp.rawSeries.rawSeriesFilters()
    case lp: PeriodicSeriesWithWindowing => lp.series.rawSeriesFilters()
    case lp: RawSeries                   => lp.rawSeriesFilters()
    case lp: RawChunkMeta                => lp.filters
    case lp: MetadataQueryPlan           => lp.filters
    case lp: TsCardinalities             => lp.filters()
  }
  // scalastyle:on cyclomatic.complexity

  /**
    * Replace filters present in logical plan
    */
  def replaceFilters(filters: Seq[ColumnFilter]): LogicalPlan = {
    this match {
      case n: LabelCardinality         => n.copy(filters = filters)
      case p: PeriodicSeriesPlan       => p.replacePeriodicSeriesFilters(filters)
      case r: RawSeriesLikePlan        => r.replaceRawSeriesFilters(filters)
      case l: LabelValues              => l.copy(filters = filters)
      case n: LabelNames               => n.copy(filters = filters)
      case s: SeriesKeysByFilters      => s.copy(filters = filters)
      case c: TsCardinalities          => c  // immutable & no members need to be updated
    }
  }

  /**
   * Optimize the logical plan by using the higher level aggregated metric if applicable
   * @param params AggRule object - contains details of the higher level aggregation rule and metric
   * @return Updated LogicalPlan if Applicable. Else return the same LogicalPlan
   */
  def useHigherLevelAggregatedMetric(aggRuleProvider: AggRuleProvider): LogicalPlan = {
    val filters = planColumnFilters()
    val enabledForFilters = aggRuleProvider.aggRuleOptimizationEnabled(filters)
    // invoke if aggRuleProvider.aggRuleOptimizationEnabled is true, or if the query has optimize_with_agg function
    if (enabledForFilters || hasOptimizeWithAgg(this) && !enabledForFilters) {
      // For now, only PeriodicSeriesPlan and RawSeriesLikePlan are optimized for higher level aggregation
      this match {
        // We start with no parent plans from the root
        case p: PeriodicSeriesPlan => p.useAggregatedMetricIfApplicable(aggRuleProvider)
        case r: RawSeriesLikePlan => r.useAggregatedMetricIfApplicable(aggRuleProvider)
        case _ => this
      }
    } else this
  }

  // scalastyle:off cyclomatic.complexity
  private def hasOptimizeWithAgg(logicalPlan: LogicalPlan): Boolean = {
    logicalPlan match {
      case lp: PeriodicSeries              => false
      case lp: PeriodicSeriesWithWindowing => false
      case lp: ApplyInstantFunction        => hasOptimizeWithAgg(lp.vectors)
      case lp: Aggregate                   => hasOptimizeWithAgg(lp.vectors)
      case lp: BinaryJoin                  => hasOptimizeWithAgg(lp.rhs) || hasOptimizeWithAgg(lp.lhs)
      case lp: ScalarVectorBinaryOperation => hasOptimizeWithAgg(lp.vector)
      case lp: ApplyMiscellaneousFunction  => lp.function == OptimizeWithAgg || hasOptimizeWithAgg(lp.vectors)
      case lp: ApplySortFunction           => hasOptimizeWithAgg(lp.vectors)
      case lp: ScalarVaryingDoublePlan     => hasOptimizeWithAgg(lp.vectors)
      case lp: ScalarTimeBasedPlan         => false
      case lp: VectorPlan                  => hasOptimizeWithAgg(lp.scalars)
      case lp: ApplyAbsentFunction         => hasOptimizeWithAgg(lp.vectors)
      case lp: ApplyLimitFunction          => hasOptimizeWithAgg(lp.vectors)
      case lp: RawSeries                   => false
      case lp: LabelValues                 => false
      case lp: LabelCardinality            => false
      case lp: LabelNames                  => false
      case lp: TsCardinalities             => false
      case lp: SeriesKeysByFilters         => false
      case lp: ApplyInstantFunctionRaw     => hasOptimizeWithAgg(lp.vectors)
      case lp: ScalarBinaryOperation       => false
      case lp: ScalarFixedDoublePlan       => false
      case sq: SubqueryWithWindowing       => hasOptimizeWithAgg(sq.innerPeriodicSeries)
      case tlsq: TopLevelSubquery          => hasOptimizeWithAgg(tlsq.innerPeriodicSeries)
      case lp: RawChunkMeta                => false
    }
  }
  // scalastyle:on cyclomatic.complexity

}

/**
  * Super class for a query that results in range vectors with raw samples (chunks),
  * or one simple transform from the raw data.  This data is likely non-periodic or at least
  * not in the same time cadence as user query windowing.
  */
sealed trait RawSeriesLikePlan extends LogicalPlan {
  def isRaw: Boolean = false
  def replaceRawSeriesFilters(newFilters: Seq[ColumnFilter]): RawSeriesLikePlan

  /**
   * Used by logical plan optimization logic in AggLpOptimization to replace metric name, type filter and columns
   * with those for the pre-agg metric
   */
  def updateRawSeriesForAggOptimize(newFilters: Seq[ColumnFilter], columns: Seq[String]): RawSeriesLikePlan

  /**
   * Optimizes the plan to use aggregated metric where possible
   * @return Updated logical plan if optimized for higher level aggregation. Else return the same logical plan
   */
  def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): RawSeriesLikePlan

  /**
   * @return Raw series column filters
   */
  def rawSeriesFilters(): Seq[ColumnFilter]

  /**
   * @return Metric name from the raw series filters
   *         Throws IllegalArgumentException if no metric name is found in the filters
   */
  def metricName(): String

  def columns(): Seq[String]

  def rangeSelector(): RangeSelector

}

sealed trait NonLeafLogicalPlan extends LogicalPlan {
  def children: Seq[LogicalPlan]
}

/**
  * Super class for a query that results in range vectors with samples
  * in regular steps
  */
sealed trait PeriodicSeriesPlan extends LogicalPlan {
  /**
    * Periodic Query start time in millis
    */
  def startMs: Long

  /**
    * Periodic Query end time in millis
    */
  def stepMs: Long
  /**
    * Periodic Query step time in millis
    */
  def endMs: Long

  def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan

  /**
   * This function traverses down the logical plan tree to the raw series leaf level and check if the
   * aggregation clauses and the column filters satisfies the constraints of the higher level aggregation rule. If so,
   * we update the raw series metric column filter with the higher level aggregation metric to reduce number of
   * timeseries to be scanned and reduce the query latency.
   * @param aggRuleProvider can provide the aggregation rules
   * @return Updated logical plan if optimized for higher level aggregation. Else return the same logical plan
   */
  def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan
}

sealed trait MetadataQueryPlan extends LogicalPlan {

  override def isTimeSplittable: Boolean = false

  val filters: Seq[ColumnFilter]

  val startMs: Long

  val endMs: Long

}

/**
 * Plans with this trait can be pushdown-optimized.
 * See LogicalPlanUtils::getPushdownKeys for more info.
 */
sealed trait CandidatePushdownPlan extends LogicalPlan

/**
  * A selector is needed in the RawSeries logical plan to specify
  * a row key range to extract from each partition.
  */
sealed trait RangeSelector extends java.io.Serializable
case object AllChunksSelector extends RangeSelector
case object WriteBufferSelector extends RangeSelector
case object InMemoryChunksSelector extends RangeSelector
case object EncodedChunksSelector extends RangeSelector
case class IntervalSelector(from: Long, to: Long) extends RangeSelector


/**
  * Concrete logical plan to query for raw data in a given range
  * @param columns the columns to read from raw chunks.  Note that it is not necessary to include
  *        the timestamp column, that will be automatically added.
  *        If no columns are included, the default value column will be used.
  */
case class RawSeries(rangeSelector: RangeSelector,
                     filters: Seq[ColumnFilter],
                     columns: Seq[String],
                     lookbackMs: Option[Long] = None,
                     offsetMs: Option[Long] = None,
                     supportsRemoteDataCall: Boolean = false) extends RawSeriesLikePlan {
  override def isRaw: Boolean = true

  override def replaceRawSeriesFilters(newFilters: Seq[ColumnFilter]): RawSeriesLikePlan = {
    val filterColumns = newFilters.map(_.column)
    val updatedFilters = this.filters.filterNot(f => filterColumns.contains(f.column)) ++ newFilters
    this.copy(filters = updatedFilters)
  }

  def updateRawSeriesForAggOptimize(newFilters: Seq[ColumnFilter], columns: Seq[String]): RawSeriesLikePlan = {
    val typeValue = this.filters.find(_.column == Schemas.TypeLabel) match {
      case Some(ColumnFilter(_, Equals(value))) => Some(value.toString)
      case Some(c) => throw new IllegalStateException(s"Should not be optimizing query with type " +
                                                      s"filter which is not equals: $c")
      case None => None
    }
    val filtersToReplace = typeValue.flatMap(t => Schemas.preAggSchema.get(t)) match {
      case Some(preAggType) => newFilters :+ ColumnFilter(Schemas.TypeLabel, Equals(preAggType))
      case _ => newFilters
    }
    val filterColumns = filtersToReplace.map(_.column)
    val updatedFilters = this.filters.filterNot(f => filterColumns.contains(f.column)) ++ filtersToReplace
    this.copy(filters = updatedFilters, columns = columns)
  }

  def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): RawSeriesLikePlan = {
    this // RawSeries queries are not optimized for higher level aggregation
  }

  override def rawSeriesFilters(): Seq[ColumnFilter] = filters

  override def metricName(): String = filters.collectFirst {
    case ColumnFilter(GlobalConfig.PromMetricLabel, Equals(value)) => value.toString
  }.getOrElse(throw new IllegalArgumentException(s"RawSeries query must always have a metric name"))
}

case class LabelValues(labelNames: Seq[String],
                       filters: Seq[ColumnFilter],
                       startMs: Long,
                       endMs: Long) extends MetadataQueryPlan


case class LabelCardinality( filters: Seq[ColumnFilter],
                             startMs: Long,
                             endMs: Long,
                             clusterType: String  = "raw") extends MetadataQueryPlan

case class LabelNames(filters: Seq[ColumnFilter],
                      startMs: Long,
                      endMs: Long) extends MetadataQueryPlan

case class SeriesKeysByFilters(filters: Seq[ColumnFilter],
                               fetchFirstLastSampleTimes: Boolean,
                               startMs: Long,
                               endMs: Long) extends MetadataQueryPlan

object TsCardinalities {
  val LABEL_WORKSPACE = "_ws_"
  val LABEL_NAMESPACE = "_ns_"
  val SHARD_KEY_LABELS = Seq(LABEL_WORKSPACE, LABEL_NAMESPACE, GlobalConfig.PromMetricLabel)
}

/**
 * Plan to answer queries of the abstract form:
 *
 * Find (active, total) cardinality pairs for all time series with <shard-key-prefix>,
 *   then group them by { key[:1], key[:2], key[:3], ... }.
 *
 * Examples:
 *
 *  { prefix=[], numGroupByFields=2 } -> {
 *      prefix=["ws_a", "ns_a"] -> (4, 6),
 *      prefix=["ws_a", "ns_b"] -> (2, 4),
 *      prefix=["ws_b", "ns_c"] -> (3, 5) }
 *
 *  { prefix=["ws_a", "ns_a"], numGroupByFields=3 } -> {
 *      prefix=["ws_a", "ns_a", "met_a"] -> (4, 6),
 *      prefix=["ws_a", "ns_a", "met_b"] -> (3, 5) }
 *
 *  { prefix=["ws_a"], numGroupByFields=1 } -> {
 *      prefix=["ws_a"] -> (3, 5) }
 *
 * @param numGroupByFields: indicates "hierarchical depth" at which to group cardinalities.
 *   For example:
 *     1 -> workspace
 *     2 -> namespace
 *     3 -> metric
 *   Must indicate a depth:
 *     (1) at least as deep as shardKeyPrefix.
 *     (2) less than '3' when the prefix does not contain values for all lesser depths.
 *   Example (if shard keys specify a ws, ns, and metric):
 *     shardKeyPrefix     numGroupByFields
 *     []                 { 1, 2 }
 *     [ws]               { 1, 2 }
 *     [ws, ns]           { 2, 3 }
 *     [ws, ns, metric]   { 3 }
 */
case class TsCardinalities(shardKeyPrefix: Seq[String],
                           numGroupByFields: Int,
                           datasets: Seq[String] = Seq(),
                           userDatasets: String = "",
                           overrideClusterName: String = "") extends LogicalPlan {
  import TsCardinalities._

  require(numGroupByFields >= 1 && numGroupByFields <= 3,
    "numGroupByFields must lie on [1, 3]")
  require(numGroupByFields >= shardKeyPrefix.size,
    "numGroupByFields indicate a depth at least as deep as shardKeyPrefix")
  require(numGroupByFields < 3 || shardKeyPrefix.size >= 2,
    "cannot group at the metric level when prefix does not contain ws and ns")

  // TODO: this should eventually be "true" to enable HAP/LTRP routing
  override def isRoutable: Boolean = false

  def filters(): Seq[ColumnFilter] = SHARD_KEY_LABELS.zip(shardKeyPrefix).map{ case (label, value) =>
    ColumnFilter(label, Equals(value))}

  /**
   * Helper function to create a Map of all the query parameters to be sent for the remote API call
   * @return Immutable Map[String, String]
   */
  def queryParams(): Map[String, String] = {
    Map(
        "match[]" -> ("{" + SHARD_KEY_LABELS.zip(shardKeyPrefix)
                       .map{ case (label, value) => s"""$label="$value""""}
                       .mkString(",") + "}"),
        "numGroupByFields" -> numGroupByFields.toString,
        "verbose" -> "true", // Using this plan to determine if we need to pass additional values in groupKey or not
        "datasets" -> userDatasets
    )
  }
}

/**
 * Concrete logical plan to query for chunk metadata from raw time series in a given range
 * @param column the column name from which to extract chunk information like chunk size and encoding type
 */
case class RawChunkMeta(rangeSelector: RangeSelector,
                        filters: Seq[ColumnFilter],
                        column: String) extends PeriodicSeriesPlan {
  override def isRoutable: Boolean = false

  // FIXME - TechDebt - This class should not be a PeriodicSeriesPlan
  override def startMs: Long = ???
  override def stepMs: Long = ???
  override def endMs: Long = ???

  override def replacePeriodicSeriesFilters(newFilters: Seq[ColumnFilter]): PeriodicSeriesPlan  = {
    val filterColumns = newFilters.map(_.column)
    val updatedFilters = this.filters.filterNot(f => filterColumns.contains(f.column)) ++ newFilters
    this.copy(filters = updatedFilters)
  }

  override def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = {
    // RawChunkMeta queries are not optimized for higher level aggregation
    this
  }
}

/**
  * Concrete logical plan to query for data in a given range
  * with results in a regular time interval.
  *
  * Issue with specifying start/end/step here in the selector
  * is that plans involving multiple series can come with different
  * ranges and steps.
  *
  * This should be taken care outside this layer, or we need to have
  * proper validation.
  */
case class PeriodicSeries(rawSeries: RawSeriesLikePlan,
                          startMs: Long,
                          stepMs: Long,
                          endMs: Long,
                          offsetMs: Option[Long] = None,
                          atMs: Option[Long] = None) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(rawSeries)

  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this.copy(rawSeries =
    rawSeries.replaceRawSeriesFilters(filters))

  override def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = {
    this.copy(rawSeries = rawSeries.useAggregatedMetricIfApplicable(aggRuleProvider))
  }
}

/**
  *  Subquery represents a series of N points conceptually generated
  *  by running N homogeneous queries where
  *  SW = subquery window
  *  SS = subquery step
  *  N = SW / SS + 1
  *  For example, foo[5m:1m], would generate a series of 6 points 1 minute apart.
  *
  *  query_range API is an equivalent of subquery though the concept of subquery
  *  is wider. query_range can be called with start/end/step parameters only on the
  *  top most expression in the query while subqueries can be:
  *  (1) top level expression (complete equivalent to query_range)
  *  (2) arguments to time range functions, potentially nested several levels deep
  *
  *  So, there are two major cases for a subquery expression:
  *  A) any_instant_expression[W:S] as a top level expression. In this case, subquery
  *     would issue "any_instant_expression" with its own start, end, and step
  *     parameters. If such an expression is called with query_range API, where start != end
  *     and step is not zero, an exception is thrown. No special subquery
  *     node in logical plan is generated as all of the PeriodicSeries logical plans can
  *     handle range_query API's start, step, and end parameters.
  *  B) RangeFunction(any_instant_expression[W:S]) at any node/level of abstract syntax tree.
  *     In this case, we ALWAYS have a range function involved and SubqueryWithWindowing
  *     logical plan node is generated. The plan is almost identical in the functionality to
  *     PeriodicSeriesWithWindowing. The difference between the two is that currently
  *     PeriodicSeriesWithWindowing expects a RawSeriesLikePlan while subquery is NOT
  *     necessarily an instant selector. PeriodicSeriesWithWindowing needs to be refactored,
  *     so, it could handle any PeriodicSeries; however, at this point we are going to
  *     replicate the functionality in the specialized SubqueryWithWindowing logical plan.
  *     We do so, that we can integrage subquery code without affecting existing existing
  *     code paths, stabilize the codebase, and later merge the two.
  *  Below is an example of B) case:
  *  sum_over_time(<someExpression>[5m:1m]) called with query_range API parameters:
  *  start=S, end=E, step=ST
  *  Here query range API start,end, and step correspond to startMs, stepMs, and endMs,
  *  however, it's not necessarily the case for nested subqueries because start, step, and en
  *  will depend on the parent expression of the subquery
  *  subqueryStepMs is used exclusively for debugging/logging, as the actual subquery step is
  *  already baked into innerPeriodicSeries which is constructed with the subquery step before
  *  passing to the constructor of SubqueryWithWindowing
  */
case class SubqueryWithWindowing(
  innerPeriodicSeries: PeriodicSeriesPlan, // someExpression
  startMs: Long, // S
  stepMs: Long, // ST
  endMs: Long, // E
  functionId: RangeFunctionId, // sum_over_time
  functionArgs: Seq[FunctionArgsPlan] = Nil,
  subqueryWindowMs: Long, // 5m
  subqueryStepMs: Long, //1m
  offsetMs: Option[Long],
  atMs: Option[Long]
) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(innerPeriodicSeries)

  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = {
    val updatedInnerPeriodicSeries = innerPeriodicSeries.replacePeriodicSeriesFilters(filters)
    val updatedFunctionArgs = functionArgs.map(_.replacePeriodicSeriesFilters(filters).asInstanceOf[FunctionArgsPlan])
    this.copy(innerPeriodicSeries = updatedInnerPeriodicSeries, functionArgs = updatedFunctionArgs)
  }

  override def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = {
    // recurse to the leaf level
    this.copy(innerPeriodicSeries = innerPeriodicSeries.useAggregatedMetricIfApplicable(aggRuleProvider))
  }
}

/**
 * Please, refer to documentation of SubqueryWithWindowing, this class
 * corresponds to case A), for example, foo[5m:1m]
 * Overall, TopLevelSubquery is just a wrapper on top of the actual underlying PeriodicSeriesPlan, and is used
 * primarily to record the existence of the actual subquery construct in the original query.
 * When we parse top level subquery to create a logical plan, start and end parameters passed from query_range API
 * are supposed to be the same. However, TopLevelSubquery logical plan as a PeriodicSeriesPlan
 * will have its own startMs set to (original_start - subquery lookback), 5m in case of the above example.
 * endMs will stay the same as the original end.
 * The original start time needs to be modified in order for the TopLevelSubquery to be splittable to support
 * long range subqueries. If logical plan has its start and end equal, this plan is impossible to split.
 * Subqueries, however, should be splittable. Generally, the parameters:
 * startMs, stepMs, endMs are not used by the further planners,
 * they are needed only for the logic to modify the logical plan to enable spanning long range queries across
 * several clusters.
 * original_start, original_end are the start and end parameters of TimeRangeParam passed to toSeriesPlan()
 * invoked to generate TopLevelSubquery
 * @param startMs (original_start  - subquery_lookback)
 * @param stepMs is the value of the subquery step, not used by the planners but for debugging/documentation
 * @param endMs should always be the same as original_start
 */
case class TopLevelSubquery(
  innerPeriodicSeries: PeriodicSeriesPlan, // someExpression
  startMs: Long, //original start - 5m
  stepMs: Long,
  endMs: Long,
  orginalLookbackMs: Long,
  originalOffsetMs: Option[Long],
  atMs: Option[Long]
) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(innerPeriodicSeries)

  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = {
    val updatedInnerPeriodicSeries = innerPeriodicSeries.replacePeriodicSeriesFilters(filters)
    this.copy(innerPeriodicSeries = updatedInnerPeriodicSeries)
  }

  override def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = {
    // recurse to the leaf level
    this.copy(innerPeriodicSeries = innerPeriodicSeries.useAggregatedMetricIfApplicable(aggRuleProvider))
  }
}

/**
  * Concrete logical plan to query for data in a given range
  * with results in a regular time interval.
  *
  * Applies a range function on raw windowed data (perhaps with instant function applied) before
  * sampling data at regular intervals.
  * @param stepMultipleNotationUsed is true if promQL lookback used a step multiple notation
  *                                 like foo{..}[3i]
  */
case class PeriodicSeriesWithWindowing(series: RawSeriesLikePlan,
                                       startMs: Long,
                                       stepMs: Long,
                                       endMs: Long,
                                       window: Long,
                                       function: RangeFunctionId,
                                       stepMultipleNotationUsed: Boolean = false,
                                       functionArgs: Seq[FunctionArgsPlan] = Nil,
                                       offsetMs: Option[Long] = None,
                                       atMs: Option[Long] = None) extends PeriodicSeriesPlan
  with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(series)

  def columnFilters: Seq[ColumnFilter] = {
    // The filters are the raw series filters, which are used to filter the raw series data
    series.rawSeriesFilters()
  }

  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan =
    this.copy(series = series.replaceRawSeriesFilters(filters),
              functionArgs = functionArgs.map(_.replacePeriodicSeriesFilters(filters).asInstanceOf[FunctionArgsPlan]))

  def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = {
    this.copy(series = series.useAggregatedMetricIfApplicable(aggRuleProvider))
  }

  def updateRawSeriesForAggOptimize(filters: Seq[ColumnFilter], col: Seq[String],
                                    rf: Option[RangeFunctionId]): PeriodicSeriesWithWindowing = {
    this.copy(series = series.updateRawSeriesForAggOptimize(filters, col), function = rf.getOrElse(function))
  }
}

/**
  * Identifies the by/without clause used with an aggregator.
  */
case class AggregateClause(clauseType: AggregateClause.ClauseType.Value,
                           labels: Seq[String] = Nil)

case object AggregateClause {

  case object ClauseType extends Enumeration {
    val By, Without = Value
  }

  /**
    * Returns an Optional-wrapped "by" AggregateClause.
    */
  def byOpt(labels: Seq[String] = Nil) : Option[AggregateClause] = {
    Option(new AggregateClause(ClauseType.By, labels))
  }

  /**
   * Returns an Optional-wrapped "without" AggregateClause.
   */
  def withoutOpt(labels: Seq[String] = Nil) : Option[AggregateClause] = {
    Option(new AggregateClause(ClauseType.Without, labels))
  }
}

/**
  * Aggregate data across partitions (not in the time dimension).
  * Aggregation can be done only on range vectors with consistent
  * sampling interval.
  * @param clauseOpt columns to group by/without.
  *     None indicates no by/without clause is specified.
  */
case class Aggregate(operator: AggregationOperator,
                     vectors: PeriodicSeriesPlan,
                     params: Seq[Any] = Nil,
                     clauseOpt: Option[AggregateClause] = None)
      extends PeriodicSeriesPlan with NonLeafLogicalPlan with CandidatePushdownPlan {
  override def children: Seq[LogicalPlan] = Seq(vectors)
  override def startMs: Long = vectors.startMs
  override def stepMs: Long = vectors.stepMs
  override def endMs: Long = vectors.endMs
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this.copy(vectors =
    vectors.replacePeriodicSeriesFilters(filters))

  def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = {
    AggLpOptimization.optimizeWithPreaggregatedDataset(this, aggRuleProvider) match {
      case Some(aggPlan) => aggPlan
      case None =>  // the query at this level is not eligible for pre-aggregation optimization
                    // check if the child plan is eligible for pre-aggregation optimization
                    this.copy(vectors = vectors.useAggregatedMetricIfApplicable(aggRuleProvider))
    }
  }
}

/**
  * Binary join between collections of RangeVectors.
  * One-To-One, Many-To-One and One-To-Many are supported.
  *
  * If data resolves to a Many-To-Many relationship, error will be returned.
  *
  * @param on columns to join on
  * @param ignoring columns to ignore while joining
  * @param include labels specified in group_left/group_right to be included from one side
  */
case class BinaryJoin(lhs: PeriodicSeriesPlan,
                      operator: BinaryOperator,
                      cardinality: Cardinality,
                      rhs: PeriodicSeriesPlan,
                      on: Option[Seq[String]] = None,
                      ignoring: Seq[String] = Nil,
                      include: Seq[String] = Nil)
      extends PeriodicSeriesPlan with NonLeafLogicalPlan with CandidatePushdownPlan {
  require(lhs.startMs == rhs.startMs)
  require(lhs.endMs == rhs.endMs)
  require(lhs.stepMs == rhs.stepMs)
  override def children: Seq[LogicalPlan] = Seq(lhs, rhs)
  override def startMs: Long = lhs.startMs
  override def stepMs: Long = lhs.stepMs
  override def endMs: Long = lhs.endMs
  override def isRoutable: Boolean = lhs.isRoutable || rhs.isRoutable
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this.copy(lhs =
    lhs.replacePeriodicSeriesFilters(filters), rhs = rhs.replacePeriodicSeriesFilters(filters))

  override def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = {
    // Optimize lhs and rhs separately. If both are optimized, create a new BinaryJoin plan with optimized
    // We do this because raw and aggregated metrics could have different retention periods, if only one of them
    // is optimized, this could lead to inconsistent results that are not explainable easily to the user
    val lhsOpt = lhs.useAggregatedMetricIfApplicable(aggRuleProvider)
    val rhsOpt = rhs.useAggregatedMetricIfApplicable(aggRuleProvider)
    if (lhsOpt != lhs && rhsOpt != rhs) // both lhs and rhs are optimized
      this.copy(
        lhs = lhsOpt,
        rhs = rhsOpt
      )
    else this
  }
}

/**
  * Apply Scalar Binary operation to a collection of RangeVectors
  */
case class ScalarVectorBinaryOperation(operator: BinaryOperator,
                                       scalarArg: ScalarPlan,
                                       vector: PeriodicSeriesPlan,
                                       scalarIsLhs: Boolean) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(vector, scalarArg)
  override def startMs: Long = vector.startMs
  override def stepMs: Long = vector.stepMs
  override def endMs: Long = vector.endMs
  override def isRoutable: Boolean = vector.isRoutable
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan =
    this.copy(vector = vector.replacePeriodicSeriesFilters(filters),
              scalarArg = scalarArg.replacePeriodicSeriesFilters(filters).asInstanceOf[ScalarPlan])

  override def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = {
    // No special handling for ScalarVectorBinaryOperation. Just pass the call to vector and and scalar plan recursively
    this.copy(
      vector = vector.useAggregatedMetricIfApplicable(aggRuleProvider),
      scalarArg = scalarArg.useAggregatedMetricIfApplicable(aggRuleProvider).asInstanceOf[ScalarPlan])
  }
}

/**
  * Apply Instant Vector Function to a collection of periodic RangeVectors,
  * returning another set of periodic vectors
  */
case class ApplyInstantFunction(vectors: PeriodicSeriesPlan,
                                function: InstantFunctionId,
                                functionArgs: Seq[FunctionArgsPlan] = Nil) extends PeriodicSeriesPlan
  with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(vectors)
  override def startMs: Long = vectors.startMs
  override def stepMs: Long = vectors.stepMs
  override def endMs: Long = vectors.endMs
  override def isRoutable: Boolean = vectors.isRoutable
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this.copy(
    vectors = vectors.replacePeriodicSeriesFilters(filters),
    functionArgs = functionArgs.map(_.replacePeriodicSeriesFilters(filters).asInstanceOf[FunctionArgsPlan]))

  override def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = {
    // No special handling for ApplyInstantFunction. Just pass the call to vectors and functionArgs recursively
    this.copy(
     vectors = vectors.useAggregatedMetricIfApplicable(aggRuleProvider),
     functionArgs = functionArgs.map(_.useAggregatedMetricIfApplicable(aggRuleProvider).asInstanceOf[FunctionArgsPlan]))
  }
}

/**
  * Apply Instant Vector Function to a collection of raw RangeVectors,
  * returning another set of non-periodic vectors
  */
case class ApplyInstantFunctionRaw(vectors: RawSeries,
                                   function: InstantFunctionId,
                                   functionArgs: Seq[FunctionArgsPlan] = Nil) extends RawSeriesLikePlan
  with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(vectors)
  override def replaceRawSeriesFilters(newFilters: Seq[ColumnFilter]): RawSeriesLikePlan = this.copy(
    vectors = vectors.replaceRawSeriesFilters(newFilters).asInstanceOf[RawSeries],
    functionArgs = functionArgs.map(_.replacePeriodicSeriesFilters(newFilters).asInstanceOf[FunctionArgsPlan]))

  // Should not be called. ApplyInstantFunctionRaw is not optimized for higher level aggregation
  def updateRawSeriesForAggOptimize(newFilters: Seq[ColumnFilter], columns: Seq[String]): RawSeriesLikePlan = ???

  override def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): RawSeriesLikePlan = {
    this // this plan is not optimized for higher level aggregation
  }

  override def rawSeriesFilters(): Seq[ColumnFilter] = vectors.rawSeriesFilters()

  override def metricName(): String = vectors.metricName()

  override def columns(): Seq[String] = vectors.columns

  override def rangeSelector(): RangeSelector = vectors.rangeSelector
}

/**
  * Apply Miscellaneous Function to a collection of RangeVectors
  */
case class ApplyMiscellaneousFunction(vectors: PeriodicSeriesPlan,
                                      function: MiscellaneousFunctionId,
                                      stringArgs: Seq[String] = Nil) extends PeriodicSeriesPlan
  with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(vectors)
  override def startMs: Long = vectors.startMs
  override def stepMs: Long = vectors.stepMs
  override def endMs: Long = vectors.endMs
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this.copy(vectors =
    vectors.replacePeriodicSeriesFilters(filters))

  override def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = {
    val filters = planColumnFilters()
    val enabledForFilters = aggRuleProvider.aggRuleOptimizationEnabled(filters)
    if ( (function != NoOptimize && enabledForFilters) ||
         (function == OptimizeWithAgg && !enabledForFilters) ) {
      this.copy(vectors = vectors.useAggregatedMetricIfApplicable(aggRuleProvider))
    } else
      this
  }
}

/**
  * Apply Sort Function to a collection of RangeVectors
  */
case class ApplySortFunction(vectors: PeriodicSeriesPlan,
                             function: SortFunctionId) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(vectors)
  override def startMs: Long = vectors.startMs
  override def stepMs: Long = vectors.stepMs
  override def endMs: Long = vectors.endMs
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this.copy(vectors =
    vectors.replacePeriodicSeriesFilters(filters))

  override def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = {
    this.copy(vectors = vectors.useAggregatedMetricIfApplicable(aggRuleProvider))
  }
}

/**
  * Nested logical plan for argument of function
  * Example: clamp_max(node_info{job = "app"},scalar(http_requests_total{job = "app"}))
  */
sealed trait FunctionArgsPlan extends LogicalPlan with PeriodicSeriesPlan

/**
  * Generate scalar
  * Example: scalar(http_requests_total), time(), hour()
  */
sealed trait ScalarPlan extends FunctionArgsPlan

/**
  * Generate scalar from vector
  * Example: scalar(http_requests_total)
  */
final case class ScalarVaryingDoublePlan(vectors: PeriodicSeriesPlan,
                                         function: ScalarFunctionId,
                                         functionArgs: Seq[FunctionArgsPlan] = Nil)
                                         extends ScalarPlan with PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(vectors)
  override def startMs: Long = vectors.startMs
  override def stepMs: Long = vectors.stepMs
  override def endMs: Long = vectors.endMs
  override def isRoutable: Boolean = vectors.isRoutable
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this.copy(
    vectors = vectors.replacePeriodicSeriesFilters(filters),
    functionArgs = functionArgs.map(_.replacePeriodicSeriesFilters(filters).asInstanceOf[FunctionArgsPlan]))

  override def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = {
    this.copy(
     vectors = vectors.useAggregatedMetricIfApplicable(aggRuleProvider),
     functionArgs = functionArgs.map(_.useAggregatedMetricIfApplicable(aggRuleProvider).asInstanceOf[FunctionArgsPlan]))
  }
}

/**
  * Scalar generated by time functions which do not have metric as input
  * Example: time(), hour()
  */
final case class ScalarTimeBasedPlan(function: ScalarFunctionId, rangeParams: RangeParams) extends ScalarPlan {
  override def isRoutable: Boolean = false
  override def startMs: Long = rangeParams.startSecs * 1000
  override def stepMs: Long = rangeParams.stepSecs * 1000
  override def endMs: Long = rangeParams.endSecs * 1000
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this // No Filter

  // No optimization for the scalar plan without raw or periodic series
  override def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = this
}

/**
  * Logical plan for numeric values. Used in queries like foo + 5
  * Example: 3, 4.2
  */
final case class ScalarFixedDoublePlan(scalar: Double,
                                       timeStepParams: RangeParams)
                                       extends ScalarPlan with FunctionArgsPlan {
  override def isRoutable: Boolean = false
  override def startMs: Long = timeStepParams.startSecs * 1000
  override def stepMs: Long = timeStepParams.stepSecs * 1000
  override def endMs: Long = timeStepParams.endSecs * 1000
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this

  // No optimization for the scalar plan without raw or periodic series
  override def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = this
}

//scalastyle:off number.of.types
/**
  * Generates vector from scalars
  * Example: vector(3), vector(scalar(http_requests_total)
  */
final case class VectorPlan(scalars: ScalarPlan) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(scalars)
  override def startMs: Long = scalars.startMs
  override def stepMs: Long = scalars.stepMs
  override def endMs: Long = scalars.endMs
  override def isRoutable: Boolean = scalars.isRoutable
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this.copy(scalars =
    scalars.replacePeriodicSeriesFilters(filters).asInstanceOf[ScalarPlan])

  override def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = {
    this.copy(
      scalars = scalars.useAggregatedMetricIfApplicable(aggRuleProvider).asInstanceOf[ScalarPlan])
  }
}

/**
  * Apply Binary operation between two fixed scalars
  */
case class ScalarBinaryOperation(operator: BinaryOperator,
                                 lhs: Either[Double, ScalarBinaryOperation],
                                 rhs: Either[Double, ScalarBinaryOperation],
                                 rangeParams: RangeParams) extends ScalarPlan {
  override def startMs: Long = rangeParams.startSecs * 1000
  override def stepMs: Long = rangeParams.stepSecs * 1000
  override def endMs: Long = rangeParams.endSecs * 1000
  override def isRoutable: Boolean = false
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = {
    val updatedLhs = if (lhs.isRight) Right(lhs.toOption.get.replacePeriodicSeriesFilters(filters).
                      asInstanceOf[ScalarBinaryOperation]) else Left(lhs.swap.toOption.get)
    val updatedRhs = if (rhs.isRight) Right(rhs.toOption.get.replacePeriodicSeriesFilters(filters).
                      asInstanceOf[ScalarBinaryOperation]) else Left(rhs.swap.toOption.get)
    this.copy(lhs = updatedLhs, rhs = updatedRhs)
  }

  override def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = {
    val updatedLhs = if (lhs.isRight)
      Right(lhs.toOption.get.useAggregatedMetricIfApplicable(aggRuleProvider).asInstanceOf[ScalarBinaryOperation])
    else Left(lhs.swap.toOption.get)

    val updatedRhs = if (rhs.isRight)
      Right(rhs.toOption.get.useAggregatedMetricIfApplicable(aggRuleProvider).asInstanceOf[ScalarBinaryOperation])
    else Left(rhs.swap.toOption.get)
    this.copy(lhs = updatedLhs, rhs = updatedRhs)
  }
}

/**
  * Apply Absent Function to a collection of RangeVectors
  */
case class ApplyAbsentFunction(vectors: PeriodicSeriesPlan,
                               columnFilters: Seq[ColumnFilter],
                               rangeParams: RangeParams,
                               functionArgs: Seq[Any] = Nil) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(vectors)
  override def startMs: Long = vectors.startMs
  override def stepMs: Long = vectors.stepMs
  override def endMs: Long = vectors.endMs
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan =
    this.copy(columnFilters = LogicalPlan.overrideColumnFilters(columnFilters, filters),
              vectors = vectors.replacePeriodicSeriesFilters(filters))

  override def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = {
    this.copy(vectors = vectors.useAggregatedMetricIfApplicable(aggRuleProvider))
  }
}

/**
 * Apply Limit Function to a collection of RangeVectors
 */
case class ApplyLimitFunction(vectors: PeriodicSeriesPlan,
                              columnFilters: Seq[ColumnFilter],
                              rangeParams: RangeParams,
                              limit: Int) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(vectors)
  override def startMs: Long = vectors.startMs
  override def stepMs: Long = vectors.stepMs
  override def endMs: Long = vectors.endMs
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan =
    this.copy(columnFilters = LogicalPlan.overrideColumnFilters(columnFilters, filters),
              vectors = vectors.replacePeriodicSeriesFilters(filters))

  override def useAggregatedMetricIfApplicable(aggRuleProvider: AggRuleProvider): PeriodicSeriesPlan = {
    this.copy(vectors = vectors.useAggregatedMetricIfApplicable(aggRuleProvider))
  }
}

object LogicalPlan {
  /**
    * Get leaf Logical Plans
    */
  def findLeafLogicalPlans (logicalPlan: LogicalPlan) : Seq[LogicalPlan] = {
   logicalPlan match {
     case lp: ScalarVaryingDoublePlan => findLeafLogicalPlans(lp.vectors)
     // scalarArg can have vector like scalar(http_requests_total)
     case lp: ScalarVectorBinaryOperation => findLeafLogicalPlans(lp.vector) ++ findLeafLogicalPlans(lp.scalarArg)
     // Find leaf logical plans for all children and concatenate results
     case lp: NonLeafLogicalPlan          => lp.children.flatMap(findLeafLogicalPlans)
     case lp: MetadataQueryPlan           => Seq(lp)
     case lp: TsCardinalities             => Seq(lp)
     case lp: ScalarBinaryOperation       => val lhsLeafs = if (lp.lhs.isRight)
                                               findLeafLogicalPlans(lp.lhs.toOption.get)
                                             else Nil
                                             val rhsLeafs = if (lp.rhs.isRight)
                                               findLeafLogicalPlans(lp.rhs.toOption.get)
                                             else Nil
                                             lhsLeafs ++ rhsLeafs
     case lp: ScalarFixedDoublePlan       => Seq(lp)
     case lp: ScalarTimeBasedPlan         => Seq(lp)
     case lp: RawSeries                   => Seq(lp)
     case lp: RawChunkMeta                => Seq(lp)
   }
  }

  /**
   * Returns true if there is a subquery with windowing in the logical plan
   */
  def hasSubqueryWithWindowing(logicalPlan: LogicalPlan) : Boolean = {
    logicalPlan match {
      case sqww: SubqueryWithWindowing => true
      case lp: NonLeafLogicalPlan => lp.children.foldLeft(false)(
        (acc: Boolean, lp: LogicalPlan) => acc || hasSubqueryWithWindowing(lp)
      )
      case _ => false
    }
  }

  def getColumnValues(logicalPlan: LogicalPlan, labelName: String): Set[String] = {
    getColumnValues(getColumnFilterGroup(logicalPlan), labelName)
  }

  def getColumnValues(columnFilterGroup: Seq[Set[ColumnFilter]],
                      labelName: String): Set[String] = {
    columnFilterGroup.flatMap (columnFilters => getColumnValues(columnFilters, labelName)) match {
      case columnValues: Iterable[String]    => if (columnValues.isEmpty) Set.empty else columnValues.toSet
      case _                                 => Set.empty
    }
  }

  def getColumnValues(columnFilters: Set[ColumnFilter], labelName: String): Set[String] = {
    columnFilters.flatMap(cFilter => {
      if (cFilter.column == labelName) {
        cFilter.filter.valuesStrings.map(_.toString)
      } else {
        Seq.empty
      }
    })
  }

  def getColumnValues(columnFilters: Seq[ColumnFilter], labelName: String): Seq[String] = {
    columnFilters.flatMap(cFilter => {
      if (cFilter.column == labelName) {
        cFilter.filter.valuesStrings.map(_.toString)
      } else {
        Seq.empty
      }
    })
  }

  /**
   *  Given a LogicalPlan, the function finds a Seq of all Child nodes, and returns a Set of ColumnFilters for
   *  each of the Leaf node
   *
   * @param logicalPlan the root LogicalPlan
   * @return Seq of Set of Column filters, Seq has size same as the number of leaf nodes
   */
  def getColumnFilterGroup(logicalPlan: LogicalPlan): Seq[Set[ColumnFilter]] = {
    LogicalPlan.findLeafLogicalPlans(logicalPlan) map { lp =>
      lp match {
        case lp: LabelValues           => lp.filters toSet
        case lp: LabelNames            => lp.filters toSet
        case lp: RawSeries             => lp.filters toSet
        case lp: RawChunkMeta          => lp.filters toSet
        case lp: SeriesKeysByFilters   => lp.filters toSet
        case lp: LabelCardinality      => lp.filters.toSet
        case lp: TsCardinalities       => lp.filters().toSet
        case _: ScalarTimeBasedPlan    => Set.empty[ColumnFilter] // Plan does not have labels
        case _: ScalarFixedDoublePlan  => Set.empty[ColumnFilter]
        case _: ScalarBinaryOperation  => Set.empty[ColumnFilter]
        case _                         => throw new BadQueryException(s"Invalid logical plan $logicalPlan")
      }
    } match {
      case groupSeq: Seq[Set[ColumnFilter]] =>
        if (groupSeq.isEmpty || groupSeq.forall(_.isEmpty)) Seq.empty else groupSeq
      case _ => Seq.empty
    }
  }

  /**
   * Given a LogicalPlan, the function finds a Seq of all Child nodes,and returns a Set of ColumnFilters.
   * It filters out the column filter group for all the scalar leaf plans
   *
   * @param logicalPlan the root LogicalPlan
   * @return Seq[ColumnFilterListWithLogicalPlan], Seq has size same as the number of leaf nodes
   */
  def getColumnFilterGroupFilteringLeafScalarPlans(logicalPlan: LogicalPlan): Seq[Set[ColumnFilter]] = {
    LogicalPlan.findLeafLogicalPlans(logicalPlan) map { lp =>
      lp match {
        case lp: LabelValues => (lp.filters toSet, true)
        case lp: LabelNames => (lp.filters toSet, true)
        case lp: RawSeries => (lp.filters toSet, true)
        case lp: RawChunkMeta => (lp.filters toSet, true)
        case lp: SeriesKeysByFilters => (lp.filters toSet, true)
        case lp: LabelCardinality => (lp.filters.toSet, true)
        case lp: TsCardinalities => (lp.filters().toSet, true)
        case _: ScalarTimeBasedPlan => (Set.empty[ColumnFilter], false)
        case _: ScalarFixedDoublePlan => (Set.empty[ColumnFilter], false)
        case _: ScalarBinaryOperation => (Set.empty[ColumnFilter], false)
        case _ => throw new BadQueryException(s"Invalid logical plan $logicalPlan")
      }
    } match {
      case groupSeq: Seq[(Set[ColumnFilter], Boolean)] =>
        if (groupSeq.isEmpty || groupSeq.forall( x => x._1.isEmpty)) Seq.empty else {
          // filter out the scalar plans with empty column filter group
          // and only include the column filter of raw leaf plans
          groupSeq.filter(y => y._2)
            .map(x => x._1)
        }
      case _ => Seq.empty
    }
  }

  def getRawSeriesFilters(logicalPlan: LogicalPlan): Seq[Seq[ColumnFilter]] = {
    LogicalPlan.findLeafLogicalPlans(logicalPlan).map { l =>
      l match {
        case lp: RawSeries    => lp.filters
        case lp: LabelValues  => lp.filters
        case lp: LabelNames  => lp.filters
        case _                => Seq.empty
      }
    }
  }

  /**
   * Returns all nonMetricShardKey column filters
   */
  def getNonMetricShardKeyFilters(logicalPlan: LogicalPlan,
                                  nonMetricShardColumns: Seq[String]): Seq[Seq[ColumnFilter]] =
    getRawSeriesFilters(logicalPlan).map { s => s.filter(f => nonMetricShardColumns.contains(f.column))}

  /**
   * Returns true when all shard key filters have Equals
   */
  def hasShardKeyEqualsOnly(logicalPlan: LogicalPlan, nonMetricShardColumns: Seq[String]): Boolean =
    getNonMetricShardKeyFilters(logicalPlan: LogicalPlan, nonMetricShardColumns: Seq[String]).
      forall(_.forall(f => f.filter.isInstanceOf[filodb.core.query.Filter.Equals]))

  /**
   *  Given a Logical Plan, the method returns a RVRange, there are two cases possible
   *  1. The given plan is a PeriodicPlan in which case the start, end and step are retrieved from the plan instance
   *  2. If plan is not a PeriodicPlan then start step and end are irrelevant for the plan and None is returned
   *
   * @param plan: The Logical Plan instance
   * @param qContext: The query context
   * @return Option of the RVRange instance
   */
  def rvRangeFromPlan(plan: LogicalPlan): Option[RvRange] = {
    plan match {
      case p: PeriodicSeriesPlan   => Some(RvRange( startMs = p.startMs,
                                                    endMs = p.endMs,
                                                    stepMs = Math.max(p.stepMs, 1)))
      case _                       => None
    }
  }

  /**
   * @param base the column filters to override
   * @param overrides the overriding column filters
   * @return union of base and override filters except where column names intersect;
   *         when names intersect, only the overriding filter is included.
   *         Example:
   *           base = [(name=a, filter=1), (name=b, filter=2)]
   *           overrides = [(name=c, filter=3), (name=a, filter=4)]
   *           result = [(name=a, filter=4), (name=b, filter=2), (name=c, filter=3)]
   */
  def overrideColumnFilters(base: Seq[ColumnFilter],
                            overrides: Seq[ColumnFilter]): Seq[ColumnFilter] = {
    if (base.nonEmpty && !base.map(_.column).contains(TsCardinalities.LABEL_WORKSPACE)) {
      val overrideColumns = overrides.map(_.column)
      base.filterNot(f => overrideColumns.contains(f.column)) ++ overrides
    } else {
      base
    }
  }
}

//scalastyle:on number.of.types
//scalastyle:on file.size.limit
