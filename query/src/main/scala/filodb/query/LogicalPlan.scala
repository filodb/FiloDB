package filodb.query

import filodb.core.query.{ColumnFilter, RangeParams}

//scalastyle:off number.of.types
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

  /**
    * Replace filters present in logical plan
    */
  def replaceFilters(filters: Seq[ColumnFilter]): LogicalPlan = {
    this match {
      case p: PeriodicSeriesPlan  => p.replacePeriodicSeriesFilters(filters)
      case r: RawSeriesLikePlan   => r.replaceRawSeriesFilters(filters)
      case l: LabelValues         => l.copy(filters = filters)
      case s: SeriesKeysByFilters => s.copy(filters = filters)
    }
  }
}

/**
  * Super class for a query that results in range vectors with raw samples (chunks),
  * or one simple transform from the raw data.  This data is likely non-periodic or at least
  * not in the same time cadence as user query windowing.
  */
sealed trait RawSeriesLikePlan extends LogicalPlan {
  def isRaw: Boolean = false
  def replaceRawSeriesFilters(newFilters: Seq[ColumnFilter]): RawSeriesLikePlan
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
}

sealed trait MetadataQueryPlan extends LogicalPlan {
  override def isTimeSplittable: Boolean = false
}

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
                     offsetMs: Option[Long] = None) extends RawSeriesLikePlan {
  override def isRaw: Boolean = true

  override def replaceRawSeriesFilters(newFilters: Seq[ColumnFilter]): RawSeriesLikePlan = {
    val filterColumns = newFilters.map(_.column)
    val updatedFilters = this.filters.filterNot(f => filterColumns.contains(f.column)) ++ newFilters
    this.copy(filters = updatedFilters)
  }
}

case class LabelValues(labelNames: Seq[String],
                       filters: Seq[ColumnFilter],
                       startMs: Long,
                       endMs: Long) extends MetadataQueryPlan

case class SeriesKeysByFilters(filters: Seq[ColumnFilter],
                               fetchFirstLastSampleTimes: Boolean,
                               startMs: Long,
                               endMs: Long) extends MetadataQueryPlan

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
                          offsetMs: Option[Long] = None) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(rawSeries)

  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this.copy(rawSeries =
    rawSeries.replaceRawSeriesFilters(filters))
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
                                       offsetMs: Option[Long] = None) extends PeriodicSeriesPlan
  with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(series)

  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this.copy(series =
    series.replaceRawSeriesFilters(filters))
}

/**
  * Aggregate data across partitions (not in the time dimension).
  * Aggregation can be done only on range vectors with consistent
  * sampling interval.
  * @param by columns to group by
  * @param without columns to leave out while grouping
  */
case class Aggregate(operator: AggregationOperator,
                     vectors: PeriodicSeriesPlan,
                     params: Seq[Any] = Nil,
                     by: Seq[String] = Nil,
                     without: Seq[String] = Nil) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(vectors)
  override def startMs: Long = vectors.startMs
  override def stepMs: Long = vectors.stepMs
  override def endMs: Long = vectors.endMs
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this.copy(vectors =
    vectors.replacePeriodicSeriesFilters(filters))
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
                      on: Seq[String] = Nil,
                      ignoring: Seq[String] = Nil,
                      include: Seq[String] = Nil) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  require(lhs.startMs == rhs.startMs)
  require(lhs.endMs == rhs.endMs)
  require(lhs.stepMs == rhs.stepMs)
  override def children: Seq[LogicalPlan] = Seq(lhs, rhs)
  override def startMs: Long = lhs.startMs
  override def stepMs: Long = lhs.stepMs
  override def endMs: Long = lhs.endMs
  override def isRoutable: Boolean = lhs.isRoutable && rhs.isRoutable
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this.copy(lhs =
    lhs.replacePeriodicSeriesFilters(filters), rhs = rhs.replacePeriodicSeriesFilters(filters))
}

/**
  * Apply Scalar Binary operation to a collection of RangeVectors
  */
case class ScalarVectorBinaryOperation(operator: BinaryOperator,
                                       scalarArg: ScalarPlan,
                                       vector: PeriodicSeriesPlan,
                                       scalarIsLhs: Boolean) extends PeriodicSeriesPlan with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(vector)
  override def startMs: Long = vector.startMs
  override def stepMs: Long = vector.stepMs
  override def endMs: Long = vector.endMs
  override def isRoutable: Boolean = vector.isRoutable
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this.copy(vector =
    vector.replacePeriodicSeriesFilters(filters))
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
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this.copy(vectors =
    vectors.replacePeriodicSeriesFilters(filters))
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
  override def replaceRawSeriesFilters(newFilters: Seq[ColumnFilter]): RawSeriesLikePlan = this.copy(vectors =
    vectors.replaceRawSeriesFilters(newFilters).asInstanceOf[RawSeries])
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
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this.copy(vectors =
    vectors.replacePeriodicSeriesFilters(filters))
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
    val updatedLhs = if (lhs.isRight) Right(lhs.right.get.replacePeriodicSeriesFilters(filters).
                      asInstanceOf[ScalarBinaryOperation]) else Left(lhs.left.get)
    val updatedRhs = if (rhs.isRight) Right(rhs.right.get.replacePeriodicSeriesFilters(filters).
                      asInstanceOf[ScalarBinaryOperation]) else Left(rhs.left.get)
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
  override def replacePeriodicSeriesFilters(filters: Seq[ColumnFilter]): PeriodicSeriesPlan = this.copy(vectors =
    vectors.replacePeriodicSeriesFilters(filters))
}

object LogicalPlan {
  /**
    * Get leaf Logical Plans
    */
  def findLeafLogicalPlans (logicalPlan: LogicalPlan) : Seq[LogicalPlan] = {
   logicalPlan match {
     // scalarArg can have vector like scalar(http_requests_total)
     case lp: ScalarVectorBinaryOperation => findLeafLogicalPlans(lp.vector) ++ findLeafLogicalPlans(lp.scalarArg)
     // Find leaf logical plans for all children and concatenate results
     case lp: NonLeafLogicalPlan          => lp.children.flatMap(findLeafLogicalPlans)
     case lp: MetadataQueryPlan           => Seq(lp)
     case lp: ScalarBinaryOperation       => val lhsLeafs = if (lp.lhs.isRight) findLeafLogicalPlans(lp.lhs.right.get)
                                                             else Nil
                                             val rhsLeafs = if (lp.rhs.isRight) findLeafLogicalPlans(lp.rhs.right.get)
                                                             else Nil
                                             lhsLeafs ++ rhsLeafs
     case lp: ScalarFixedDoublePlan       => Seq(lp)
     case lp: ScalarTimeBasedPlan         => Seq(lp)
     case lp: RawSeries                   => Seq(lp)
     case lp: RawChunkMeta                => Seq(lp)
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
      cFilter.column == labelName match {
        case true  => cFilter.filter.valuesStrings.map(_.toString)
        case false => Seq.empty
      }
    })
  }

  def getColumnFilterGroup(logicalPlan: LogicalPlan): Seq[Set[ColumnFilter]] = {
    LogicalPlan.findLeafLogicalPlans(logicalPlan) map { lp =>
      lp match {
        case lp: LabelValues           => lp.filters toSet
        case lp: RawSeries             => lp.filters toSet
        case lp: RawChunkMeta          => lp.filters toSet
        case lp: SeriesKeysByFilters   => lp.filters toSet
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

  def getRawSeriesFilters(logicalPlan: LogicalPlan): Seq[Seq[ColumnFilter]] = {
    LogicalPlan.findLeafLogicalPlans(logicalPlan).map { l =>
      l match {
        case lp: RawSeries    => lp.filters
        case lp: LabelValues  => lp.filters
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

}
//scalastyle:on number.of.types