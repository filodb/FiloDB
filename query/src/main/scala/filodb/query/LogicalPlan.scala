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
}

/**
  * Super class for a query that results in range vectors with raw samples (chunks),
  * or one simple transform from the raw data.  This data is likely non-periodic or at least
  * not in the same time cadence as user query windowing.
  */
sealed trait RawSeriesLikePlan extends LogicalPlan {
  override def isRoutable: Boolean = false
  def isRaw: Boolean = false
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
}

sealed trait MetadataQueryPlan extends LogicalPlan {
  override def isRoutable: Boolean = false
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
}

case class LabelValues(labelNames: Seq[String],
                       labelConstraints: Map[String, String],
                       lookbackTimeMs: Long) extends MetadataQueryPlan

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
}

/**
  * Concrete logical plan to query for data in a given range
  * with results in a regular time interval.
  *
  * Applies a range function on raw windowed data (perhaps with instant function applied) before
  * sampling data at regular intervals.
  */
case class PeriodicSeriesWithWindowing(series: RawSeriesLikePlan,
                                       startMs: Long,
                                       stepMs: Long,
                                       endMs: Long,
                                       window: Long,
                                       function: RangeFunctionId,
                                       functionArgs: Seq[FunctionArgsPlan] = Nil,
                                       offsetMs: Option[Long] = None) extends PeriodicSeriesPlan
  with NonLeafLogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(series)
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
}

/**
  * Nested logical plan for argument of function
  * Example: clamp_max(node_info{job = "app"},scalar(http_requests_total{job = "app"}))
  */
trait FunctionArgsPlan extends LogicalPlan

/**
  * Generate scalar
  * Example: scalar(http_requests_total), time(), hour()
  */
trait ScalarPlan extends LogicalPlan with PeriodicSeriesPlan with FunctionArgsPlan

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
}

case class LabelValueOperator(columnName: String, value: Seq[String], operator: String)

case class LabelValueOperatorGroup(labelValueOperators: Seq[LabelValueOperator])

object LogicalPlan {
  /**
    * Get leaf Logical Plans
    */
  def findLeafLogicalPlans (logicalPlan: LogicalPlan) : Seq[LogicalPlan] = {
   logicalPlan match {
     // Find leaf logical plans for all children and concatenate results
     case lp: NonLeafLogicalPlan => lp.children.flatMap(findLeafLogicalPlans)
     case _                      => Seq(logicalPlan)
   }
  }

  def getLabelValueFromLogicalPlan(logicalPlan: LogicalPlan, labelName: String): Option[Seq[String]] = {
    getLabelValueFromLogicalPlan(getLabelValueOperatorsFromLogicalPlan(logicalPlan), labelName)
  }

  def getLabelValueFromLogicalPlan(labelValues: Option[Seq[LabelValueOperatorGroup]],
                                   labelName: String): Option[Seq[String]] = {
    labelValues match {
      case None => None
      case _    => labelValues.get.flatMap(group =>
        group.labelValueOperators.flatMap(lops => {
          lops.columnName.equals(labelName) match {
            case true  => lops.value
            case false => Seq()
          }
        })).distinct match {
        case Nil                    => None
        case lVFilters: Seq[String] => Some(lVFilters)
      }
    }
  }

  private def getLabelValueOpsFromFilters(filters: Seq[ColumnFilter]): Option[LabelValueOperatorGroup] = {
    Some(LabelValueOperatorGroup(filters.map(cf => LabelValueOperator(cf.column,
      cf.filter.valuesStrings.map(_.toString).toSeq.sorted, cf.filter.operatorString))))
  }

  def getLabelValueOperatorsFromLogicalPlan(logicalPlan: LogicalPlan): Option[Seq[LabelValueOperatorGroup]] = {
    LogicalPlan.findLeafLogicalPlans(logicalPlan).flatMap { lp =>
      lp match {
        case lp: LabelValues           => Some(
          LabelValueOperatorGroup(
            lp.labelConstraints.map(lbc => LabelValueOperator(lbc._1, Seq(lbc._2), "=")).toSeq))
        case lp: RawSeries             => getLabelValueOpsFromFilters(lp.filters)
        case lp: RawChunkMeta          => getLabelValueOpsFromFilters(lp.filters)
        case lp: SeriesKeysByFilters   => getLabelValueOpsFromFilters(lp.filters)
        case _: ScalarTimeBasedPlan    => Nil // Plan does not have labels
        case _: ScalarFixedDoublePlan  => Nil
        case _: ScalarBinaryOperation  => Nil
        case _                         => throw new BadQueryException(s"Invalid logical plan $logicalPlan")
      }
    } match {
      case Nil => None
      case groupSeq: Seq[LabelValueOperatorGroup] => Some(groupSeq)
    }
  }
}
//scalastyle:on number.of.types