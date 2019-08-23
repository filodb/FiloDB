package filodb.query.util

import filodb.core.query
import filodb.core.query.{ColumnFilter, Filter}
import filodb.query._

object LogicalPlanUtil {

  private def getFilterValue(filter: Filter): Set[String] = {
    filter match {
      case f: query.Filter.Equals => Set(f.value.toString)
      case f: query.Filter.And => getFilterValue(f.left).union(getFilterValue(f.right))
      case f: query.Filter.In => f.values.map(_.toString)
      case f: query.Filter.NotEquals => Set(f.value.toString)
      case f: query.Filter.NotEqualsRegex => Set(f.value.toString)
      case f: query.Filter.EqualsRegex => Set(f.value.toString)
    }
  }

 private def getLabelFromFilters(filters: Seq[ColumnFilter], metricColumnName: String): Option[Set[String]] = {
   val matchingFilters = filters.filter(_.column.equals(metricColumnName))
    if (matchingFilters.isEmpty)
      None
    else
      Some(getFilterValue(matchingFilters.head.filter))
  }

  def getLabelNameFromLogicalPlan(logicalPlan: LogicalPlan, labelValue: String): Option[Set[String]] = {

    logicalPlan match {
      case lp: PeriodicSeries => getLabelNameFromLogicalPlan(lp.rawSeries, labelValue)
      case lp: PeriodicSeriesWithWindowing => getLabelNameFromLogicalPlan(lp.rawSeries, labelValue)
      case lp: ApplyInstantFunction => getLabelNameFromLogicalPlan(lp.vectors, labelValue)
      case lp: Aggregate => getLabelNameFromLogicalPlan(lp.vectors, labelValue)
      case lp: BinaryJoin => val lhs = getLabelNameFromLogicalPlan(lp.lhs, labelValue)
        val rhs = getLabelNameFromLogicalPlan(lp.rhs, labelValue)
        if (lhs.isEmpty)
          rhs
        else if (rhs.isEmpty)
          lhs
        else
          Some(lhs.get.union(rhs.get))
      case lp: ScalarVectorBinaryOperation => getLabelNameFromLogicalPlan(lp.vector, labelValue)
      case lp: ApplyMiscellaneousFunction => getLabelNameFromLogicalPlan(lp.vectors, labelValue)
      case lp: LabelValues => val label = lp.labelConstraints.filter(_._2.equals(labelValue)).toList
                              if (label.isEmpty)
                                None
                              else
                                Some(Set(label.head._1))
      case lp: RawSeries => getLabelFromFilters(lp.filters, labelValue)
      case lp: RawChunkMeta => getLabelFromFilters(lp.filters, labelValue)
      case lp: SeriesKeysByFilters => getLabelFromFilters(lp.filters, labelValue)
      case _ => throw new BadQueryException("Invalid logical plan")
    }
  }
}
