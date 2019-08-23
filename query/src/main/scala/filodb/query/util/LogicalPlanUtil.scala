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

 private def getLabelValueFromFilters(filters: Seq[ColumnFilter], metricColumnName: String): Option[Set[String]] = {
   val matchingFilters = filters.filter(_.column.equals(metricColumnName))
    if (matchingFilters.isEmpty)
      None
    else
      Some(getFilterValue(matchingFilters.head.filter))
  }

  def getLabelValueFromLogicalPlan(logicalPlan: LogicalPlan, labelName: String): Option[Set[String]] = {

    logicalPlan match {
      case lp: PeriodicSeries => getLabelValueFromLogicalPlan(lp.rawSeries, labelName)
      case lp: PeriodicSeriesWithWindowing => getLabelValueFromLogicalPlan(lp.rawSeries, labelName)
      case lp: ApplyInstantFunction => getLabelValueFromLogicalPlan(lp.vectors, labelName)
      case lp: Aggregate => getLabelValueFromLogicalPlan(lp.vectors, labelName)
      case lp: BinaryJoin => val lhs = getLabelValueFromLogicalPlan(lp.lhs, labelName)
        val rhs = getLabelValueFromLogicalPlan(lp.rhs, labelName)
        if (lhs.isEmpty)
          rhs
        else if (rhs.isEmpty)
          lhs
        else
          Some(lhs.get.union(rhs.get))
      case lp: ScalarVectorBinaryOperation => getLabelValueFromLogicalPlan(lp.vector, labelName)
      case lp: ApplyMiscellaneousFunction => getLabelValueFromLogicalPlan(lp.vectors, labelName)
      case lp: LabelValues => val label = lp.labelConstraints.filter(_._2.equals(labelName)).toList
                              if (label.isEmpty)
                                None
                              else
                                Some(Set(label.head._1))
      case lp: RawSeries => getLabelValueFromFilters(lp.filters, labelName)
      case lp: RawChunkMeta => getLabelValueFromFilters(lp.filters, labelName)
      case lp: SeriesKeysByFilters => getLabelValueFromFilters(lp.filters, labelName)
    }
  }
}
