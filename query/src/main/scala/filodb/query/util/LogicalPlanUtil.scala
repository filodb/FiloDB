package filodb.query.util

import filodb.core.query.ColumnFilter
import filodb.query._

object LogicalPlanUtil {

  private def getLabelValueFromFilters(filters: Seq[ColumnFilter], metricColumnName: String): Option[Set[String]] = {
    val matchingFilters = filters.filter(_.column.equals(metricColumnName))
    if (matchingFilters.isEmpty)
      None
    else
     Some(matchingFilters.head.filter.valuesStrings.map(_.toString))
  }

  def getLabelValueFromLogicalPlan(logicalPlan: LogicalPlan, labelName: String): Option[Set[String]] = {
    val labelValues = LogicalPlan.findLeafLogicalPlans(logicalPlan).flatMap { lp =>
      lp match {
        case lp: LabelValues         => lp.labelConstraints.get(labelName).map(Set(_))
        case lp: RawSeries           => getLabelValueFromFilters(lp.filters, labelName)
        case lp: RawChunkMeta        => getLabelValueFromFilters(lp.filters, labelName)
        case lp: SeriesKeysByFilters => getLabelValueFromFilters(lp.filters, labelName)
        case _                       => throw new BadQueryException("Invalid logical plan")
      }
    }
    if (labelValues.isEmpty) {
      None
    } else {
      var res: Set[String] = Set()
      Some(labelValues.foldLeft(res) { (acc, i) => i.union(acc) })
    }
  }
}
