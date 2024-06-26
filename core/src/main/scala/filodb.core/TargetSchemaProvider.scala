package filodb.core

import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.utils.ColumnFilterMap

trait TargetSchemaProvider {
  def targetSchemaFunc(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange]
}

final case class StaticTargetSchemaProvider(targetSchemaOpt: Option[Seq[String]] = None) extends TargetSchemaProvider {
  def targetSchemaFunc(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange] = {
    targetSchemaOpt.map(tschema => Seq(TargetSchemaChange(0, tschema))).getOrElse(Nil)
  }
}

/**
 * A TargetSchemaProvider backed by a ColumnFilterMap.
 */
final case class ColumnFilterMapTargetSchemaProvider(columnFilterMap: ColumnFilterMap[Seq[TargetSchemaChange]])
  extends TargetSchemaProvider {

  /**
   * Given a label->value map, returns an applicable set of
   *   target-schema changes (if one exists).
   */
  def targetSchemaFunc(labelValues: Map[String, String]): Seq[TargetSchemaChange] = {
    columnFilterMap.get(labelValues).getOrElse(Nil)
  }

  /**
   * NOTE!: this implementation only considers Equals filters.
   * All other filters are ignored.
   */
  override def targetSchemaFunc(columnFilters: Seq[ColumnFilter]): Seq[TargetSchemaChange] = {
    val equalsMap = columnFilters
      .filter(_.filter.isInstanceOf[Filter.Equals])
      .map(f => f.column -> f.filter.asInstanceOf[Filter.Equals].value.toString)
      .toMap
    this.targetSchemaFunc(equalsMap)
  }
}

object ColumnFilterMapTargetSchemaProvider {
  /**
   * @param filterChangePairs (filters,changes) pairs used to populate the backing ColumnFilterMap;
   *                          at most one of 'filters' can be a non-Equals filter.
   */
  def apply(filterChangePairs: Iterable[(
      Iterable[ColumnFilter],
      Iterable[TargetSchemaChange]
    )]): ColumnFilterMapTargetSchemaProvider = {
    val columnFilterMap = {
      val sortedFilterChangePairs = filterChangePairs.map { case (filters, changes) =>
        (filters, changes.toSeq.sortBy(_.time))
      }
      new ColumnFilterMap[Seq[TargetSchemaChange]](sortedFilterChangePairs)
    }
    new ColumnFilterMapTargetSchemaProvider(columnFilterMap)
  }
}

final case class TargetSchemaChange(time: Long = 0L, schema: Seq[String])
