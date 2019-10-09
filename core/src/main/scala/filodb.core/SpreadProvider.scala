package filodb.core

import filodb.core.query.ColumnFilter

trait SpreadProvider {
  def spreadFunc(filter: Seq[ColumnFilter]): Seq[SpreadChange]
}

final case class SpreadChange(time: Long = 0L, spread: Int = 1)