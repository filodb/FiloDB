package filodb.core

import filodb.core.query.ColumnFilter

trait TargetSchemaProvider {
  def targetSchemaFunc(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange]
}

final case class StaticTargetSchemaProvider(targetSchema: Seq[String] = Seq.empty) extends TargetSchemaProvider {
  def targetSchemaFunc(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange] = Seq(TargetSchemaChange(0, targetSchema))
}

final case class TargetSchemaChange(time: Long = 0L, schema: Seq[String])
