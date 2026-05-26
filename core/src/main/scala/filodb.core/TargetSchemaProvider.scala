package filodb.core

import filodb.core.query.ColumnFilter

trait TargetSchemaProvider {
  def targetSchemaFunc(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange]
}

final case class StaticTargetSchemaProvider(targetSchemaOpt: Option[Seq[String]] = None) extends TargetSchemaProvider {
  def targetSchemaFunc(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange] = {
    targetSchemaOpt.map(tschema => Seq(TargetSchemaChange(0, tschema))).getOrElse(Nil)
  }
}

final case class TargetSchemaChange(time: Long = 0L, schema: Seq[String])
