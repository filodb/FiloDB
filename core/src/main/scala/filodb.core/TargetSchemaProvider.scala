package filodb.core

import filodb.core.query.ColumnFilter

trait TargetSchemaProvider {
  def targetSchemaFunc(filter: Seq[ColumnFilter]): Seq[String]
}

final case class StaticTargetSchemaProvider(targetSchema: Seq[String] = Seq.empty) extends TargetSchemaProvider {
  def targetSchemaFunc(filter: Seq[ColumnFilter]): Seq[String] = targetSchema
}
