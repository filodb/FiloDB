package filodb.core

import filodb.core.query.ColumnFilter

trait TargetSchemaProvider {
  def targetSchemaFunc(filter: Seq[ColumnFilter]): Seq[TargetSchema]
}

final case class StaticTargetSchemaProvider(targetSchema: Seq[String] = Seq.empty) extends TargetSchemaProvider {
  def targetSchemaFunc(filter: Seq[ColumnFilter]): Seq[TargetSchema] = Seq(TargetSchema(0, targetSchema))
}

final case class TargetSchema(time: Long = 0L, schema: Seq[String])
