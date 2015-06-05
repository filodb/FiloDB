package filodb.spark

import org.apache.spark.sql.types._

import filodb.core.metadata.Column

object TypeConverters {
  val colTypeToSqlType: PartialFunction[Column.ColumnType, DataType] = {
    case Column.ColumnType.IntColumn    => IntegerType
    case Column.ColumnType.DoubleColumn => DoubleType
    case Column.ColumnType.LongColumn   => LongType
    case Column.ColumnType.StringColumn => StringType
    case Column.ColumnType.BitmapColumn => BooleanType
  }

  def columnsToSqlFields(columns: Seq[Column]): Seq[StructField] =
    columns.map { case Column(name, _, _, colType, _, false, false) =>
      StructField(name, colTypeToSqlType(colType), true)
    }

  def columnsToSqlTypes(columns: Seq[Column]): Seq[DataType] =
    columns.map { column => colTypeToSqlType(column.columnType) }
}
