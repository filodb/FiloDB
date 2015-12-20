package filodb.core.metadata

import filodb.core.KeyType
import filodb.core.Types._
import filodb.core.store.MetaStore.BadSchema
import org.velvia.filo.{RowReader, VectorInfo}


object Projection {
  def toFiloSchema(schema: Seq[Column]): Seq[VectorInfo] = schema.map {
    case Column(name, _, _, colType, serializer, false, false) =>
      VectorInfo(name, colType.clazz)
    case _ => throw new IllegalArgumentException("Need schema to be composed of columns")
  }
}

/**
 * This is a Projection with information filled out from Dataset and Columns.
 * ie it has the actual Dataset and Column types as opposed to IDs, and a list of all columns.
 * It is also guaranteed to have a valid sortColumn and dataset partition column, and will
 * have a SortKeyHelper and partitioning function as well.
 */
case class Projection(id: Int,
                      dataset: String,
                      reverse: Boolean,
                      schema: Seq[Column],
                      partitionColumns: Seq[ColumnId],
                      keyColumns: Seq[ColumnId],
                      sortColumns: Seq[ColumnId],
                      segmentColumns: Seq[ColumnId],
                      partitionType: KeyType,
                      keyType: KeyType,
                      sortType: KeyType,
                      segmentType: KeyType) {

  val schemaMap = schema.map(c => c.name -> c).toMap
  val columnIndexes = schema.zipWithIndex.map { case (c, i) => c.name -> i }.toMap
  val columnNames = schema.map(c => c.name)

  // scalastyle:off

  def partitionFunction(columnIndexes: Map[String, Int]) = getKeyFunction[partitionType.T](
    partitionType.asInstanceOf[KeyType {type T = partitionType.T}],
    partitionColumns, columnIndexes)

  def keyFunction(columnIndexes: Map[String, Int]) = getKeyFunction[keyType.T](
    keyType.asInstanceOf[KeyType {type T = keyType.T}],
    keyColumns, columnIndexes)

  def sortFunction(columnIndexes: Map[String, Int]) = getKeyFunction[sortType.T](
    sortType.asInstanceOf[KeyType {type T = sortType.T}],
    sortColumns, columnIndexes)

  def segmentFunction(columnIndexes: Map[String, Int]) = getKeyFunction[segmentType.T](
    segmentType.asInstanceOf[KeyType {type T = segmentType.T}],
    segmentColumns, columnIndexes)

  def getKeyFunction[R](keyType: KeyType {type T = R},
                        columns: Seq[ColumnId],
                        columnIndexes: Map[String, Int]): RowReader => R = {
    val keyColNos = columns.map(col => columnIndexes.getOrElse(col, throw BadSchema("Invalid column $col")))
    keyType.getKeyFunc(keyColNos)
  }

  // scalastyle:on


}

