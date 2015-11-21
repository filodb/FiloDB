package filodb.core.metadata

import filodb.core.KeyType
import filodb.core.Types._
import filodb.core.store.MetaStore.BadSchema
import org.velvia.filo.{RowReader, VectorInfo}


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

  val schemaMap = schema.map(i => (i.name -> i)).toMap

  val columnIndexes = schema.zipWithIndex.map { case (c, i) => c.name -> i }.toMap


  def filoSchema: Seq[VectorInfo] = schema.map {
    case Column(name, _, _, colType, serializer, false, false) =>
      require(serializer == Column.Serializer.FiloSerializer)
      VectorInfo(name, colType.clazz)
  }

  // scalastyle:off

  val partitionFunction = keyFunction[partitionType.T](
    partitionType.asInstanceOf[KeyType {type T = partitionType.T}],
    partitionColumns)

  val keyFunction = keyFunction[keyType.T](
    keyType.asInstanceOf[KeyType {type T = keyType.T}],
    keyColumns)

  val sortFunction = keyFunction[sortType.T](
    sortType.asInstanceOf[KeyType {type T = sortType.T}],
    sortColumns)

  val segmentFunction = keyFunction[segmentType.T](
    segmentType.asInstanceOf[KeyType {type T = segmentType.T}],
    segmentColumns)

  def keyFunction[R](keyType: KeyType {type T = R}, columns: Seq[ColumnId]): RowReader => R = {
    val keyColNos = columns.map(col => columnIndexes.getOrElse(col, throw BadSchema("Invalid column $col")))
    keyType.getKeyFunc(keyColNos)
  }

  // scalastyle:on


}

