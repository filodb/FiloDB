package filodb.core.metadata

import filodb.core.KeyType
import filodb.core.Types._
import org.velvia.filo.{RowReader, VectorInfo}


/**
 * This is a Projection with information filled out from Dataset and Columns.
 * ie it has the actual Dataset and Column types as opposed to IDs, and a list of all columns.
 * It is also guaranteed to have a valid sortColumn and dataset partition column, and will
 * have a SortKeyHelper and partitioning function as well.
 */
case class ProjectionInfo[R, S](id: Int,
                                dataset: String,
                                keyColumn: Column,
                                keyColNo: Int,
                                sortColumn: Column,
                                sortColNo: Int,
                                reverse: Boolean,
                                columns: Seq[Column],
                                rowKeyType: KeyType[R],
                                segmentType: KeyType[S],
                                partitionFunc: RowReader => PartitionKey) {

  def segmentFunction: RowReader => S = segmentType.getKeyFunc(Seq(sortColNo))

  def rowKeyFunction: RowReader => R = rowKeyType.getKeyFunc(Seq(keyColNo))

  def filoSchema: Seq[VectorInfo] = columns.map {
    case Column(name, _, _, colType, serializer, false, false) =>
      require(serializer == Column.Serializer.FiloSerializer)
      VectorInfo(name, colType.clazz)
  }

}

