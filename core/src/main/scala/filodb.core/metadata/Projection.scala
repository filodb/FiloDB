package filodb.core.metadata

import org.velvia.filo.RowReader
import scala.util.{Try, Success, Failure}

import filodb.core.KeyType
import filodb.core.Types._

/**
 * A Projection defines one particular view of a dataset, designed to be optimized for a particular query.
 * It usually defines a sort order and subset of the columns.
 * Within a partition, **key columns** define a unique primary key for each record.
 * Records/rows are grouped by the **segment key** into segments.
 * Projections are sorted by the **segment key**.
 *
 * By convention, projection 0 is the SuperProjection which consists of all columns from the dataset.
 *
 * The Projection base class is normalized, ie it doesn't have all the information.
 */
case class Projection(id: Int,
                      dataset: TableName,
                      keyColIds: Seq[ColumnId],
                      segmentColId: ColumnId,
                      reverse: Boolean = false,
                      // Nil columns means all columns
                      // Must include the keyColumns and segmentColumn.
                      columns: Seq[ColumnId] = Nil)

/**
 * This is a Projection with information filled out from Dataset and Columns.
 * ie it has the actual Dataset and Column types as opposed to IDs, and a list of all columns.
 * It is also guaranteed to have a valid segmentColumn and dataset partition columns.
 */
case class RichProjection(projection: Projection,
                          dataset: Dataset,
                          columns: Seq[Column],
                          segmentColumn: Column,
                          segmentColIndex: Int,
                          segmentType: KeyType,
                          keyColumns: Seq[Column],
                          keyColIndices: Seq[Int],
                          keyType: KeyType,
                          partitionColumns: Seq[Column],
                          partitionColIndices: Seq[Int],
                          partitionType: KeyType) {
  def datasetName = projection.dataset

  def segmentKeyFunc: RowReader => segmentType.T =
    segmentType.getKeyFunc(Array(segmentColIndex))

  def keyKeyFunc: RowReader => keyType.T =
    keyType.getKeyFunc(keyColIndices.toArray)

  // Returns the partition key function, checking to see if a default partition key
  // should be returned.
  // TODO(velvia): get rid of default partition key when computed columns are introduced.
  def partitionKeyFunc: RowReader => partitionType.T =
    if (dataset.partitionColumns == Seq(Dataset.DefaultPartitionColumn)) {
      (row: RowReader) => Dataset.DefaultPartitionKey.asInstanceOf[partitionType.T]
    } else {
      partitionType.getKeyFunc(partitionColIndices.toArray)
    }
}

object RichProjection {
  case class BadSchema(reason: String) extends Exception("BadSchema: " + reason)

  def apply(dataset: Dataset, columns: Seq[Column], projectionId: Int = 0): RichProjection =
    make(dataset, columns, projectionId).get

  def make(dataset: Dataset, columns: Seq[Column], projectionId: Int = 0): Try[RichProjection] = {
    def fail(reason: String): Try[RichProjection] = Failure(BadSchema(reason))

    if (projectionId >= dataset.projections.length) return fail(s"projectionId $projectionId missing")

    val normProjection = dataset.projections(projectionId)
    val richColumns = {
      if (normProjection.columns.isEmpty) {
        columns
      } else {
        val columnMap = columns.map { c => c.name -> c }.toMap
        val missing = normProjection.columns.toSet -- columnMap.keySet
        if (missing.nonEmpty) return fail(s"Specified projection columns are missing: $missing")
        normProjection.columns.map(columnMap)
      }
    }

    val idToIndex = richColumns.zipWithIndex.map { case (col, i) => col.name -> i }.toMap

    val segmentColIndex = idToIndex.getOrElse(normProjection.segmentColId,
      return fail(s"Segment column ${normProjection.segmentColId} not in columns $richColumns"))
    val segmentColumn = richColumns(segmentColIndex)

    val keyColIndices = normProjection.keyColIds.map { colId =>
      idToIndex.getOrElse(colId, return fail(s"Key column $colId not in columns $richColumns"))
    }
    val keyColumns = keyColIndices.map(richColumns)

    val partitionColIndices = dataset.partitionColumns.map { colId =>
      idToIndex.getOrElse(colId, return fail(s"Partition column $colId not in columns $richColumns"))
    }
    val partitionColumns = partitionColIndices.map(richColumns)

    for { segmentType <- KeyType.getKeyType(segmentColumn.columnType.clazz)
          keyType     <- Column.columnsToKeyType(keyColumns)
          partitionType <- Column.columnsToKeyType(partitionColumns) } yield {
      RichProjection(normProjection, dataset, richColumns,
                     segmentColumn, segmentColIndex, segmentType,
                     keyColumns, keyColIndices, keyType,
                     partitionColumns, partitionColIndices, partitionType)
    }
  }
}