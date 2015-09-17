package filodb.core.metadata

import filodb.core.SortKeyHelper
import filodb.core.Types._

/**
 * A Projection defines one particular view of a dataset, designed to be optimized for a particular query.
 * It usually defines a sort order and subset of the columns.
 *
 * By convention, projection 0 is the SuperProjection which consists of all columns from the dataset.
 *
 * The Projection base class is normalized, ie it doesn't have all the information.
 */
case class Projection(id: Int,
                      dataset: TableName,
                      // TODO: support multiple sort columns
                      sortColumn: ColumnId,
                      reverse: Boolean = false,
                      // Nil columns means all columns
                      columns: Seq[ColumnId] = Nil,
                      // Probably not necessary in the future
                      segmentSize: String = "10000")

/**
 * This is a Projection with information filled out from Dataset and Columns.
 * ie it has the actual Dataset and Column types as opposed to IDs, and a list of all columns.
 */
case class RichProjection(id: Int,
                          dataset: Dataset,
                          sortColumn: Column,
                          reverse: Boolean,
                          columns: Seq[Column]) {
  def helper[K]: SortKeyHelper[K] = Dataset.sortKeyHelper(dataset, sortColumn).get
}

object RichProjection {
  // TODO: error handling
  def apply(dataset: Dataset, columns: Seq[Column], projectionId: Int = 0): RichProjection = {
    val normProjection = dataset.projections(projectionId)
    val richColumns = {
      if (normProjection.columns.isEmpty) {
        columns
      } else {
        val columnMap = columns.map { c => c.name -> c }.toMap
        normProjection.columns.map(columnMap)
      }
    }
    val sortColumn = richColumns.filter(_.hasId(normProjection.sortColumn)).head
    RichProjection(projectionId, dataset, sortColumn, normProjection.reverse, richColumns)
  }
}