package filodb.core.metadata

import org.velvia.filo.RowReader
import scala.util.{Try, Success, Failure}

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
 * It is also guaranteed to have a valid sortColumn and dataset partition column, and will
 * have a SortKeyHelper and partitioning function as well.
 */
case class RichProjection[K](id: Int,
                             dataset: Dataset,
                             sortColumn: Column,
                             sortColNo: Int,
                             reverse: Boolean,
                             columns: Seq[Column],
                             helper: SortKeyHelper[K],
                             partitionFunc: RowReader => PartitionKey) {
  def sortKeyFunc: RowReader => K = helper.getSortKeyFunc(Seq(sortColNo))
}

object RichProjection {
  case class BadSchema(reason: String) extends Exception("BadSchema: " + reason)

  def apply[K](dataset: Dataset, columns: Seq[Column], projectionId: Int = 0): RichProjection[K] =
    make[K](dataset, columns, projectionId).get

  def make[K](dataset: Dataset, columns: Seq[Column], projectionId: Int = 0): Try[RichProjection[K]] = {
    def fail(reason: String): Try[RichProjection[K]] = Failure(BadSchema(reason))

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

    val sortColNo = richColumns.indexWhere(_.hasId(normProjection.sortColumn))
    if (sortColNo < 0) return fail(s"Sort column ${normProjection.sortColumn} not in columns $richColumns")

    val sortColumn = richColumns(sortColNo)
    if (!(SortKeyHelper.ValidSortClasses contains sortColumn.columnType.clazz)) {
      return fail(s"Unsupported sort column type ${sortColumn.columnType}")
    }

    val helper = Dataset.sortKeyHelper[K](dataset, sortColumn).get

    for { partitionFunc <- Dataset.getPartitioningFunc(dataset, richColumns) } yield {
      RichProjection[K](projectionId, dataset, sortColumn, sortColNo,
                        normProjection.reverse, richColumns, helper, partitionFunc)
    }
  }
}