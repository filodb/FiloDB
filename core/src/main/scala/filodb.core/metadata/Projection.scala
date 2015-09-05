package filodb.core.metadata

import filodb.core.Types._

/**
 * A Projection defines one particular view of a dataset, designed to be optimized for a particular query.
 * It usually defines a sort order and subset of the columns.
 *
 * By convention, projection 0 is the SuperProjection which consists of all columns from the dataset.
 */
case class Projection(id: Int,
                      // TODO: support multiple sort columns
                      sortColumn: ColumnId,
                      reverse: Boolean = false,
                      // Nil columns means all columns
                      columns: Seq[ColumnId] = Nil,
                      // Probably not necessary in the future
                      segmentSize: String = "10000")