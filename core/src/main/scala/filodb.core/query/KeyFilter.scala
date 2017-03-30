package filodb.core.query

import org.scalactic._
import org.velvia.filo.{RowReader, SingleValueRowReader, ZeroCopyUTF8String, UTF8Wrapper}
import scala.language.postfixOps
import scalaxy.loops._

import filodb.core._
import filodb.core.Types.PartitionKey
import filodb.core.metadata.{Column, DataColumn, ComputedColumn, RichProjection}

/**
 * Utilities to generate functions to filter keys.
 */
object KeyFilter {
  import Requirements._

  def forceType(kt: KeyType, item: Any): kt.T = item match {
    case s: String => kt.fromString(s)
    case t: Any    => t.asInstanceOf[kt.T]
  }

  def equalsFunc(value: Any): Any => Boolean = (item: Any) => value == item

  def inFunc(values: Set[Any]): Any => Boolean = (item: Any) => values.contains(item)

  def andFunc(left: Any => Boolean, right: Any => Boolean): Any => Boolean =
    (item: Any) => left(item) && right(item)

  // Parses the literal in an expression through a KeyType's key function... intended mostly for
  // ComputedColumns so that proper transformation of a value can happen for predicate pushdowns.
  // For example, if a partition column uses :stringPrefix, then apply that first to a value.
  def parseSingleValue(col: Column, value: Any): Any =
    col.extractor.getField(SingleValueRowReader(value), 0) match {
      case z: ZeroCopyUTF8String => UTF8Wrapper(z)
      case o: Any                => o
    }

  def parseValues(col: Column, values: Iterable[Any]): Iterable[Any] =
    values.map(v => parseSingleValue(col, v))

  /**
   * Identifies column names belonging to a projection's partition key columns and their positions within
   * the partition key.  For computed columns, if there is only one source column, the source column is used.
   * Computed columns with multiple source columns are ignored.
   * @param proj a full RichProjection - don't try passing in a rowKeyOnlyProjection or readOnlyProjection
   * @param columnNames the names of columns to match
   * @return a Map(column name -> (position, Column)) of identified partition columns
   */
  def mapPartitionColumns(proj: RichProjection, columnNames: Seq[String]): Map[String, (Int, Column)] =
    mapColumns(proj.partitionColumns, proj.columns, columnNames)

  def mapRowKeyColumns(proj: RichProjection, columnNames: Seq[String]): Map[String, (Int, Column)] =
    mapColumns(proj.rowKeyColumns, proj.columns, columnNames)

  def mapColumns(columns: Seq[Column],
                 allCols: Seq[Column],
                 columnNames: Seq[String]): Map[String, (Int, Column)] = {
    columns.zipWithIndex.collect {
      case d @ (DataColumn(_, name, _, _, _, _), idx)           => name -> (idx -> d._1)
      case d @ (ComputedColumn(_, _, _, _, Seq(index), _), idx) => allCols(index).name -> (idx -> d._1)
    }.toMap.filterKeys { name => columnNames.contains(name) }
  }

  /**
   * Creates a final filter function, checking against the partition keyType.
   * If it is a SingleKeyType, only a single position and func are allowed.
   * If it is a CompositeKeyType, then compositeFilterFunc will be called to create the final func.
   */
  def makePartitionFilterFunc(proj: RichProjection,
                              positions: Array[Int],
                              funcs: Array[Any => Boolean]): PartitionKey => Boolean = {
    require(positions.size == funcs.size)
    def partFunc(p: PartitionKey): Boolean = {
      for { i <- 0 until positions.size optimized } {
        val bool = funcs(i)(p.getAny(positions(i)))
        // Short circuit when any filter returns false
        if (!bool) return false
      }
      true
    }
    partFunc
  }

  def makePartitionFilterFunc(proj: RichProjection, func: Any => Boolean): PartitionKey => Boolean =
    makePartitionFilterFunc(proj, Array(0), Array(func))
}
