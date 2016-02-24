package filodb.core.query

import org.scalactic._
import org.velvia.filo.RowReader
import scala.language.postfixOps
import scalaxy.loops._

import filodb.core._
import filodb.core.metadata.{DataColumn, ComputedColumn, RichProjection}

/**
 * Utilities to generate functions to filter keys.
 */
object KeyFilter {
  import Requirements._

  def forceType(kt: KeyType, item: Any): kt.T = item match {
    case s: String => kt.fromString(s)
    case t: Any    => t.asInstanceOf[kt.T]
  }

  def equalsFunc(kt: KeyType)(value: kt.T): Any => Boolean =
    (item: Any) => item == value

  def inFunc(kt: KeyType)(values: Set[kt.T]): Any => Boolean =
    (item: Any) => values.contains(item.asInstanceOf[kt.T])

  def andFunc(left: Any => Boolean, right: Any => Boolean): Any => Boolean =
    (item: Any) => left(item) && right(item)

  // Parses the literal in an expression through a KeyType's key function... intended mostly for
  // ComputedColumns so that proper transformation of a value can happen for predicate pushdowns.
  // For example, if a partition column uses :stringPrefix, then apply that first to a value.
  def parseSingleValue(kt: KeyType)(value: Any): kt.T = {
    val keyFunc = kt.getKeyFunc(Array[Int]())
    keyFunc(SingleValueRowReader(value)).asInstanceOf[kt.T]
  }

  def parseValues(kt: KeyType)(values: Iterable[Any]): Iterable[kt.T] = {
    val keyFunc = kt.getKeyFunc(Array[Int]())
    values.map(v => keyFunc(SingleValueRowReader(v)).asInstanceOf[kt.T])
  }

  /**
   * Generates a filter func for a composite key type.
   * @param kt the CompositeKeyType
   * @param positions an array of 0-based positions, first column in key = 0, second = 1 etc.
   * @param funcs an array of filter functions corresponding to each position
   */
  def compositeFilterFunc(kt: CompositeKeyType)
                         (positions: Array[Int], funcs: Array[Any => Boolean]): Any => Boolean = {
    require(positions.max < kt.atomTypes.length)
    require(positions.size == funcs.size)

    def filterFunc(item: Any): Boolean = {
      val items = item.asInstanceOf[Seq[_]]
      for { i <- 0 until positions.size optimized } {
        val bool = funcs(i)(items(positions(i)))
        // Short circuit when any filter returns false
        if (!bool) return false
      }
      true
    }

    filterFunc
  }

  /**
   * Identifies column names belonging to a projection's partition key columns and their positions within
   * the partition key.  For computed columns, if there is only one source column, the source column is used.
   * Computed columns with multiple source columns are ignored.
   * @param proj a full RichProjection - don't try passing in a rowKeyOnlyProjection or readOnlyProjection
   * @param columnNames the names of columns to match
   * @return a Map(column name -> (position, keyType)) of identified partition columns
   */
  def mapPartitionColumns(proj: RichProjection, columnNames: Seq[String]): Map[String, (Int, KeyType)] = {
    proj.partitionColumns.zipWithIndex.collect {
      case (DataColumn(_, name, _, _, columnType, _), idx) => name -> (idx -> columnType.keyType)
      case (ComputedColumn(_, _, _, _, Seq(srcColumn), keyType), idx) => srcColumn -> (idx -> keyType)
    }.toMap.filterKeys { name => columnNames.contains(name) }
  }

  /**
   * Creates a final filter function, checking against the partition keyType.
   * If it is a SingleKeyType, only a single position and func are allowed.
   * If it is a CompositeKeyType, then compositeFilterFunc will be called to create the final func.
   */
  def makePartitionFilterFunc(proj: RichProjection,
                              positions: Seq[Int],
                              funcs: Seq[Any => Boolean]): Any => Boolean = {
    proj.partitionType match {
      case c: CompositeKeyType =>
        compositeFilterFunc(c)(positions.toArray, funcs.toArray)
      case p: KeyType =>
        require(positions.size == 1)
        funcs.head
    }
  }
}

case class SingleValueRowReader(value: Any) extends RowReader {
  def notNull(columnNo: Int): Boolean = Option(value).isDefined
  def getBoolean(columnNo: Int): Boolean = value.asInstanceOf[Boolean]
  def getInt(columnNo: Int): Int = value.asInstanceOf[Int]
  def getLong(columnNo: Int): Long = value.asInstanceOf[Long]
  def getDouble(columnNo: Int): Double = value.asInstanceOf[Double]
  def getFloat(columnNo: Int): Float = value.asInstanceOf[Float]
  def getString(columnNo: Int): String = value.asInstanceOf[String]
  def getAny(columnNo: Int): Any = value
}