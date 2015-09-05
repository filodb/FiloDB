package filodb.core.reprojector

/**
 * The MemTable serves these purposes:
 * 1) Holds incoming rows of data before being flushed
 * 2) Can extract rows of data in a given sort order, and remove them
 * 3) Can read rows of data in a given sort order for queries
 */
trait MemTable {
  def numRows: Long
  def tables: Set[String]
}

