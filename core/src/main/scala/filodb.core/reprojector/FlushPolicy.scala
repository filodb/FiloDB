package filodb.core.reprojector

import filodb.core.Types._

trait FlushPolicy {
  // this could check memory size, state of memtable, etc.
  def shouldFlush(memtable: MemTable): Boolean

  // Determine the next dataset and version to flush
  def nextFlush(memtable: MemTable): (TableName, Int)

  protected def allNumRows(memtable: MemTable): Seq[((TableName, Int), Long)] = {
    for { dataset <- memtable.datasets.toSeq
          version <- memtable.versionsForDataset(dataset).get.toSeq }
    yield { ((dataset, version), memtable.numRows(dataset, version, MemTable.Active).get) }
  }
}

/**
 * A really dumb flush policy based purely on the total # of rows across all datasets.
 */
class NumRowsFlushPolicy(maxTotalRows: Long) extends FlushPolicy {
  def shouldFlush(memtable: MemTable): Boolean = {
    val totalRows = allNumRows(memtable).map(_._2).sum
    totalRows > maxTotalRows
  }

  // Determine the next dataset and version to flush
  def nextFlush(memtable: MemTable): (TableName, Int) = {
    // Just iterate and find the (dataset, version) with the most rows
    val numRows = allNumRows(memtable)
    val highest = numRows.foldLeft(numRows.head) { case (highest, (nameVer, numRows)) =>
      if (numRows > highest._2) (nameVer, numRows) else highest
    }
    highest._1
  }
}

