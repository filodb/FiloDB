package filodb.core.reprojector

import filodb.core.Types._

/**
 * FlushPolicy's check for new flush cycle opportunities by looking at the active memtables and using
 * some heuristics to determine what datasets to flush next.  Flush means to flipBuffers() and cause
 * the Active table to be swapped into Locked state.
 */
trait FlushPolicy {
  /**
   * Determine the next dataset and version to flush using some heuristic.
   * Should ignore currently flushing datasets (ie Locked memtable is nonempty)
   * @return None if it is not time to flush yet, or all datasets are already being flushed.
   *          Some((dataset, version)) for the next flushing candidate.
   */
  def nextFlush(memtable: MemTable): Option[(TableName, Int)]
}

/**
 * A really dumb flush policy based purely on the total # of rows across all datasets.
 * Flushes if the total # of rows is equal to or exceeding the maxTotalRows.
 */
class NumRowsFlushPolicy(maxTotalRows: Long) extends FlushPolicy {
  override def toString: String = s"NumRowsFlushPolicy($maxTotalRows)"
  def nextFlush(memtable: MemTable): Option[(TableName, Int)] = {
    val activeRows = memtable.allNumRows(MemTable.Active, nonZero = true)
    val flushingRows = memtable.flushingDatasets
    val totalRows = activeRows.map(_._2).sum + flushingRows.map(_._2).sum

    if (totalRows < maxTotalRows) {
      None
    } else {
      val flushingSet = flushingRows.map(_._1).toSet
      val notFlushingRows = activeRows.filterNot { case (dv, numRows) => flushingSet contains dv }
      if (notFlushingRows.isEmpty) {
        None
      } else {
        val highest = notFlushingRows.foldLeft(notFlushingRows.head) { case (highest, (nameVer, numRows)) =>
          if (numRows > highest._2) (nameVer, numRows) else highest
        }
        Some(highest._1)
      }
    }
  }
}

