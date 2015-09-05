package filodb.core.reprojector

import filodb.core.Types._

trait FlushPolicy {

  // this could check memory size, no of documents, time interval
  def shouldFlush(memtable: MemTable): Boolean

  // Determine the next table and partition to flush
  def nextFlushInfo(memtable: MemTable): (String, PartitionKey)
}

