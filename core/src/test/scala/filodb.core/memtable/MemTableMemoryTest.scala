package filodb.core.memtable

import filodb.core.metadata._
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.velvia.filo.TupleRowReader

class MemTableMemoryTest extends FunSpec with Matchers with BeforeAndAfter {

  import filodb.core.Setup._

  val newSetting = "memtable.max-rows-per-table = 200000"
  val mTable = new MapDBMemTable(projection)

  before {
    mTable.clearAllData()
  }

  val schemaWithPartCol = schema ++ Seq(
    Column("league", "dataset", 0, Column.ColumnType.StringColumn)
  )

  val numRows = 100000

  val lotsOfNames = (0 until (numRows / 6)).toIterator.flatMap { partNum =>
    names.map { t => (Some(partNum.toString), t._2, t._3, t._4, t._5) }.toIterator
  }

  private def printDetailedMemUsage() {
    val mxBean = java.lang.management.ManagementFactory.getMemoryMXBean
    println(mxBean.getNonHeapMemoryUsage)
  }

  // To really see amount of memory used, might have to uncomment the thread sleep and use VisualVM,
  // because tests are forked.
  it("should add tons of rows without overflowing memory and taking too long") {
    val start = System.currentTimeMillis
    val startFreeMem = sys.runtime.freeMemory
    println(s"Start: free memory = $startFreeMem")
    printDetailedMemUsage()

    var numRows = 0
    lotsOfNames.map(TupleRowReader).grouped(2000).foreach { rows =>
      mTable.ingestRows(rows.toSeq)
      // println(s"Ingested $numRows rows")
      // Thread sleep 1000
    }

    val elapsed = System.currentTimeMillis - start
    val endFreeMem = sys.runtime.freeMemory
    println(s"End: free memory = $endFreeMem   elapsed = ${elapsed} ms")
    printDetailedMemUsage()
  }
}
