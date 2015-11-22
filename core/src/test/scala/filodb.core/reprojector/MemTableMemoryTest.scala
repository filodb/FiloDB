package filodb.core.reprojector

import com.typesafe.config.ConfigFactory
import org.velvia.filo.TupleRowReader

import filodb.core.KeyRange
import filodb.core.metadata.{Column, Dataset}
import filodb.core.columnstore.SegmentSpec

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}

class MemTableMemoryTest extends FunSpec with Matchers with BeforeAndAfter {
  import SegmentSpec._

  val keyRange = KeyRange("dataset", Dataset.DefaultPartitionKey, 0L, 10000L)
  val newSetting = "memtable.max-rows-per-table = 200000"
  val config = ConfigFactory.parseString(newSetting).withFallback(
                 ConfigFactory.load("application_test.conf"))
  val mTable = new FiloMemTable(projection, config)
  import scala.concurrent.ExecutionContext.Implicits.global

  before {
    mTable.clearAllData()
  }

  val schemaWithPartCol = schema ++ Seq(
    Column("league", "dataset", 0, Column.ColumnType.StringColumn)
  )

  val numRows = 100000

  val lotsOfNames = (0 until (numRows/6)).toIterator.flatMap { partNum =>
    names.map { t => (t._1, t._2, t._3, Some(partNum.toString)) }.toIterator
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
      mTable.ingestRows(rows.toSeq) { numRows += rows.length }
      // println(s"Ingested $numRows rows")
      // Thread sleep 1000
    }

    val elapsed = System.currentTimeMillis - start
    val endFreeMem = sys.runtime.freeMemory
    println(s"End: free memory = $endFreeMem   elapsed = ${elapsed} ms")
    printDetailedMemUsage()
  }
}
