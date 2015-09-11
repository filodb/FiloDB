package filodb.core.reprojector

import com.typesafe.config.ConfigFactory
import scala.concurrent.Future

import filodb.core._
import filodb.core.metadata.{Column, Dataset}
import filodb.core.columnstore.{TupleRowReader, RowReader, SegmentSpec}

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}

class SchedulerSpec extends FunSpec with Matchers with BeforeAndAfter {
  import SegmentSpec._
  import MemTable._

  val keyRange = KeyRange("dataset", Dataset.DefaultPartitionKey, 0L, 10000L)
  var mTable: MemTable = _
  var reprojections: Seq[(Types.TableName, Int)] = Nil
  val flushPolicy = new NumRowsFlushPolicy(100L)
  var scheduler: Scheduler = _

  before {
    mTable = new MapDBMemTable(ConfigFactory.load)
    reprojections = Nil
    scheduler = new Scheduler(mTable, testReprojector, flushPolicy, maxTasks = 2)
  }

  after {
    mTable.close()
  }

  val schemaWithPartCol = schema ++ Seq(
    Column("league", "dataset", 0, Column.ColumnType.StringColumn)
  )

  val namesWithPartCol = (0 until 50).flatMap { partNum =>
    names.map { t => (t._1, t._2, t._3, Some(partNum.toString)) }
  }

  private def ingestRows(datasetName: String, numRows: Int) {
    val myDataset = dataset.copy(partitionColumn = "league", name = datasetName)
    mTable.setupIngestion(myDataset, schemaWithPartCol, 0) should equal (SetupDone)

    val resp = mTable.ingestRows(datasetName, 0, namesWithPartCol.take(numRows).map(TupleRowReader))
    resp should equal (Ingested)
  }

  import RowReader._
  val testReprojector = new Reprojector {
    def reproject[K: TypedFieldExtractor](memTable: MemTable, setup: IngestionSetup, version: Int):
        Future[Seq[Response]] = {
      reprojections = reprojections :+ (setup.dataset.name -> version)
      Future.successful(Seq(Success))
    }
  }

  it("should do nothing if memtable is empty") {
    scheduler.runOnce()
    scheduler.tasks should have size (0)
    reprojections should equal (Nil)
  }

  it("should do nothing if datasets not reached limit yet") {
    ingestRows("A", 50)
    ingestRows("B", 49)
    scheduler.runOnce()
    scheduler.tasks should have size (0)
    reprojections should equal (Nil)
  }

  it("should pick largest dataset for flushing, and keep flush going") {
    ingestRows("A", 51)
    ingestRows("B", 49)

    // Should schedule flush of "A"
    scheduler.runOnce()
    scheduler.tasks should have size (1)
    reprojections should equal (Seq(("A", 0)))
    mTable.flushingDatasets should equal (Seq((("A", 0), 51)))

    // Add some new rows to A active
    mTable.ingestRows("A", 0, namesWithPartCol.take(10).map(TupleRowReader))

    // Should continue flush of "A", but recommend "B" since limit not reached
    // Also tests that "A" is not re-recommended since it is already flushing (if it did try,
    // then would not be able to recommend flushing B)
    reprojections = Nil
    scheduler.runOnce()
    scheduler.tasks should have size (2)
    reprojections should equal (Seq(("A", 0), ("B", 0)))
    mTable.flushingDatasets.toSet should equal (Set((("A", 0), 51), (("B", 0), 49)))
    mTable.allNumRows(Active, true) should equal (Seq((("A", 0), 10)))
  }

  it("should limit flushes if reached max task limit") {
    ingestRows("A", 30)
    ingestRows("B", 40)
    ingestRows("C", 50)

    // Should schedule flush of "C"
    scheduler.runOnce()
    scheduler.tasks should have size (1)
    reprojections should equal (Seq(("C", 0)))

    // Should reschedule "C", then flush "B"
    reprojections = Nil
    scheduler.runOnce()
    scheduler.tasks should have size (2)
    reprojections should equal (Seq(("C", 0), ("B", 0)))

    // Now, should reschedule "C" and "B", then reach limit and not flush anymore.
    reprojections = Nil
    scheduler.runOnce()
    scheduler.tasks should have size (2)
    reprojections should equal (Seq(("C", 0), ("B", 0)))
    mTable.flushingDatasets.toSet should equal (Set((("B", 0), 40), (("C", 0), 50)))
  }
}
