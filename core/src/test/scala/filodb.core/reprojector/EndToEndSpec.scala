package filodb.core.reprojector

import com.typesafe.config.ConfigFactory
import org.velvia.filo.TupleRowReader
import scala.concurrent.Future

import filodb.core._
import filodb.core.metadata.{Column, Dataset}
import filodb.core.columnstore.{SegmentSpec, RowReaderSegment, InMemoryColumnStore}

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span, Seconds}

/**
 * Ingests rows into MemTable, calling Scheduler with DefaultReprojector, with the InMemoryColumnStore.
 * Then reads data back out.
 *
 * Tests every major component in ingestion and read pipelines, except:
 * - No MetaStore (but Dataset and Column are used)
 * - Only one dataset
 *
 * Debugging this test is probably best done through the filodb-test.log
 */
class EndToEndSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  import SegmentSpec._
  import MemTable._

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  val mTable = new MapDBMemTable(ConfigFactory.load("application_test.conf"))
  val flushPolicy = new NumRowsFlushPolicy(100L)
  val columnStore = new InMemoryColumnStore
  val reprojector = new DefaultReprojector(columnStore)
  val scheduler = new Scheduler(mTable, reprojector, flushPolicy, maxTasks = 2)

  after {
    mTable.close()
  }

  // OK, what we want is to test multiple partitions, segments, multiple chunks per segment too.
  // With default segmentSize of 10000, change chunkSize to say 100.
  // Thus let's have the following:
  // "nfc"  0-99  10000-10099 10100-10199  20000-20099 20100-20199 20200-20299
  // "afc"  the same
  // 1200 rows total, 6 segments (3 x 2 partitions)
  // No need to test out of order since that's covered by other things (but we can scramble the rows
  // just for fun)
  val schemaWithPartCol = schema ++ Seq(
    Column("league", "dataset", 0, Column.ColumnType.StringColumn)
  )

  val largeDataset = dataset.copy(options = Dataset.DefaultOptions.copy(chunkSize = 100),
                                  partitionColumn = "league")

  val namesWithPartCol = {
    for { league <- Seq("nfc", "afc")
          numChunks <- 0 to 2
          chunk  <- 0 to numChunks
          startRowNo = numChunks * 10000 + chunk * 100
          rowNo  <- startRowNo to (startRowNo + 99) }
    yield { (names(rowNo % 6)._1, names(rowNo % 6)._2, Some(rowNo.toLong), Some(league)) }
  }

  it("should ingest all rows, flush them to columstore, and be able to read them back") {
    // ingest rows into MemTable and check they are ingested OK
    mTable.setupIngestion(largeDataset, schemaWithPartCol, 0) should equal (SetupDone)
    mTable.ingestRows("dataset", 0, namesWithPartCol.map(TupleRowReader)) should equal (Ingested)
    mTable.allNumRows(Active) should equal (Seq((("dataset", 0), 1200L)))

    // Schedule a flush, wait, and see that it's done and all segments written.
    scheduler.runOnce()
    scheduler.tasks should have size (1)
    whenReady(scheduler.tasks.values.head) { responses =>
      scheduler.failedTasks should equal (Nil)
      mTable.allNumRows(Active) should equal (Seq((("dataset", 0), 0L)))
      mTable.flushingDatasets should equal (Nil)
    }

    // Run this another time to verify no tasks scheduled
    scheduler.runOnce()
    scheduler.tasks should have size (0)

    // Now, read stuff back from the column store and check that it's all there
    val keyRange = KeyRange(dataset.name, "nfc", 0L, 30000L)
    whenReady(columnStore.readSegments(schema, keyRange, 0)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (3)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment[Long]]
      readSeg.keyRange should equal (keyRange.copy(end = 10000L))
      readSeg.rowIterator().map(_.getLong(2)).toSeq should equal ((0 to 99).map(_.toLong))
    }

    whenReady(columnStore.scanSegments[Long](schema, dataset.name, 0)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (6)
    }
  }
}