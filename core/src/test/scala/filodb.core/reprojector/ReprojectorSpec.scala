package filodb.core.reprojector

import com.typesafe.config.ConfigFactory
import org.scalatest.time.{Millis, Seconds, Span}
import org.velvia.filo.ZeroCopyUTF8String

import filodb.core._
import filodb.core.metadata.{Dataset, Column, DataColumn, Projection, RichProjection}
import filodb.core.store._

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.ScalaFutures

/**
 * This is probably as close to an end to end test for core as there is.
 * It exercises FiloMemTable, Reprojector, SegmentStateCache, and both write and read paths in the
 * column stores.
 */
class ReprojectorSpec extends FunSpec with Matchers
with BeforeAndAfter with BeforeAndAfterAll with ScalaFutures {
  import monix.execution.Scheduler.Implicits.global
  import NamesTestData._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(250, Millis))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val colStore = new InMemoryColumnStore(global)
  val stateCache = new SegmentStateCache(config, colStore)
  val reprojector = new DefaultReprojector(config, colStore, stateCache)
  val memTable = new FiloMemTable(projection, config, "localhost", 0)

  val partScan = SinglePartitionScan(defaultPartKey)
  val segInfo = SegmentInfo(defaultPartKey, 0).basedOn(projection)

  override def beforeAll(): Unit = {
    super.beforeAll()
    colStore.initializeProjection(dataset.projections.head).futureValue
  }

  before {
    colStore.clearProjectionData(dataset.projections.head).futureValue
    colStore.reset()
    stateCache.clear()
    memTable.clearAllData()
    gdeltMemTable.clearAllData()
  }

  it("should write out new chunkSet in sorted rowKey order") {
    memTable.ingestRows(mapper(names))
    val reprojectedSegInfos = reprojector.reproject(memTable, 0).futureValue

    reprojectedSegInfos should have length (1)
    reprojectedSegInfos.head.segment should equal ("")

    val segState = stateCache.getSegmentState(projection, schema, 0)(segInfo)
    segState.numChunks should equal (1)

    val rowIter = colStore.scanRows(projection, schema, 0, partScan)
    rowIter.map(_.filoUTF8String(0)).toSeq should equal (sortedUtf8Firsts)
  }

  val gdeltMemTable = new FiloMemTable(GdeltTestData.projection3, config, "localhost", 0)

  it("should reuse segment metadata on successive flushes") {
    import GdeltTestData._

    // First flush: first 60 rows, partition key = year, actor2code
    gdeltMemTable.ingestRows(readers take 60)
    reprojector.reproject(gdeltMemTable, 0).futureValue

    // Second flush: rest of rows. Some actor2code repeats, such as CHL, GOV, RUS
    gdeltMemTable.clearAllData()
    gdeltMemTable.ingestRows(readers drop 60)
    reprojector.reproject(gdeltMemTable, 0).futureValue

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    val rowIter = colStore.scanRows(projection3, schema, 0, FilteredPartitionScan(paramSet.head))
    rowIter.map(_.getInt(6)).sum should equal (492)
  }

  it("should reload segment metadata if state no longer in cache") {
    import GdeltTestData._

    gdeltMemTable.ingestRows(readers take 60)
    reprojector.reproject(gdeltMemTable, 0).futureValue

    gdeltMemTable.clearAllData()
    stateCache.clear()
    gdeltMemTable.ingestRows(readers drop 60)
    reprojector.reproject(gdeltMemTable, 0).futureValue

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    val rowIter = colStore.scanRows(projection3, schema, 0, FilteredPartitionScan(paramSet.head))
    rowIter.map(_.getInt(6)).sum should equal (492)
  }

  it("should reload segment metadata and replace previous chunk rows successfully") {
    import GdeltTestData._

    gdeltMemTable.ingestRows(readers take 60)
    reprojector.reproject(gdeltMemTable, 0).futureValue

    gdeltMemTable.clearAllData()
    stateCache.clear()
    gdeltMemTable.ingestRows(readers drop 57)   // replay 3 rows, see if they get replaced properly
    reprojector.reproject(gdeltMemTable, 0).futureValue

    val paramSet = colStore.getScanSplits(datasetRef, 1)
    paramSet should have length (1)

    val rowIter = colStore.scanRows(projection3, schema, 0, FilteredPartitionScan(paramSet.head))
    rowIter.map(_.getInt(6)).sum should equal (492)
  }
}
