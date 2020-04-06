package filodb.core.store

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.reactive.Observable
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, TimeSeriesMemStore}
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter}

// TODO: figure out what to do with this..  most of the tests are really irrelevant
trait ColumnStoreSpec extends FlatSpec with Matchers
with BeforeAndAfter with BeforeAndAfterAll with ScalaFutures {
  import NamesTestData._
  import TestData._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  implicit val s = monix.execution.Scheduler.Implicits.global

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  def colStore: ColumnStore
  def metaStore: MetaStore
  val policy = new FixedMaxPartitionsEvictionPolicy(100)
  val schemas = Schemas(dataset.schema.partition,
                        Map(dataset.name -> dataset.schema))  // Since 99 GDELT rows, this will never evict
  val memStore = new TimeSeriesMemStore(config, colStore, metaStore, Some(policy))

  // First create the tables in C*
  override def beforeAll(): Unit = {
    super.beforeAll()
    metaStore.initialize().futureValue
    colStore.initialize(dataset.ref, 4).futureValue
    colStore.initialize(GdeltTestData.dataset2.ref, 4).futureValue
  }

  before {
    colStore.truncate(dataset.ref, 4).futureValue
    colStore.truncate(GdeltTestData.dataset2.ref, 4).futureValue
    memStore.reset()
    metaStore.clearAllData()
  }

  val partScan = SinglePartitionScan(defaultPartKey, 0)

  implicit val keyType = SingleKeyTypes.LongKeyType

  // NOTE: The test below purposefully does not use any of the read APIs so that if only the read code
  // breaks, this test can independently test for write failures
  "write" should "return NotApplied and not write if no ChunkSets" in {
    whenReady(colStore.write(dataset.ref, Observable.empty)) { response =>
      response should equal (NotApplied)
    }
  }

  it should "append new rows successfully" ignore {
    whenReady(colStore.write(dataset.ref, chunkSetStream(names take 3))) { response =>
      response should equal (Success)
    }

    // last 3 rows, should get appended to first 3 in same partition
    whenReady(colStore.write(dataset.ref, chunkSetStream(names drop 3))) { response =>
      response should equal (Success)
    }

    val rows = memStore.scanRows(dataset, Seq(0, 1, 2), partScan)
                       .map(r => (r.getLong(2), r.filoUTF8String(0))).toSeq
    rows.map(_._1) should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
    rows.map(_._2) should equal (utf8FirstNames)
  }

  "scanRows SinglePartitionScan" should "return no chunksets if cannot find chunk (SinglePartitionRangeScan)" in {
    colStore.write(dataset.ref, chunkSetStream()).futureValue should equal (Success)

    // partition exists but no chunks in range > 1000, this should find nothing
    val noChunkScan = TimeRangeChunkScan(1000L, 2000L)
    val parts = colStore.readRawPartitions(dataset.ref, 1.millis.toMillis,
                             partScan, noChunkScan).toListL.runAsync.futureValue
    parts should have length (0)
  }

  it should "return empty iterator if cannot find partition or version" in {
    // Don't write any data
    colStore.readRawPartitions(dataset.ref, 1.hour.toMillis, partScan)
            .toListL.runAsync.futureValue should have length (0)
  }

  "scanRows" should "read back rows that were written" ignore {
    whenReady(colStore.write(dataset.ref, chunkSetStream())) { response =>
      response should equal (Success)
    }

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    val rowIt = memStore.scanRows(dataset, Seq(0, 1, 2), FilteredPartitionScan(paramSet.head))
    rowIt.map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))

    // check that can read from same partition again
    val rowIt2 = memStore.scanRows(dataset, Seq(2), FilteredPartitionScan(paramSet.head))
    rowIt2.map(_.getLong(0)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
  }

  it should "read back rows written with multi-column row keys" ignore {
    import GdeltTestData._
    val stream = toChunkSetStream(schema2, partBuilder2.partKeyFromObjects(schema2, 197901), dataRows(dataset2))
    colStore.write(dataset2.ref, stream).futureValue should equal (Success)

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    val rowIt = memStore.scanRows(dataset2, schema2.colIDs("NumArticles").get,
                                  FilteredPartitionScan(paramSet.head))
    rowIt.map(_.getInt(0)).sum should equal (492)
  }

  // TODO: FilteredPartitionScan() for ColumnStores does not work without an index right now
  ignore should "filter rows written with single partition key" in {
    import GdeltTestData._
    memStore.setup(dataset2.ref, schemas, 0, TestData.storeConf)
    val stream = Observable.now(records(dataset2))
    // Force flush of all groups at end
    memStore.ingestStream(dataset2.ref, 0, stream, s, Task {}).futureValue

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    val filter = ColumnFilter("MonthYear", Filter.Equals(197902))
    val method = FilteredPartitionScan(paramSet.head, Seq(filter))
    val rowIt = memStore.scanRows(dataset2, schema2.colIDs("NumArticles").get, method)
    rowIt.map(_.getInt(0)).sum should equal (22)
  }

  // "rangeVectors api" should "return Range Vectors for given filter and read all rows" in {
  ignore should "return Range Vectors for given filter and read all rows" in {
    import GdeltTestData._
    memStore.setup(dataset2.ref, schemas, 0, TestData.storeConf)
    val stream = Observable.now(records(dataset2))
    // Force flush of all groups at end
    memStore.ingestStream(dataset2.ref, 0, stream, s, Task {}).futureValue

    val paramSet = colStore.getScanSplits(dataset.ref, 1)
    paramSet should have length (1)

    val filter = ColumnFilter("MonthYear", Filter.Equals(197902))
    val method = FilteredPartitionScan(paramSet.head, Seq(filter))
    val lookupRes = memStore.lookupPartitions(dataset2.ref, method, AllChunkScan)
    val rangeVectorObs = memStore.rangeVectors(dataset2.ref, lookupRes, schema2.colIDs("NumArticles").get,
                                               schema2, false)
    val rangeVectors = rangeVectorObs.toListL.runAsync.futureValue

    rangeVectors should have length (1)
    rangeVectors.head.key.labelValues.head._1.asNewString shouldEqual "MonthYear"
    rangeVectors.head.key.labelValues.head._2.asNewString shouldEqual "197902"
    rangeVectors.head.rows.map(_.getInt(0)).sum should equal (22)
    rangeVectors.head.key.sourceShards shouldEqual Seq(0)
  }
}
