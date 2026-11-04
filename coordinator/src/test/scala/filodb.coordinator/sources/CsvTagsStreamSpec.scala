package filodb.coordinator.sources

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler.Implicits.global
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.metadata.{Dataset, Schemas}
import filodb.memory.format.ZeroCopyUTF8String

class CsvTagsStreamSpec extends AnyFunSpec with Matchers {

  it("parseTags parses a k=v,k=v cell into a UTF8 map") {
    val m = CsvStream.parseTags("_ws_=test_ws,_ns_=promcompat,mode=idle")
    m(ZeroCopyUTF8String("_ws_")) shouldEqual ZeroCopyUTF8String("test_ws")
    m(ZeroCopyUTF8String("_ns_")) shouldEqual ZeroCopyUTF8String("promcompat")
    m(ZeroCopyUTF8String("mode")) shouldEqual ZeroCopyUTF8String("idle")
    m.size shouldEqual 3
  }

  it("parseTags returns an empty map for an empty cell") {
    CsvStream.parseTags("") shouldEqual Map.empty
  }

  it("CsvStreamFactory ingests a labeled CSV into a gauge dataset with a tags map") {
    // The gauge schema: partition ["_metric_:string","tags:map"], data ["timestamp:ts","value:double"].
    val schemas = Schemas(Schemas.gauge)
    val conf = ConfigFactory.parseString(
      """header = true
        |batch-size = 10
        |resource = "/labeled-metrics-test.csv"
        |""".stripMargin)
    val stream = new CsvStreamFactory().create(conf, schemas, shard = 0, offset = None)
    val batches = Await.result(stream.get.toListL.runToFuture, 30.seconds)
    stream.teardown()

    // Sum the records across all containers; there should be 4 rows.
    val totalRecords: Int = batches.map(_.records.countRecords()).sum
    totalRecords shouldEqual 4
  }

  import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, TimeSeriesMemStore}
  import filodb.core.query.{ColumnFilter, Filter, QuerySession}
  import filodb.core.store.{FilteredPartitionScan, InMemoryMetaStore, NullColumnStore}
  import filodb.core.TestData

  it("tags parsed from CSV are queryable as label filters in the memstore") {
    val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
    val schemas = Schemas(Schemas.gauge)
    val dsRef = Dataset("prometheus", Schemas.gauge).ref
    val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new NullColumnStore,
      new InMemoryMetaStore(), Some(new FixedMaxPartitionsEvictionPolicy(100)))
    memStore.setup(dsRef, schemas, 0, TestData.storeConf, 1)

    val conf = ConfigFactory.parseString(
      """header = true
        |batch-size = 10
        |resource = "/labeled-metrics-test.csv"
        |""".stripMargin)
    val stream = new CsvStreamFactory().create(conf, schemas, 0, offset = None)
    Await.result(stream.get.toListL.runToFuture, 30.seconds)
      .foreach(sd => memStore.ingest(dsRef, 0, sd))
    stream.teardown()
    memStore.refreshIndexForTesting(dsRef)

    val filters = Seq(ColumnFilter("mode", Filter.Equals(ZeroCopyUTF8String("user"))))
    val split = memStore.getScanSplits(dsRef, 1).head
    val parts = Await.result(
      memStore.scanPartitions(dsRef, Seq(0), FilteredPartitionScan(split, filters),
        querySession = QuerySession.makeForTestingOnly).toListL.runToFuture,
      30.seconds)
    // The key assertion: filtering on a TAG KEY (which only exists inside the parsed tags map)
    // is accepted without error and selects exactly the "user" series (2 rows, 1 partition).
    parts.size shouldEqual 1

    memStore.shutdown()
  }
}
