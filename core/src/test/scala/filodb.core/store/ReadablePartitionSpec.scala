package filodb.core.store

import filodb.core.{GlobalConfig, Utils}
import filodb.core.NamesTestData.dataset
import filodb.core.downsample.OffHeapMemory
import filodb.core.memstore.{PagedReadablePartition, TimeSeriesPartition, TimeSeriesShardInfo, TimeSeriesShardStats}
import filodb.core.metadata.Schemas
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
class ReadablePartitionSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  it("doesSchemaMatchOrBackCompatibleHistograms should return true for delta and otel-delta") {
    val rawData = RawPartData(Array.empty, Seq.empty)
    val p1 = new PagedReadablePartition(Schemas.deltaHistogram, 0, -1, rawData, 10)
    val p2 = new PagedReadablePartition(Schemas.otelDeltaHistogram, 0, -1, rawData, 10)
    Utils.doesSchemaMatchOrBackCompatibleHistograms(
      p1.schema.name, p1.schema.schemaHash, p2.schema.name, p2.schema.schemaHash) shouldEqual true
  }

  it("doesSchemaMatchOrBackCompatibleHistograms should return true for otel-delta and delta") {
    val rawData = RawPartData(Array.empty, Seq.empty)
    val p1 = new PagedReadablePartition(Schemas.otelDeltaHistogram, 0, -1, rawData, 10)
    val p2 = new PagedReadablePartition(Schemas.deltaHistogram, 0, -1, rawData, 10)
    Utils.doesSchemaMatchOrBackCompatibleHistograms(
      p1.schema.name, p1.schema.schemaHash, p2.schema.name, p2.schema.schemaHash) shouldEqual true
  }

  it("doesSchemaMatchOrBackCompatibleHistograms should return true for cumulative and otel-cumulative") {
    val rawData = RawPartData(Array.empty, Seq.empty)
    val p1 = new PagedReadablePartition(Schemas.promHistogram, 0, -1, rawData, 10)
    val p2 = new PagedReadablePartition(Schemas.otelCumulativeHistogram, 0, -1, rawData, 10)
    Utils.doesSchemaMatchOrBackCompatibleHistograms(
      p1.schema.name, p1.schema.schemaHash, p2.schema.name, p2.schema.schemaHash) shouldEqual true
  }

  it("doesSchemaMatchOrBackCompatibleHistograms should return true for otel-cumulative and prom histogram") {
    val rawData = RawPartData(Array.empty, Seq.empty)
    val p1 = new PagedReadablePartition(Schemas.otelCumulativeHistogram, 0, -1, rawData, 10)
    val p2 = new PagedReadablePartition(Schemas.promHistogram, 0, -1, rawData, 10)
    Utils.doesSchemaMatchOrBackCompatibleHistograms(
      p1.schema.name, p1.schema.schemaHash, p2.schema.name, p2.schema.schemaHash) shouldEqual true
  }

  it("doesSchemaMatchOrBackCompatibleHistograms should return true if schema matches paged readble partition") {
    val rawData = RawPartData(Array.empty, Seq.empty)
    val p1 = new PagedReadablePartition(Schemas.deltaCounter, 0, -1, rawData, 10)
    val p2 = new PagedReadablePartition(Schemas.deltaCounter, 0, -1, rawData, 10)
    Utils.doesSchemaMatchOrBackCompatibleHistograms(
      p1.schema.name, p1.schema.schemaHash, p2.schema.name, p2.schema.schemaHash) shouldEqual true

    val p3 = new PagedReadablePartition(Schemas.gauge, 0, -1, rawData, 10)
    val p4 = new PagedReadablePartition(Schemas.gauge, 0, -1, rawData, 10)
    Utils.doesSchemaMatchOrBackCompatibleHistograms(
      p3.schema.name, p3.schema.schemaHash, p4.schema.name, p4.schema.schemaHash) shouldEqual true

    val p5 = new PagedReadablePartition(Schemas.promHistogram, 0, -1, rawData, 10)
    val p6 = new PagedReadablePartition(Schemas.promHistogram, 0, -1, rawData, 10)
    Utils.doesSchemaMatchOrBackCompatibleHistograms(
      p5.schema.name, p5.schema.schemaHash, p6.schema.name, p6.schema.schemaHash) shouldEqual true

    val p7 = new PagedReadablePartition(Schemas.promCounter, 0, -1, rawData, 10)
    val p8 = new PagedReadablePartition(Schemas.promCounter, 0, -1, rawData, 10)
    Utils.doesSchemaMatchOrBackCompatibleHistograms(
      p7.schema.name, p7.schema.schemaHash, p8.schema.name, p8.schema.schemaHash) shouldEqual true
  }

  it("doesSchemaMatchOrBackCompatibleHistograms should return true if schema matches timeseries partition") {
    val storeConfig = StoreConfig(GlobalConfig.defaultFiloConfig.getConfig("downsampler.downsample-store-config"))
    val offheapMem = new OffHeapMemory(Seq(Schemas.gauge, Schemas.promCounter, Schemas.promHistogram, Schemas.untyped),
      Map.empty, 100, storeConfig)
    val shardInfo = TimeSeriesShardInfo(
      0, new TimeSeriesShardStats(dataset.ref, 0), offheapMem.bufferPools, offheapMem.nativeMemoryManager)

    val p1 = new TimeSeriesPartition(0, Schemas.deltaHistogram, 0, shardInfo, 1)
    val p2 = new TimeSeriesPartition(0, Schemas.otelDeltaHistogram, 0, shardInfo, 1)
    Utils.doesSchemaMatchOrBackCompatibleHistograms(
      p1.schema.name, p1.schema.schemaHash, p2.schema.name, p2.schema.schemaHash) shouldEqual true

    val p3 = new TimeSeriesPartition(0, Schemas.gauge, 0, shardInfo, 1)
    val p4 = new TimeSeriesPartition(0, Schemas.gauge, 0, shardInfo, 1)
    Utils.doesSchemaMatchOrBackCompatibleHistograms(
      p3.schema.name, p3.schema.schemaHash, p4.schema.name, p4.schema.schemaHash) shouldEqual true
  }

  it("doesSchemaMatchOrBackCompatibleHistograms should return false if schema does not matches timeseries partition") {
    val storeConfig = StoreConfig(GlobalConfig.defaultFiloConfig.getConfig("downsampler.downsample-store-config"))
    val offheapMem = new OffHeapMemory(Seq(Schemas.gauge, Schemas.promCounter, Schemas.promHistogram, Schemas.untyped),
      Map.empty, 100, storeConfig)
    val shardInfo = TimeSeriesShardInfo(
      0, new TimeSeriesShardStats(dataset.ref, 0), offheapMem.bufferPools, offheapMem.nativeMemoryManager)

    val p1 = new TimeSeriesPartition(0, Schemas.deltaHistogram, 0, shardInfo, 1)
    val p2 = new TimeSeriesPartition(0, Schemas.promHistogram, 0, shardInfo, 1)
    Utils.doesSchemaMatchOrBackCompatibleHistograms(
      p1.schema.name, p1.schema.schemaHash, p2.schema.name, p2.schema.schemaHash) shouldEqual false

    val p3 = new TimeSeriesPartition(0, Schemas.promCounter, 0, shardInfo, 1)
    val p4 = new TimeSeriesPartition(0, Schemas.deltaCounter, 0, shardInfo, 1)
    Utils.doesSchemaMatchOrBackCompatibleHistograms(
      p3.schema.name, p3.schema.schemaHash, p4.schema.name, p4.schema.schemaHash) shouldEqual false

    val p5 = new TimeSeriesPartition(0, Schemas.gauge, 0, shardInfo, 1)
    val p6 = new TimeSeriesPartition(0, Schemas.dsGauge, 0, shardInfo, 1)
    Utils.doesSchemaMatchOrBackCompatibleHistograms(
      p5.schema.name, p5.schema.schemaHash, p6.schema.name, p6.schema.schemaHash) shouldEqual false

    val p7 = new TimeSeriesPartition(0, Schemas.otelCumulativeHistogram, 0, shardInfo, 1)
    val p8 = new TimeSeriesPartition(0, Schemas.otelDeltaHistogram, 0, shardInfo, 1)
    Utils.doesSchemaMatchOrBackCompatibleHistograms(
      p7.schema.name, p7.schema.schemaHash, p8.schema.name, p8.schema.schemaHash) shouldEqual false
  }
}
