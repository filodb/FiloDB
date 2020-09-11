package filodb.gateway.conversion

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import remote.RemoteStorage.{LabelPair, Sample, TimeSeries}

import filodb.core.binaryrecord2.{RecordBuilder, StringifyMapItemConsumer}
import filodb.core.metadata.Schemas
import filodb.memory.MemFactory
import filodb.memory.format.ZeroCopyUTF8String._

object TimeSeriesFixture {
  //  "num_partitions,dataset=timeseries,host=MacBook-Pro-229.local,shard=0,_ws_=demo,_ns_=filodb counter=0 1536790212000000000",
  def timeseries(no: Int, tags: Map[String, String]): TimeSeries = {
    val builder = TimeSeries.newBuilder
                    .addSamples(Sample.newBuilder.setTimestampMs(1000000L + no).setValue(1.1 + no).build)
    tags.foldLeft(builder) { case (b, (k, v)) =>
      b.addLabels(LabelPair.newBuilder.setName(k).setValue(v).build)
    }.build
  }
}

class PrometheusInputRecordSpec extends AnyFunSpec with Matchers {
  val schema = Schemas.promCounter
  val baseTags = Map("dataset" -> "timeseries",
                     "host" -> "MacBook-Pro-229.local",
                     "shard" -> "0")
  val tagsWithMetric = baseTags + ("__name__" -> "num_partitions")

  it("should parse from TimeSeries proto and write to RecordBuilder") {
    val proto1 = TimeSeriesFixture.timeseries(0, tagsWithMetric + ("_ns_" -> "filodb", "_ws_" -> "demo"))
    val builder = new RecordBuilder(MemFactory.onHeapFactory)

    val records = PrometheusInputRecord(proto1)
    records should have length (1)
    val record1 = records.head
    record1.tags shouldEqual (baseTags + ("_ns_" -> "filodb", "_ws_" -> "demo"))
    record1.getMetric shouldEqual "num_partitions"
    record1.nonMetricShardValues shouldEqual Seq("filodb", "demo")

    record1.shardKeyHash shouldEqual RecordBuilder.shardKeyHash(Seq("filodb", "demo"), "num_partitions")

    record1.addToBuilder(builder)
    builder.allContainers.head.foreach { case (base, offset) =>
      schema.ingestionSchema.partitionHash(base, offset) should not equal (7)
      schema.ingestionSchema.getLong(base, offset, 0) shouldEqual 1000000L
      schema.ingestionSchema.getDouble(base, offset, 1) shouldEqual 1.1
      schema.ingestionSchema.asJavaString(base, offset, 2) shouldEqual "num_partitions"

      val consumer = new StringifyMapItemConsumer()
      schema.ingestionSchema.consumeMapItems(base, offset, 3, consumer)
      consumer.stringPairs.toMap shouldEqual (baseTags + ("_ns_" -> "filodb", "_ws_" -> "demo"))
    }
  }

  it("should be able to change the name of predefined tag and be able to read old part keys with new tag") {
    val builder = new RecordBuilder(MemFactory.onHeapFactory)
    val oldSchema = Schemas.promCounter.copy(
      partition = Schemas.promCounter.partition.copy(
        predefinedKeys = Schemas.promCounter.partition.predefinedKeys.updated(9, "_step_")))

    val records = Seq(new MetricTagInputRecord(
      Seq[Any](1000000L, 1.1d),
      "num_partitions",
      Map("_step_".utf8 -> "0".utf8, "_ns_".utf8 -> "filodb".utf8, "_ws_".utf8 -> "demo".utf8),
      oldSchema
    ))

    records should have length (1)
    val record1 = records.head
    record1.getMetric shouldEqual "num_partitions"
    record1.nonMetricShardValues shouldEqual Seq("filodb", "demo")

    record1.shardKeyHash shouldEqual RecordBuilder.shardKeyHash(Seq("filodb", "demo"), "num_partitions")

    record1.addToBuilder(builder)
    builder.allContainers.head.foreach { case (base, offset) =>
      schema.ingestionSchema.partitionHash(base, offset) should not equal (7)
      schema.ingestionSchema.getLong(base, offset, 0) shouldEqual 1000000L
      schema.ingestionSchema.getDouble(base, offset, 1) shouldEqual 1.1d
      schema.ingestionSchema.asJavaString(base, offset, 2) shouldEqual "num_partitions"

      val consumer = new StringifyMapItemConsumer()
      schema.ingestionSchema.consumeMapItems(base, offset, 3, consumer)
      // if there is an undefined key, it should result in empty tag
      consumer.stringPairs.toMap shouldEqual Map("_pi_" -> "0", "_ns_" -> "filodb", "_ws_" -> "demo")
    }
  }

  it("should not return any records if metric missing") {
    val proto1 = TimeSeriesFixture.timeseries(0, baseTags)
    val records = PrometheusInputRecord(proto1)
    records should have length (0)
  }

  it("should copy tags from another key if copyTags defined and original key missing") {
    // add exporter and see if it gets renamed
    val tagsWithExporter = tagsWithMetric + ("exporter" -> "gateway", "_ws_" -> "demo")
    val proto1 = TimeSeriesFixture.timeseries(0, tagsWithExporter)
    val records = PrometheusInputRecord(proto1)
    records should have length (1)
    val record1 = records.head
    record1.tags shouldEqual (tagsWithExporter - "__name__" + ("_ns_" -> "gateway"))
    record1.getMetric shouldEqual "num_partitions"
    record1.nonMetricShardValues shouldEqual Seq("gateway", "demo")

    // no exporter.  Nothing added
    val proto2 = TimeSeriesFixture.timeseries(0, tagsWithMetric)
    val records2 = PrometheusInputRecord(proto2)
    records2 should have length (1)
    val record2 = records2.head
    record2.tags shouldEqual (baseTags)
    record2.getMetric shouldEqual "num_partitions"
    record2.nonMetricShardValues shouldEqual Nil
  }
}