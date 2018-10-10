package filodb.gateway.conversion

import org.scalatest.{FunSpec, Matchers}

import remote.RemoteStorage.{LabelPair, Sample, TimeSeries}

import filodb.core.binaryrecord2.{RecordBuilder, StringifyMapItemConsumer}
import filodb.memory.MemFactory
import filodb.prometheus.FormatConversion

object TimeSeriesFixture {
  //  "num_partitions,dataset=timeseries,host=MacBook-Pro-229.local,shard=0,app=filodb counter=0 1536790212000000000",
  def timeseries(no: Int, tags: Map[String, String]): TimeSeries = {
    val builder = TimeSeries.newBuilder
                    .addSamples(Sample.newBuilder.setTimestampMs(1000000L + no).setValue(1.1 + no).build)
    tags.foldLeft(builder) { case (b, (k, v)) =>
      b.addLabels(LabelPair.newBuilder.setName(k).setValue(v).build)
    }.build
  }
}

class PrometheusInputRecordSpec extends FunSpec with Matchers {
  val dataset = FormatConversion.dataset
  val baseTags = Map("dataset" -> "timeseries",
                     "host" -> "MacBook-Pro-229.local",
                     "shard" -> "0")
  val tagsWithMetric = baseTags + ("__name__" -> "num_partitions")

  it("should parse from TimeSeries proto and write to RecordBuilder") {
    val proto1 = TimeSeriesFixture.timeseries(0, tagsWithMetric + ("app" -> "filodb"))
    val builder = new RecordBuilder(MemFactory.onHeapFactory, dataset.ingestionSchema)

    val records = PrometheusInputRecord(proto1, dataset)
    records should have length (1)
    val record1 = records.head
    record1.tags shouldEqual (baseTags + ("app" -> "filodb"))
    record1.getMetric shouldEqual "num_partitions"
    record1.nonMetricShardValues shouldEqual Seq("filodb")

    record1.shardKeyHash shouldEqual RecordBuilder.shardKeyHash(Seq("filodb"), "num_partitions")

    record1.addToBuilder(builder)
    builder.allContainers.head.foreach { case (base, offset) =>
      dataset.ingestionSchema.partitionHash(base, offset) should not equal (7)
      dataset.ingestionSchema.getLong(base, offset, 0) shouldEqual 1000000L
      dataset.ingestionSchema.getDouble(base, offset, 1) shouldEqual 1.1

      val consumer = new StringifyMapItemConsumer()
      dataset.ingestionSchema.consumeMapItems(base, offset, 2, consumer)
      consumer.stringPairs.toMap shouldEqual (tagsWithMetric + ("app" -> "filodb"))
    }
  }

  it("should not return any records if metric missing") {
    val proto1 = TimeSeriesFixture.timeseries(0, baseTags)
    val records = PrometheusInputRecord(proto1, dataset)
    records should have length (0)
  }
}