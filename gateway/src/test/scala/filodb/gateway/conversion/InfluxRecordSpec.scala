package filodb.gateway.conversion

import org.jboss.netty.buffer.ChannelBuffers
import org.scalatest.{FunSpec, Matchers}

import filodb.core.binaryrecord2.{RecordBuilder, RecordSchema, StringifyMapItemConsumer}
import filodb.core.metadata.Schemas
import filodb.memory.MemFactory

class InfluxRecordSpec extends FunSpec with Matchers {
  // First one has app tag
  // Second one does not
  // Third one is a histogram
  val rawInfluxLPs = Seq(
    "recovery_row_skipped_total,dataset=timeseries,host=MacBook-Pro-229.local,_ws_=demo,_ns_=filodb counter=0 1536790212000000000",
    "num_partitions,dataset=timeseries,host=MacBook-Pro-229.local,shard=1 counter=0 1536790212000000000",
    "num_partitions,dataset=timeseries,host=MacBook-Pro-229.local,shard=0,_ws_=demo,_ns_=filodb counter=0 1536790212000000000",
    "num_partitions,dataset=timeseries,host=MacBook-Pro-229.local,shard=1,_ws_=demo,_ns_=filodb counter=0 1536790212000000000",
    "memstore_flushes_success_total,dataset=timeseries,host=MacBook-Pro-229.local,shard=1,url=http://localhost:9095 gauge=5 1536628260000000000",
    "span_processing_time_seconds,error=false,host=MacBook-Pro-229.local,operation=memstore-recover-index-latency 0.075=37,2.5=47,5=47,sum=6.287654912,0.025=8,0.05=25,0.75=47,+Inf=47,count=5,0.5=42,0.25=40,0.1=40 1536790212000000000"
  )

  val schema = Schemas.promCounter
  val buffer = ChannelBuffers.buffer(8192)

  def convertToRecords(rawText: Seq[String]): Seq[Option[InfluxRecord]] = {
    rawText.map { line =>
      buffer.writeBytes(line.getBytes())
      InfluxProtocolParser.parse(buffer)
    }
  }

  describe("InfluxPromSingleRecord") {
    it("can getMetric") {
      val recordOpts = convertToRecords(rawInfluxLPs take 2)
      recordOpts(0).isDefined shouldEqual true
      recordOpts(0).get.getMetric shouldEqual "recovery_row_skipped_total"

      recordOpts(1).isDefined shouldEqual true
      recordOpts(1).get.getMetric shouldEqual "num_partitions"
    }

    it("can read nonMetricShardValues") {
      val recordOpts = convertToRecords(rawInfluxLPs take 2)
      recordOpts(0).isDefined shouldEqual true
      recordOpts(0).get.nonMetricShardValues shouldEqual Seq("filodb")
      recordOpts(0).get.schema shouldEqual Schemas.promCounter

      // app not in tags, return nothing
      recordOpts(1).isDefined shouldEqual true
      recordOpts(1).get.nonMetricShardValues shouldEqual Nil
      recordOpts(1).get.schema shouldEqual Schemas.promCounter
    }

    it("should return varying results for shard key and partition hashes") {
      val recordOpts = convertToRecords(rawInfluxLPs take 5)

      // third and fourth should return same shard key hash and differing part key hashes
      val thirdShardHash = recordOpts(2).get.shardKeyHash
      thirdShardHash should not equal (7)
      recordOpts(3).get.shardKeyHash shouldEqual thirdShardHash
      println(thirdShardHash)

      // Should match what is computable with shardKeyHash method
      thirdShardHash shouldEqual RecordBuilder.shardKeyHash(Seq("filodb"), "num_partitions")

      val thirdPartHash = recordOpts(2).get.partitionKeyHash
      thirdPartHash should not equal (7)
      recordOpts(3).get.partitionKeyHash should not equal (thirdPartHash)

      // first and third shard hash should not be the same
      recordOpts(0).get.shardKeyHash should not equal (thirdShardHash)
    }

    it("should create a full ingestion BinaryRecordV2 with addToBuilder") {
      val recordOpts = convertToRecords(rawInfluxLPs take 1)
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      // println(recordOpts(0).get)
      recordOpts(0).get.addToBuilder(builder)
      builder.allContainers.head.foreach { case (base, offset) =>
        RecordSchema.schemaID(base, offset) shouldEqual Schemas.promCounter.schemaHash
        schema.ingestionSchema.partitionHash(base, offset) should not equal (7)
        schema.ingestionSchema.getLong(base, offset, 0) shouldEqual 1536790212000L
        schema.ingestionSchema.getDouble(base, offset, 1) shouldEqual 0.0

        schema.ingestionSchema.asJavaString(base, offset, 2) shouldEqual "recovery_row_skipped_total"

        val consumer = new StringifyMapItemConsumer()
        schema.ingestionSchema.consumeMapItems(base, offset, 3, consumer)
        consumer.stringPairs.toMap shouldEqual Map("dataset" -> "timeseries",
                                                   "host" -> "MacBook-Pro-229.local",
                                                   "_ws_" -> "demo",
                                                   "_ns_" -> "filodb")
      }

      builder.reset()
      val recordOpts2 = convertToRecords(rawInfluxLPs drop 4 take 1)
      recordOpts2(0).get.addToBuilder(builder)
      builder.allContainers.head.foreach { case (base, offset) =>
        RecordSchema.schemaID(base, offset) shouldEqual Schemas.gauge.schemaHash
        schema.ingestionSchema.partitionHash(base, offset) should not equal (7)
        schema.ingestionSchema.getLong(base, offset, 0) shouldEqual 1536628260000L
        schema.ingestionSchema.getDouble(base, offset, 1) shouldEqual 5.0

        schema.ingestionSchema.asJavaString(base, offset, 2) shouldEqual "memstore_flushes_success_total"

        val consumer = new StringifyMapItemConsumer()
        schema.ingestionSchema.consumeMapItems(base, offset, 3, consumer)
        consumer.stringPairs.toMap shouldEqual Map("dataset" -> "timeseries",
                                                   "host" -> "MacBook-Pro-229.local",
                                                   "shard" -> "1",
                                                   "url" -> "http://localhost:9095")
      }
    }
  }

  describe("InfluxHistogramRecord") {
    it("should create single BinaryRecordV2s with addToBuilder with FiloDB histogram") {
      val recordOpts = convertToRecords(rawInfluxLPs drop 5)
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      println(recordOpts(0).get)
      recordOpts(0).get.addToBuilder(builder)
      builder.allContainers.head.countRecords shouldEqual 1

      val ingSchema = Schemas.promHistogram.ingestionSchema
      builder.allContainers.head.foreach { case (base, offset) =>
        RecordSchema.schemaID(base, offset) shouldEqual Schemas.promHistogram.schemaHash
        ingSchema.partitionHash(base, offset) should not equal (7)

        val consumer = new StringifyMapItemConsumer()
        ingSchema.consumeMapItems(base, offset, 5, consumer)
        val map = consumer.stringPairs.toMap
        map("operation") shouldEqual "memstore-recover-index-latency"
        map.keySet shouldEqual Set("operation", "host", "error")
      }

      builder.allContainers.head.iterate(ingSchema).foreach { reader =>
        reader.getLong(0) shouldEqual 1536790212000L

        val metric = reader.getString(4)
        metric shouldEqual "span_processing_time_seconds"

        // sum
        reader.getDouble(1) shouldEqual 6.287654912 +- 0.00000001
        // count
        reader.getDouble(2) shouldEqual 5

        val hist = reader.getHistogram(3)
        hist.numBuckets shouldEqual 10
        // 0.075=37,2.5=47,5=47,sum=6.287654912,0.025=8,0.05=25,0.75=47,+Inf=47,count=5,0.5=42,0.25=40,0.1=40
        val expectedTops = Seq(0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 2.5, 5, Double.PositiveInfinity)
        (0 until hist.numBuckets).foreach { n =>
          hist.bucketTop(n) shouldEqual expectedTops(n) +- 0.000001
        }
        hist.bucketValue(2) shouldEqual 37
        hist.bucketValue(4) shouldEqual 40
      }
    }
  }
}