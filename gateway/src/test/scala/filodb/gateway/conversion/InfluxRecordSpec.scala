package filodb.gateway.conversion

import org.jboss.netty.buffer.ChannelBuffers
import org.scalatest.{FunSpec, Matchers}

import filodb.core.binaryrecord2.{RecordBuilder, StringifyMapItemConsumer}
import filodb.memory.MemFactory
import filodb.prometheus.FormatConversion

class InfluxRecordSpec extends FunSpec with Matchers {
  // First one has app tag
  // Second one does not
  // Third one is a histogram
  val rawInfluxLPs = Seq(
    "recovery_row_skipped_total,dataset=timeseries,host=MacBook-Pro-229.local,_ns=filodb counter=0 1536790212000000000",
    "num_partitions,dataset=timeseries,host=MacBook-Pro-229.local,shard=1 counter=0 1536790212000000000",
    "num_partitions,dataset=timeseries,host=MacBook-Pro-229.local,shard=0,_ns=filodb counter=0 1536790212000000000",
    "num_partitions,dataset=timeseries,host=MacBook-Pro-229.local,shard=1,_ns=filodb counter=0 1536790212000000000",
    "memstore_flushes_success_total,dataset=timeseries,host=MacBook-Pro-229.local,shard=1,url=http://localhost:9095 counter=0 1536628260000000000",
    "span_processing_time_seconds,error=false,host=MacBook-Pro-229.local,operation=memstore-recover-index-latency +Inf=2,0.005=0,0.01=0,0.025=0,0.05=0,0.075=0,0.1=1,0.25=2,0.5=2,0.75=2,1=2,10=2,2.5=2,5=2,7.5=2,count=2,sum=0.230162432 1536790212000000000"
  )

  val dataset = FormatConversion.dataset
  val buffer = ChannelBuffers.buffer(8192)

  def convertToRecords(rawText: Seq[String]): Seq[Option[InfluxRecord]] = {
    rawText.map { line =>
      buffer.writeBytes(line.getBytes())
      InfluxProtocolParser.parse(buffer, dataset.options)
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

      // app not in tags, return nothing
      recordOpts(1).isDefined shouldEqual true
      recordOpts(1).get.nonMetricShardValues shouldEqual Nil
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
      val builder = new RecordBuilder(MemFactory.onHeapFactory, dataset.ingestionSchema)
      // println(recordOpts(0).get)
      recordOpts(0).get.addToBuilder(builder)
      builder.allContainers.head.foreach { case (base, offset) =>
        dataset.ingestionSchema.partitionHash(base, offset) should not equal (7)
        dataset.ingestionSchema.getLong(base, offset, 0) shouldEqual 1536790212000L
        dataset.ingestionSchema.getDouble(base, offset, 1) shouldEqual 0.0

        val consumer = new StringifyMapItemConsumer()
        dataset.ingestionSchema.consumeMapItems(base, offset, 2, consumer)
        consumer.stringPairs.toMap shouldEqual Map("dataset" -> "timeseries",
                                                   "host" -> "MacBook-Pro-229.local",
                                                   "_ns" -> "filodb",
                                                   "__name__" -> "recovery_row_skipped_total")
      }
    }
  }

  describe("InfluxPromHistogramRecord") {
    it("should create multiple BinaryRecordV2s with addToBuilder, one for each bucket") {
      val recordOpts = convertToRecords(rawInfluxLPs drop 5)
      val builder = new RecordBuilder(MemFactory.onHeapFactory, dataset.ingestionSchema)
      println(recordOpts(0).get)
      recordOpts(0).get.addToBuilder(builder)
      builder.allContainers.head.countRecords shouldEqual 17
      builder.allContainers.head.foreach { case (base, offset) =>
        dataset.ingestionSchema.partitionHash(base, offset) should not equal (7)
        dataset.ingestionSchema.getLong(base, offset, 0) shouldEqual 1536790212000L
        val consumer = new StringifyMapItemConsumer()
        dataset.ingestionSchema.consumeMapItems(base, offset, 2, consumer)
        val map = consumer.stringPairs.toMap
        map("__name__") should startWith ("span_processing_time_seconds")
        map("operation") shouldEqual "memstore-recover-index-latency"
        map("__name__").drop("span_processing_time_seconds".length) match {
          case "_sum"    => dataset.ingestionSchema.getDouble(base, offset, 1) shouldEqual 0.230162432 +- 0.00000001
          case "_count"  => dataset.ingestionSchema.getDouble(base, offset, 1) shouldEqual 2
          case "_bucket" => map.contains("le") shouldEqual true
        }
      }
    }
  }
}