package filodb.core.store

import java.util.UUID

import com.typesafe.config.{ConfigException, ConfigFactory, ConfigRenderOptions}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.ScalaFutures

import filodb.core._
import filodb.core.metadata.{Dataset, DatasetOptions}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

trait MetaStoreSpec extends AnyFunSpec with Matchers
with BeforeAndAfter with BeforeAndAfterAll with ScalaFutures {
  def metaStore: MetaStore
  implicit def defaultPatience: PatienceConfig

  override def beforeAll(): Unit = {
    super.beforeAll()
    metaStore.initialize().futureValue
  }

  val dataset = Dataset("foo", Seq("part:string"), Seq("timestamp:long", "value:double"),
    DatasetOptions.DefaultOptions)

  before { metaStore.clearAllData().futureValue }

  describe("checkpoint api") {
    it("should allow reading and writing checkpoints for shard") {
      val ref = DatasetRef("gdelt-" + UUID.randomUUID()) // Add uuid so tests can be rerun

      // when there is no data for a shard, then return Long.MinValue as the checkpoint
      metaStore.readEarliestCheckpoint(ref, 2).futureValue shouldEqual Long.MinValue
      metaStore.readCheckpoints(ref, 2).futureValue.isEmpty shouldBe true

      // should be able to insert checkpoints for new groups
      metaStore.writeCheckpoint(ref, 2, 1, 10).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ref, 2, 2, 9).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ref, 2, 3, 13).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ref, 2, 4, 5).futureValue shouldEqual Success
      metaStore.readEarliestCheckpoint(ref, 2).futureValue shouldEqual 5

      // should be able to update checkpoints for existing groups
      metaStore.writeCheckpoint(ref, 2, 1, 12).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ref, 2, 2, 15).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ref, 2, 3, 17).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ref, 2, 4, 7).futureValue shouldEqual Success
      metaStore.readEarliestCheckpoint(ref, 2).futureValue shouldEqual 7

      // should be able to retrieve raw offsets as well
      val offsets = metaStore.readCheckpoints(ref, 2).futureValue
      offsets.size shouldEqual 4
      offsets(1) shouldEqual 12
      offsets(2) shouldEqual 15
      offsets(3) shouldEqual 17
      offsets(4) shouldEqual 7

      // should be able to clear the table
      metaStore.clearAllData().futureValue
      metaStore.readCheckpoints(ref, 2).futureValue.isEmpty shouldBe true
    }
  }

  private def stripStoreConf(c: IngestionConfig): IngestionConfig =
    c.copy(sourceConfig = c.sourceConfig.withoutPath("store"))

  describe("IngestionConfig API") {
    val config1 = ConfigFactory.parseString("""brokers=["foo.bar.com:1234", "foo.baz.com:5678"]
                                              |auto.offset.commit=false""".stripMargin)
    val factory1 = "filodb.fake.SomeStore"
    val resource1 = ConfigFactory.parseString("""num-shards=100
                                                 min-num-nodes=16""")
    val resource2 = ConfigFactory.parseString("""num-shards=50
                                                 min-num-nodes=10""")
    val source1 = IngestionConfig(dataset.ref, resource1, factory1, config1, TestData.storeConf)
    val source2 = IngestionConfig(DatasetRef("juju"), resource2, factory1, config1, TestData.storeConf)

    it("should be able to parse source config with no sourceFactory") {
      val sourceConf = """
                       |dataset = "gdelt"
                       |num-shards = 32   # for Kafka this should match the number of partitions
                       |min-num-nodes = 10     # This many nodes needed to ingest all shards
                       |sourceconfig {
                       |  store {
                       |    flush-interval = 5m
                       |    shard-mem-size = 500MB
                       |}}""".stripMargin
      val ingestionConfig = IngestionConfig(sourceConf, "a.backup").get
      ingestionConfig.ref shouldEqual DatasetRef("gdelt")
      ingestionConfig.streamFactoryClass shouldEqual "a.backup"
      ingestionConfig.sourceConfig.isEmpty shouldEqual false
    }

    val exampleSourceConf = """
                       |dataset = "gdelt"
                       |num-shards = 128
                       |min-num-nodes = 32
                       |sourceconfig {
                       |  filo-topic-name = "org.example.app.topic1"
                       |  bootstrap.servers = "host:port"
                       |  filo-record-converter = "org.example.app.SomeRecordConverter"
                       |  value.deserializer=com.apple.pie.filodb.timeseries.TimeSeriesDeserializer
                       |  group.id = "org.example.app.consumer.group1"
                       |  my.custom.key = "custom.value"
                       |  store {
                       |    flush-interval = 5m
                       |    shard-mem-size = 500MB
                       |  }
                       |  downsample {
                       |    enabled = true
                       |    resolutions-ms = [ 60000 ]
                       |    publisher-class = "filodb.kafka.KafkaDownsamplePublisher"
                       |    publisher-config {
                       |      kafka {
                       |        bootstrap.servers = "localhost:9092"
                       |        group.id = "filo-db-timeseries-downsample"
                       |      }
                       |      topics {
                       |        60000 = "ts-downsample-60000"
                       |      }
                       |      max-queue-size = 5000
                       |      min-queue-size = 100
                       |      consume-batch-size = 20
                       |    }
                       |  }
                       |}
                     """.stripMargin


    it("should be able to parse for IngestionConfig with sourceconfig and downsample config") {
      val ingestionConfig = IngestionConfig(exampleSourceConf, "a.backup").get
      ingestionConfig.ref shouldEqual DatasetRef("gdelt")
      ingestionConfig.streamFactoryClass shouldEqual "a.backup"
      ingestionConfig.sourceConfig.isEmpty shouldEqual false
      ingestionConfig.sourceConfig.getString("group.id") shouldEqual "org.example.app.consumer.group1"
      ingestionConfig.sourceConfig.getString("my.custom.key") shouldEqual "custom.value"
      ingestionConfig.downsampleConfig.config.getString("publisher-class")shouldEqual
                               "filodb.kafka.KafkaDownsamplePublisher"
    }

    it("should be able to store and read back same source config") {
      val ingestionConfig = IngestionConfig(exampleSourceConf, "a.backup").get
      val storedConfig = ingestionConfig.sourceStoreConfig.root.render(ConfigRenderOptions.concise)
      val readConfig = IngestionConfig(DatasetRef("gdelt"),
        "a.backup","{\"min-num-nodes\":32,\"num-shards\":128}", storedConfig)
      ingestionConfig.streamFactoryClass shouldEqual "a.backup"
      ingestionConfig.sourceConfig.isEmpty shouldEqual false
      ingestionConfig.sourceConfig.getString("group.id") shouldEqual "org.example.app.consumer.group1"
      ingestionConfig.sourceConfig.getString("my.custom.key") shouldEqual "custom.value"
      ingestionConfig.downsampleConfig.config.getString("publisher-class")shouldEqual
                         "filodb.kafka.KafkaDownsamplePublisher"
      ingestionConfig.storeConfig.flushInterval.toMillis shouldEqual 300000

    }

    it("should return failure context when no dataset is configured") {
      val sourceConf = s"""${IngestionKeys.SourceFactory} = "fu""""

      val ingestionConfig = IngestionConfig(ConfigFactory.parseString(sourceConf))
      ingestionConfig.isFailure shouldEqual true
      ingestionConfig.failed.get.getClass shouldEqual classOf[ConfigException.Missing]
    }

    it("should return failure context when source factory class is configured") {
      val sourceConf = s"""${IngestionKeys.Dataset} = "gdelt"""".stripMargin

      val ingestionConfig = IngestionConfig(ConfigFactory.parseString(sourceConf))
      ingestionConfig.isFailure shouldEqual true
      ingestionConfig.failed.get.getClass shouldEqual classOf[ConfigException.Missing]
    }
  }
}