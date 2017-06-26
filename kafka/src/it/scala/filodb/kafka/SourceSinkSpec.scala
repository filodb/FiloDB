package filodb.kafka

import scala.concurrent.duration._
import scala.concurrent.Future

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.scalatest.WordSpecLike

import filodb.core.memstore.{IngestRecord, TimeSeriesMemStore}
import filodb.core.metadata.{Column, DataColumn, Dataset, RichProjection}

/** Start Zookeeper and Kafka.
  * Run ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 128 --topic sstest2
  * Run sbt kafka/it:testOnly filodb.kafka.SourceSinkSpec
  */
class SourceSinkSpec extends TestKit(ActorSystem("source-sink"))
  with BaseSpec with WordSpecLike with ImplicitSender with StrictLogging {

  import filodb.core._
  import Column.ColumnType._
  import system.dispatcher

  private val expected = 2

  implicit val timeout: Timeout = 5.seconds

  private val schema = Seq(DataColumn(0, "timestamp", "timestamp", 0, StringColumn))
  private val dataset = Dataset("metrics", "timestamp", ":string 0")
  private val datasetRef = DatasetRef(dataset.name)
  private val projection = RichProjection(dataset, schema)

  private val memStore = new TimeSeriesMemStore(ConfigFactory.load.getConfig("filodb"))
  memStore.setup(projection)

  override def beforeAll(): Unit = {
    System.setProperty("filodb.kafka.clients.config", "./kafka/src/test/resources/kafka.client.properties")
  }

  override def beforeEach(): Unit = memStore.reset()

  override def afterAll(): Unit = {
    memStore.shutdown()
    system.shutdown()
  }

  override def afterEach(): Unit = memStore.reset()

  "KafkaSource" must {
    "should connect streaming source directly to sink" in {

      val source = KafkaSource(system)
      val subscriber = TestProbe()
      source.to(subscriber.ref, projection, memStore)

      val sink = KafkaSink(system)
      for (p <- 0 until expected) sink.publish(p.toLong, UtcTime.now.toString)

      val received = subscriber.receiveN(expected)
      awaitCond(received.size == expected, timeout.duration * 2)
      logger.info(s"Received ${received.size} records")
    }
    "should support the current factory create functionality" in {

      val source = KafkaSource(system)
      val factory = source.factory(projection, memStore)
      val rowSource = system.actorOf(factory.create(system.settings.config, projection, memStore))

      val sink = KafkaSink(system)
      for (p <- 0 until expected) sink.publish(p.toLong, UtcTime.now.toString)

      def received: Boolean = {
        import QueueRowSource._

        rowSource ! GetQueueCount
        expectMsgPF(timeout.duration) {
          case QueueCount(count) if count >= expected =>
            logger.debug(s"Received $count records")
            true
          case _ =>
            Thread.sleep(1000)
            received
        }
      }

      awaitCond(received, timeout.duration)
    }

    "should support fu" in {

      val source = KafkaSource(system)
      val subscriber = TestProbe()
      source.to(subscriber.ref, projection, memStore)

      val sink = KafkaSink(system)
      val sent = for (key <- 0 until expected) yield {
        sink.publishWithAck(key.toLong, UtcTime.now.toString).collect {
          case task@Sink.PublishSuccess(eventTime, topic, partition, offset) =>
            logger.debug(s"Published: topic: $topic, partition: $partition, offset: $offset")
            offset
        }}
      val sentF = Future.sequence(sent)
      awaitCond(sentF.isCompleted && sent.size == expected, timeout.duration * 2)

      val received = subscriber.receiveN(expected).collect {
        case record: IngestRecord => record.offset
      }
      awaitCond(received.size == expected, timeout.duration * 2)

      var sentOffsetRange = (0L,0L)
      var receivedOffsetRange = (0L,0L)
      val results = for {
        offsets <- sentF
      } yield {
        val receivedOffsetsEqSent = offsets.forall { sentOffset =>
          received.collect { case offset if offset == sentOffset =>
            logger.debug(s"sent offset: $sentOffset, received offset $offset")
          }
          received.contains(sentOffset)
        }
        sentOffsetRange = (offsets.min, offsets.max)
        receivedOffsetRange = (received.min,received.max)
      }

      logger.debug(s"sent offset: $sentOffsetRange, received offset $receivedOffsetRange")

    }
  }
}
