package filodb.kafka

import scala.concurrent.blocking

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.kafka.{CommittableMessage, KafkaConsumerConfig, KafkaConsumerObservable}
import monix.reactive.Observable
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import filodb.coordinator.{IngestionStream, IngestionStreamFactory}
import filodb.core.binaryrecord2.RecordContainer
import filodb.core.memstore.SomeData
import filodb.core.metadata.Schemas

/**
  * KafkaIngestionStream creates an IngestionStream of RecordContainers by consuming from a single partition
  * in Apache Kafka.  Each message in Kafka is expected to be a RecordContainer's bytes.
  *
  * @param config  the Typesafe source config to use, see sourceconfig in `docs/ingestion.md`
  * @param schemas the Schemas to support reading from
  * @param shard   the shard / partition
  */
class KafkaIngestionStream(config: Config,
                           schemas: Schemas,
                           shard: Int,
                           offset: Option[Long]) extends IngestionStream with StrictLogging {

  private val _sc = new SourceConfig(config, shard)
  import _sc._

  protected def sc: SourceConfig = _sc

  private val tp = new TopicPartition(IngestionTopic, shard)

  logger.info(s"Creating consumer assigned to topic ${tp.topic} partition ${tp.partition} offset $offset")

  /*
  * NOTE: Why are we keeping reference to both KafkaConsumer and KafkaConsumerObservable ?
  * This is because we need the KafkaConsumerObservable for the existing monix style streaming of data.
  * And we need the direct reference to KafkaConsumer for:
  *  1. To fetch the endOffsets for the topic partition. This is used in recovery path to stream to end of ingestion.
  *  2. To close the KafkaConsumer on teardown, when isForced is set to true. This is done to ensure cleanup of
  *     KafkaConsumer resources during recovery and before normalIngestion is started.
  * */
  val kafkaConsumer : KafkaConsumer[Long, Any] = createConsumer(sc, tp, offset)
  protected lazy val consumer = createKCO(sc)
  private[filodb] def createKCO(sourceConfig: SourceConfig
                               ): KafkaConsumerObservable[Long, Any, CommittableMessage[Long, Any]] = {

    val consumer = createConsumerTask(kafkaConsumer)
    val cfg = KafkaConsumerConfig(sourceConfig.asConfig)
    require(!cfg.enableAutoCommit, "'enable.auto.commit' must be false.")

    KafkaConsumerObservable.manualCommit(cfg, consumer)
  }

  private[filodb] def createConsumer(sourceConfig: SourceConfig,
                                     topicPartition: TopicPartition,
                                     offset: Option[Long]): KafkaConsumer[Long, Any] = {
    import scala.jdk.CollectionConverters._
    val props = sourceConfig.asProps
    if (sourceConfig.LogConfig) logger.info(s"Consumer properties: $props")

    blocking {
      props.put("client.id", s"${props.get("group.id")}.${System.getenv("INSTANCE_ID")}.$shard")
      val consumer = new KafkaConsumer(props)
      consumer.assign(List(topicPartition).asJava)
      offset.foreach { off => consumer.seek(topicPartition, off) }
      consumer.asInstanceOf[KafkaConsumer[Long, Any]]
    }
  }

  private[filodb] def createConsumerTask(consumer: KafkaConsumer[Long, Any]): Task[KafkaConsumer[Long, Any]] = {
    Task {
      consumer
    }
  }

  /**
   * Returns a reactive Observable stream of RecordContainers from Kafka.
   * NOTE: the scheduler used makes a huge difference.
   * The IO scheduler allows all the Kafka partition inits to happen at beginning,
   *   & allows lots of simultaneous streams to stream efficiently.
   * The global scheduler allows parallel stream init, multiple streams to consume in parallel, but REALLY slowly
   * The computation() sched seems to behave like a round robin: it seems to take turns pulling from only
   *   one or a few partitions at a time; probably doesn't work when you have lots of streams
   */
  override def get: Observable[SomeData] =
    consumer.map { msg =>
      SomeData(msg.record.value.asInstanceOf[RecordContainer], msg.record.offset)
    }

  override def teardown(isForced: Boolean = false): Unit = {
    logger.info(s"Shutting down stream $tp")
    // the kco consumer on cancel, makes a callback to close the kafka-consumer but confirm

    if (isForced) {
      // Manually closing the kafka consumer on teardown to avoid the `InstanceAlreadyExistsException` when:
      // 1. Moving from "IngestionActor.doRecovery" to "IngestionActor.normalIngestion" state.
      // 2. During multiple iterations of "IngestionActor.doRecoveryWithIngestionStreamEndOffset".
      //
      // NOTE:
      // -------------
      // After completion of "IngestionActor.doRecovery" or "IngestionActor.doRecoveryWithIngestionStreamEndOffset",
      // we DO NOT CLOSE the kafka consumer as IngestionActor.removeAndReleaseResources, which takes care of canceling
      // the KafkaConsumerObservable and close the kafka consumer.
      // Hence, to ensure cleanup of resources correctly, we are closing the kafka consumer here.
      // isForced is set to true only in the "IngestionActor.doRecoveryWithIngestionStreamEndOffset" method.
      kafkaConsumer.close()
    }
  }

  /**
   * @return returns the offset of the last record in the stream, if applicable. This is used in
   *         "IngestionActor.doRecovery" method to retrieve the ending watermark for shard recovery.
   */
  override def endOffset: Option[Long] = {
    // Kafka offsets are Long, so we can return the last offset as the endOffset
    import scala.jdk.CollectionConverters._
    kafkaConsumer.endOffsets(List(tp).asJava).asScala.get(tp) match {
      case Some(offset) => Some(offset)
      case None => None
    }
  }
}

/** The no-arg constructor `IngestionFactory` for the kafka ingestion stream.
  * INTERNAL API.
  */
class KafkaIngestionStreamFactory extends IngestionStreamFactory {

  /**
    * Returns an IngestionStream that can be subscribed to for a given shard,
    * or in this case, a single partition (1 shard => 1 Kafka partition) of a given Kafka topic.
    */
  override def create(config: Config, schemas: Schemas, shard: Int, offset: Option[Long]): IngestionStream = {
    new KafkaIngestionStream(config, schemas, shard, offset)
  }
}
