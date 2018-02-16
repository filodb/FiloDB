package filodb.kafka

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.reactive.Observable
import org.apache.kafka.common.TopicPartition

import filodb.coordinator.{GlobalConfig, IngestionStream, IngestionStreamFactory}
import filodb.core.memstore.IngestRecord
import filodb.core.metadata.Dataset

/** Used by the coordinator.
  * INTERNAL API.
  *
  * @param config  the Typesafe source config to use, see sourceconfig in `docs/ingestion.md`
  * @param dataset the dataset
  * @param shard   the shard / partition
  */
class KafkaIngestionStream(config: Config,
                           dataset: Dataset,
                           shard: Int,
                           offset: Option[Long]) extends IngestionStream with StrictLogging {

  protected val settings = new KafkaSettings(config)
  import settings._

  private val converter = RecordConverter(RecordConverterClass, dataset)
  private val tp = new TopicPartition(IngestionTopic, shard)

  logger.info(s"Creating consumer assigned to topic ${tp.topic} partition ${tp.partition} offset $offset")
  protected val consumer = PartitionedConsumerObservable.create(settings, tp, offset)

  /**
   * Returns a reactive Observable stream of IngestRecord sequences from Kafka.
   * NOTE: the scheduler used makes a huge difference.
   * The IO scheduler allows all the Kafka partiton inits to happen at beginning,
   *   & allows lots of simultaneous streams to stream efficiently.
   * The global scheduler allows parallel stream init, multiple streams to consume in parallel, but REALLY slowly
   * The computation() sched seems to behave like a round robin: it seems to take turns pulling from only
   *   one or a few partitions at a time; probably doesn't work when you have lots of streams
   */
  override def get: Observable[Seq[IngestRecord]] =
    consumer.map { record =>
      converter.convert(record.value.asInstanceOf[AnyRef], record.partition, record.offset)
    }.executeOn(GlobalConfig.ioPool)

  override def teardown(): Unit = {
    logger.info(s"Shutting down stream $tp")
    // consumer does callback to close but confirm
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
  override def create(config: Config, dataset: Dataset, shard: Int, offset: Option[Long]): IngestionStream = {
    new KafkaIngestionStream(config, dataset, shard, offset)
  }
}
