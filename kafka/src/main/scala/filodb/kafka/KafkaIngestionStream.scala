package filodb.kafka

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.reactive.Observable
import org.apache.kafka.common.TopicPartition

import filodb.coordinator.{IngestionStream, IngestionStreamFactory}
import filodb.core.memstore.IngestRecord
import filodb.core.metadata.RichProjection

/** Used by the coordinator.
  *
  * INTERNAL API.
  *
  * @param config     the Typesafe config to use
  *
  * @param projection the projection for the dataset
  *
  * @param shard      the shard / partition
  */
private[filodb] class KafkaIngestionStream(config: Config,
                                           projection: RichProjection,
                                           shard: Int) extends IngestionStream with StrictLogging {

  private val settings = new KafkaSettings(config)
  import settings._

  private val converter = RecordConverter(RecordConverterClass)

  private val tp = new TopicPartition(IngestionTopic, shard)

  logger.info(s"Creating consumer assigned to topic ${tp.topic} partition ${tp.partition}")
  private val consumer = PartitionedConsumerObservable.create(settings, tp)

  override def get: Observable[Seq[IngestRecord]] =
    consumer.map { record =>
      converter.convert(projection, record.value.asInstanceOf[AnyRef], record.partition, record.offset)
    }

  override def teardown(): Unit = {
    logger.info(s"Shutting down stream $tp")
    // consumer does callback to close but confirm
   }
}

/** The no-arg constructor `IngestionFactory` for the kafka ingestion stream.
  *
  * INTERNAL API.
  */
class KafkaIngestionStreamFactory extends IngestionStreamFactory {

  /**
    * Returns an IngestionStream that can be subscribed to for a given shard.
    * If a source does not support streams for n shards, it could support just one shard and require
    * users to limit the number of shards.
    *
    * @param config     the configuration for the data source
    *
    * @param projection the projection for the dataset
    */
  override def create(config: Config, projection: RichProjection, shard: Int): IngestionStream = {
    new KafkaIngestionStream(config, projection, shard)
  }
}
