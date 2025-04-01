package filodb.coordinator

import com.typesafe.config.Config
import monix.reactive.Observable

import filodb.core.memstore.SomeData
import filodb.core.metadata.Schemas

/**
 * Unlike the RowSource, an IngestionStream simply provides a stream of records, keeping things simple.
 * It is the responsibility of subscribers (code in FiloDB coordinator, usually) to then perform ingestion
 * and routing as necessary.  Reactive API allows for backpressure to be propagated back.
 */
trait IngestionStream {
  /**
   * Should return the observable for the stream.  Ideally should be cached or be not expensive, should
   * return the same stream every time.
   */
  def get: Observable[SomeData]

  /**
   * @return returns the offset of the last record in the stream, if applicable. This is used in
   *         "IngestionActor.doRecovery" method to retrieve the ending watermark for shard recovery.
   */
  def endOffset: Option[Long]

  /**
   * NOTE: this does not cancel any subscriptions to the Observable.  That should be done prior to
   * calling this, which is more for release of resources.
   * @param isForced if true, then the teardown should clean up all the resources without waiting for the scheduler
   *                 to invoke the cancel-task, which cleans up the state. For example, in the case of Kafka, this
   *                 would mean closing the Kafka consumer and releasing the resources, without waiting for the
   *                 KafkaConsumerObservable task to cancel.
   */
  def teardown(isForced: Boolean = false): Unit
}

object IngestionStream {
  /**
   * Wraps a simple observable into an IngestionStream with no teardown behavior
   */
  def apply(stream: Observable[SomeData]): IngestionStream = new IngestionStream {
    val get = stream
    def teardown(isForced: Boolean): Unit = {}
    def endOffset: Option[Long] = None
  }

  val empty = apply(Observable.empty[SomeData])
}

/**
 * A zero-arg constructor class that knows how to create an IngestionStream.
 */
trait IngestionStreamFactory {
  /**
   * Returns an IngestionStream that can be subscribed to for a given shard.
   * If a source does not support streams for n shards, it could support just one shard and require
   * users to limit the number of shards.
   * @param config the configuration for the data source.  For an example see the sourceconfig {} in
   *               ingestion.md or `conf/timeseries-dev-source.conf`
   * @param schemas the Schemas that this StreamFactory must ingest
   * @param shard  the shard number
   * @param offset Some(offset) to rewind the source to a particular "offset" for recovery
   */
  def create(config: Config, schemas: Schemas, shard: Int, offset: Option[Long]): IngestionStream
}

/**
 * An IngestionStreamFactory to use when you want to just push manually to a coord.  Used for testing.
 */
class NoOpStreamFactory extends IngestionStreamFactory {
  def create(config: Config, schemas: Schemas, shard: Int, offset: Option[Long]): IngestionStream =
      IngestionStream.empty
}
