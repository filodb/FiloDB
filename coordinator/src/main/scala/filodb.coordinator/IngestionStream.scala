package filodb.coordinator

import com.typesafe.config.Config
import monix.reactive.Observable

import filodb.core.memstore.SomeData
import filodb.core.metadata.Dataset

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
   * NOTE: this does not cancel any subscriptions to the Observable.  That should be done prior to
   * calling this, which is more for release of resources.
   */
  def teardown(): Unit
}

object IngestionStream {
  /**
   * Wraps a simple observable into an IngestionStream with no teardown behavior
   */
  def apply(stream: Observable[SomeData]): IngestionStream = new IngestionStream {
    val get = stream
    def teardown(): Unit = {}
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
   * @param dataset the Dataset to ingest into
   * @param shard  the shard number
   * @param offset Some(offset) to rewind the source to a particular "offset" for recovery
   */
  def create(config: Config, dataset: Dataset, shard: Int, offset: Option[Long]): IngestionStream
}

/**
 * An IngestionStreamFactory to use when you want to just push manually to a coord.  Used for testing.
 */
class NoOpStreamFactory extends IngestionStreamFactory {
  def create(config: Config, dataset: Dataset, shard: Int, offset: Option[Long]): IngestionStream =
      IngestionStream.empty
}
