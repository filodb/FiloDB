package filodb.coordinator

import scala.concurrent.Future

import akka.actor.ActorRef
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.execution.{Ack, Cancelable, Scheduler}
import monix.reactive.Observable

import filodb.core.binaryrecord.BinaryRecord
import filodb.core.memstore.IngestRecord
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
  def get: Observable[Seq[IngestRecord]]

  /**
   * NOTE: this does not cancel any subscriptions to the Observable.  That should be done prior to
   * calling this, which is more for release of resources.
   */
  def teardown(): Unit
}

object IngestionStream {
  import client.IngestionCommands

  /**
   * Wraps a simple observable into an IngestionStream with no teardown behavior
   */
  def apply(stream: Observable[Seq[IngestRecord]]): IngestionStream = new IngestionStream {
    val get = stream
    def teardown(): Unit = {}
  }

  val empty = apply(Observable.empty[Seq[IngestRecord]])

  implicit class RichIngestionStream(stream: IngestionStream) extends StrictLogging {
    import Ack._
    /**
     * Converts incoming stream to BinaryRecords, groups by shard, and subscribes each one to a
     * function that gets an ack back from the remote node before continuing
     */
    def routeToShards(mapper: ShardMapper,
                      dataset: Dataset,
                      protocolActor: ActorRef)
                     (implicit s: Scheduler): Cancelable = {
      def onNext(elem: (Int, Seq[IngestRecord])): Future[Ack] = {
        val (shard, records) = elem
        if (records.isEmpty) { Future.successful(Continue) }
        else {
          // protects IngestionProtocol -> MoreRows() -> mapper.coordForShard(shardNum) NPE
          if (mapper.assignedShards.contains(shard)) {
            IngestProtocol.sendRowsGetAck(protocolActor, shard, records).map {
              case IngestionCommands.Ack(seqNo) if seqNo == records.last.offset => Continue
              case IngestionCommands.Ack(seqNo) =>
                logger.warn(s"Mismatching offsets: got $seqNo, expected ${records.last.offset}")
                Stop
              case other: Any =>
                logger.warn(s"Unexpected result from remote ActorRef: $other... stopping stream")
                Stop
            }
          }
          else {
            logger.warn(s"Shard $shard is not an assigned shard, no coordinator is assigned, in mapper $mapper")
            Future.successful(Continue)
          }
        }
      }

      stream.get.map { records =>
              records.map { r => r.copy(partition = dataset.partKey(r.partition),
                                        data = BinaryRecord(dataset.dataBinSchema, r.data)) }
            }.flatMap { records =>
              val byShardRecords = records.groupBy { r =>
                mapper.partitionToShardNode(r.partition.hashCode).shard
              }
              Observable.fromIterable(byShardRecords)
            }.subscribe(onNext _)
    }
  }
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
 * An IngestionStreamFactory to use when you want to just push manually to a coord.
 */
class NoOpStreamFactory extends IngestionStreamFactory {
  def create(config: Config, dataset: Dataset, shard: Int, offset: Option[Long]): IngestionStream =
      IngestionStream.empty
}
