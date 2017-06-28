package filodb.coordinator

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.event.LoggingReceive
import akka.pattern.ask
import akka.util.Timeout
import kamon.Kamon
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.language.existentials

import filodb.core._
import filodb.core.metadata.RichProjection
import filodb.core.memstore.{IngestRecord, DatasetAlreadySetup}

object IngestProtocol {
  final case class MoreRows(shard: Int, records: Seq[IngestRecord])
  final case class IngestionErr(msg: String, cause: Option[Throwable] = None)

  def sendRowsGetAck(protocol: ActorRef,
                     shard: Int,
                     rows: Seq[IngestRecord],
                     ackTimeout: FiniteDuration = 30.seconds): Future[Any] = {
    implicit val timeout = Timeout(ackTimeout)
    protocol ? MoreRows(shard, rows)
  }

  def props(clusterActor: ActorRef, ref: DatasetRef, version: Int = 0): Props =
    Props(classOf[IngestProtocol], clusterActor, ref, version)
}

/**
 * IngestProtocol is an actor that handles network sends of ingestion records to destination
 * NodeCoordinators, including the backpressure/at least once protocol.  The interaction looks like this:
 *   - it sends subscription message for ShardMapUpdates to the cluster actor
 *   - send a Seq[IngestRecord].  Actor will respond with an Ack(offset) when remote node does
 *     Note that the send should be for records of a single shard number.
 *
 * This is meant to work together with a Reactive/Observable stream, in which the Subscriber returns a
 * Future[Ack] to a set of incoming records.  Due to the network latency involved, it is HIGHLY suggested
 * that some kind of batching is done.  The backpressure comes from the next send not being done until
 * another Ack comes back.
 *
 * IngestProtocol goes into waiting state after an ackTimeout and checks ability to replay messages.
 * TBD: not sure this is needed anymore.
 *
 * NOTE: this might very well be going away and be replaced by something like rsocket.io
 */
class IngestProtocol(clusterActor: ActorRef,
                     ref: DatasetRef,
                     version: Int = 0) extends BaseActor {
  import IngestProtocol._

  private var mapper: ShardMapper = ShardMapper.empty

  // Send an initial message to the cluster actor for subscription
  clusterActor ! NodeClusterActor.SubscribeShardUpdates(ref)

  // *** Metrics ***
  private val kamonTags = Map("dataset" -> ref.dataset, "version" -> version.toString)
  private val rowsIngested = Kamon.metrics.counter("protocol-rows-ingested", kamonTags)
  private val shardHist    = Kamon.metrics.histogram("source-shards-distributed", kamonTags)

  import context.dispatcher

  def start: Receive = LoggingReceive {
    case NodeClusterActor.ShardMapUpdate(_, newMap) =>
      logger.info(s"Starting, received initial shard map with ${newMap.numShards} shards")
      mapper = newMap
      logger.info(s" ==> Starting ingestion, waiting for new rows...")
      context.become(reading)
  }

  def mapUpdate: Receive = LoggingReceive {
    case NodeClusterActor.ShardMapUpdate(_, newMap) =>
      logger.info(s"Received new partition map")
      mapper = newMap
  }

  def errorCatcher: Receive = LoggingReceive {
    case IngestionCommands.UnknownDataset =>
      context.parent ! IngestionErr(s"Ingestion actors shut down from ref $sender, check error logs")

    case t: Throwable =>
      context.parent ! IngestionErr(s"Error from $sender, " + t.getMessage, Some(t))

    case e: ErrorResponse =>
      context.parent ! IngestionErr(s"Error from $sender, " + e.toString)
  }

  def reading: Receive = (LoggingReceive {
    case MoreRows(shardNum, records) if records.nonEmpty =>
      val nodeRef = mapper.coordForShard(shardNum)
      // We forward it so that the one who sent us MoreRows will get back the Ack message directly
      // which also means we don't have to deal with responses
      nodeRef.forward(IngestionCommands.IngestRows(ref, version, shardNum, records))
      rowsIngested.increment(records.length)
      shardHist.record(shardNum)
  }) orElse mapUpdate orElse errorCatcher

  val receive = start
}
