package filodb.coordinator.flight

import java.net.InetAddress
import java.util.Collections
import java.util.concurrent.Executors

import scala.annotation.nowarn

import akka.actor.ActorRef
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import io.grpc.{BindableService, Server}
import io.grpc.netty.NettyServerBuilder
import monix.eval.Task
import org.apache.arrow.flight._
import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.flight.auth.ServerAuthHandler
import org.apache.arrow.memory.BufferAllocator

import filodb.core.memstore.TimeSeriesStore
import filodb.core.query._
import filodb.query.{QueryError, QueryResponse}
import filodb.query.exec.ExecPlan


/**
 * FiloDB Flight Producer for single-partition queries - serves Flight RPCs for FiloDB single-partition queries
 * It extends FlightQueryExecutor to execute query plans and stream results back to client using Flight.
 * @param memStore memstore to execute queries against
 * @param serverAllocator allocator for Flight buffers
 * @param location location advertised to clients for where to connect for flight RPCs. Not used during invocation now.
 * @param sysConfig system config
 */
class FiloDBSinglePartitionFlightProducer(
            val memStore: TimeSeriesStore,
            val serverAllocator: BufferAllocator,
            val location: Location,
            val sysConfig: Config) extends NoOpFlightProducer with FlightQueryResultStreaming {

  override def listActions(context: FlightProducer.CallContext,
                           listener: FlightProducer.StreamListener[ActionType]): Unit = {
    // empty for now since this is only for reads, no updates or actions supported
    listener.onCompleted()
  }

  override def listFlights(context: FlightProducer.CallContext, criteria: Criteria,
                           listener: FlightProducer.StreamListener[FlightInfo]): Unit = {
    // empty for now - we dont support listing flights since we only support Command FlightDescriptors
    listener.onCompleted()
  }

  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
    if (!descriptor.isCommand) {
      throw new UnsupportedOperationException("Only Command FlightDescriptors are supported")
    } else {
      val flightEndpoint = new FlightEndpoint(new Ticket(descriptor.getCommand), location)
      new FlightInfo(ArrowSerializedRangeVectorOps.arrowSrvSchema,
        descriptor, Collections.singletonList(flightEndpoint), -1, -1)
    }
  }

  def executePlan(q: ExecPlan, querySession: QuerySession): Task[QueryResponse] = {
    q.execute(memStore, querySession)(queryScheduler)
      .executeOn(queryScheduler)
      .asyncBoundary
      .onErrorHandle { t =>
        QueryError(q.queryContext.queryId, querySession.queryStats, t)
      }
  }

  /**
   * Handle doGet requests - execute query plan and stream results
   */
  // scalastyle:off method.length
  override def getStream(context: FlightProducer.CallContext,
                         ticket: Ticket,
                         listener: ServerStreamListener): Unit = {
    try {
        FlightKryoSerDeser.deserialize(ticket.getBytes) match {
          case execPlan: ExecPlan =>
            executePhysicalPlanAndRespond(context, execPlan, listener)
          case other =>
            val errMsg = s"Invalid ticket type ${other.getClass.getName}, expected ExecPlan"
            logger.error(errMsg)
            listener.error(new IllegalArgumentException(errMsg))
        }
    } catch {
      case ex: Throwable =>
        logger.error("Error executing plan", ex)
        listener.error(ex)
    }
  }
}

object FiloDBSinglePartitionFlightProducer extends StrictLogging {

  def akkaPortToFlightPort(akkaPort: Int): Int = akkaPort + 5000

  def akkaActorToFlightLocation(actor: ActorRef): Location = {
    val host = actor.path.address.host.get
    val port = akkaPortToFlightPort(actor.path.address.port.get)
    Location.forGrpcInsecure(host, port)
  }

  def start(memStore: TimeSeriesStore, allConfig: Config): Server = {

    val compressionEnabled = allConfig.getBoolean("filodb.flight.compression-enabled")
    val host = {
      val h = allConfig.getString("akka.remote.netty.tcp.hostname") // for now, use akka hostname
      if (h.isEmpty) InetAddress.getLocalHost.getHostAddress else h
    }
    val port = akkaPortToFlightPort(allConfig.getInt("akka.remote.netty.tcp.port"))
    val location = Location.forGrpcInsecure(host, port)
    val executor = Executors.newCachedThreadPool()
    @nowarn
    val svc: BindableService = FlightGrpcUtils.createFlightService(FlightAllocator.serverAllocator,
      new FiloDBSinglePartitionFlightProducer(memStore, FlightAllocator.serverAllocator, location, allConfig),
      ServerAuthHandler.NO_OP,
      executor)

    val server1 = NettyServerBuilder.forPort(port)
    val server2 = if (compressionEnabled) {
      server1.intercept(ZstdServerInterceptor)
        .compressorRegistry(ZstdCodecs.compressorRegistry)
        .decompressorRegistry(ZstdCodecs.decompressorRegistry)
    } else server1
    val server3 = server2.addService(svc).build()
    logger.info(s"Starting FiloDB Flight server on $host:$port with compression = $compressionEnabled")
    server3.start()
    server3
  }
}
