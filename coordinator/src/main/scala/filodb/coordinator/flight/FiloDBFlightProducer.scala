package filodb.coordinator.flight

import java.net.InetAddress
import java.util
import java.util.{Collections, Optional}
import java.util.concurrent.Executors

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import io.grpc.{BindableService, CallOptions, Channel, ClientCall, ClientInterceptor, Metadata, MethodDescriptor,
                Server, ServerBuilder, ServerCall, ServerCallHandler, ServerInterceptor}
import io.grpc.netty.NettyServerBuilder
import org.apache.arrow.flight._
import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.flight.auth.ServerAuthHandler
import org.apache.arrow.memory.BufferAllocator

import filodb.core.memstore.TimeSeriesStore
import filodb.core.query._
import filodb.query.exec.ExecPlan

/**
 * FiloDB Flight Producer - serves Flight RPCs for FiloDB
 * It extends FlightQueryExecutor to execute queries
 */
class FiloDBFlightProducer(val memStore: TimeSeriesStore,
                           val allocator: BufferAllocator,
                           val location: Location,
                           val sysConfig: Config) extends NoOpFlightProducer with FlightQueryExecutor {

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
      new FlightInfo(ArrowSerializedRangeVector.arrowSrvSchema,
        descriptor, Collections.singletonList(flightEndpoint), -1, -1)
    }
  }

  /**
   * Handle doGet requests - execute query plan and stream results
   */
  // scalastyle:off method.length
  override def getStream(context: FlightProducer.CallContext,
                         ticket: Ticket,
                         listener: ServerStreamListener): Unit = {

    // scalastyle:off null
    try {
      FlightKryoSerDeser.deserialize(ticket.getBytes) match {
        case execPlan: ExecPlan =>
          executePhysicalPlanEntry(context, execPlan, listener)
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

object FiloDBFlightProducer extends StrictLogging {

  def akkaPortToFlightPort(akkaPort: Int): Int = akkaPort + 5000

  def start(memStore: TimeSeriesStore, allConfig: Config): Server = {

    val compressionEnabled = allConfig.getBoolean("filodb.flight.compression-enabled")
    val host = {
      val h = allConfig.getString("akka.remote.netty.tcp.hostname") // for now, use akka hostname
      if (h.isEmpty) InetAddress.getLocalHost.getHostAddress else h
    }
    val port = akkaPortToFlightPort(allConfig.getInt("akka.remote.netty.tcp.port"))
    val location = Location.forGrpcInsecure(host, port)
    val executor = Executors.newCachedThreadPool()
    val noAuthHandler = new ServerAuthHandler {
      override def isValid(token: Array[Byte]): Optional[String] = Optional.of("")
      override def authenticate(outgoing: ServerAuthHandler.ServerAuthSender,
                                incoming: util.Iterator[Array[Byte]]): Boolean = true
    }
    val svc: BindableService = FlightGrpcUtils.createFlightService(FlightAllocator.serverAllocator,
      new FiloDBFlightProducer(memStore, FlightAllocator.serverAllocator, location, allConfig),
      noAuthHandler,
      executor)

    val server1 = ServerBuilder.forPort(port)
    val server2 = if (compressionEnabled) server1.intercept(GzipServerInterceptor) else server1
    val server3 = server2.asInstanceOf[ServerBuilder[NettyServerBuilder]].addService(svc).build()
    logger.info(s"Starting FiloDB Flight server on $host:$port with compression = $compressionEnabled")
    server3.start()
    server3
  }
}

object GzipServerInterceptor extends ServerInterceptor {
  override def interceptCall[ReqT, RespT](call: ServerCall[ReqT, RespT],
                                          headers: Metadata,
                                          next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    call.setCompression("gzip")
    next.startCall(call, headers)
  }
}

object GzipClientInterceptor extends ClientInterceptor {

  override def interceptCall[ReqT, RespT](method: MethodDescriptor[ReqT, RespT],
                                          callOptions: CallOptions,
                                          next: Channel): ClientCall[ReqT, RespT] = {
    next.newCall(method, callOptions.withCompression("gzip"))
  }
}
