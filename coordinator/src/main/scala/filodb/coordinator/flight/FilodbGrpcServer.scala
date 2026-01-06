package filodb.coordinator.flight

import java.net.InetAddress
import java.util
import java.util.Optional
import java.util.concurrent.Executors

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import io.grpc._
import io.grpc.netty.NettyServerBuilder
import org.apache.arrow.flight.{FlightGrpcUtils, Location}
import org.apache.arrow.flight.auth.ServerAuthHandler

import filodb.coordinator.flight.FiloDBFlightProducer.akkaPortToFlightPort
import filodb.core.memstore.TimeSeriesStore

object FilodbGrpcServer extends StrictLogging {

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

