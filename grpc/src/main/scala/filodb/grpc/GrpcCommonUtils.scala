package filodb.grpc

import java.util.concurrent.TimeUnit

import io.grpc.ManagedChannel
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}

import filodb.core.GlobalConfig

object GrpcCommonUtils {

  def buildChannelFromEndpoint(endpointUrl: String): ManagedChannel = {

    val grpcConfig = GlobalConfig.defaultFiloConfig.getConfig("grpc")
    val idleTimeout = grpcConfig.getInt("idle-timeout-seconds")
    val keepAliveTime = grpcConfig.getInt("keep-alive-time-seconds")
    val keepAliveTimeOut = grpcConfig.getInt("keep-alive-timeout-seconds")
    val lbPolicy = grpcConfig.getString("load-balancing-policy")
    val builder = NettyChannelBuilder
      .forTarget(endpointUrl)
      .defaultLoadBalancingPolicy(lbPolicy)
      // TODO: Configure this to SSL/Plain text later based on config, currently only Plaintext supported
      .negotiationType(NegotiationType.PLAINTEXT)

    if (idleTimeout > 0) {
      builder.idleTimeout(idleTimeout, TimeUnit.SECONDS)
    }
    if (keepAliveTime > 0) {
      builder.keepAliveTime(keepAliveTime, TimeUnit.SECONDS).keepAliveWithoutCalls(true)
    }
    if (keepAliveTimeOut > 0) {
      builder.keepAliveTimeout(keepAliveTimeOut, TimeUnit.SECONDS)
    }
    // TODO: Add metrics for tracking the actual number of connections opened by this ManagedChannel
    builder.build()
  }
}
