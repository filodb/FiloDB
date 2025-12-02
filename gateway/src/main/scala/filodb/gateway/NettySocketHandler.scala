package filodb.gateway

// This file has been commented out - Netty TCP server functionality removed
// (JBoss Netty 3.x is not available for Scala 2.13)
//
// The TCP server functionality using JBoss Netty has been deprecated.
// Use Kafka-based ingestion or implement an alternative TCP server using Netty 4.x or other libraries.

// Commented out original implementation:
// import com.typesafe.scalalogging.StrictLogging
// import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
// import org.jboss.netty.channel.ChannelHandlerContext
// import org.jboss.netty.channel.ExceptionEvent
// import org.jboss.netty.channel.MessageEvent
// import org.jboss.netty.channel.SimpleChannelUpstreamHandler
// import filodb.memory.format.UnsafeUtils
//
// class NettySocketHandler(delimiter: Option[Char], handler: ChannelBuffer => Unit)
//   extends SimpleChannelUpstreamHandler with StrictLogging {
//   ... (implementation commented out)
// }
