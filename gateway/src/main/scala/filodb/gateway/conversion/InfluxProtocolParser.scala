package filodb.gateway.conversion

// This file has been commented out - Netty TCP server functionality removed
// (JBoss Netty 3.x is not available for Scala 2.13)
//
// The InfluxDB protocol parser used JBoss Netty's ChannelBuffer.
// This functionality has been deprecated along with the TCP server.
// Use Kafka-based ingestion or implement an alternative using Netty 4.x or other libraries.

// Commented out original implementation:
// import org.jboss.netty.buffer.ChannelBuffer
// import filodb.core.binaryrecord2.RecordBuilder
// import filodb.memory.format.{vectors => bv, ZeroCopyUTF8String => ZCUTF8}
//
// object InfluxProtocolParser {
//   def parse(buffer: ChannelBuffer): Option[InfluxRecord] = {
//     ... (implementation commented out)
//   }
//   ... (other methods commented out)
// }
