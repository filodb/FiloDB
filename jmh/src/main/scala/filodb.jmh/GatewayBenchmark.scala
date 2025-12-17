package filodb.jmh

// This benchmark has been commented out - Netty TCP server functionality removed
// (JBoss Netty 3.x is not available for Scala 2.13)
//
// GatewayBenchmark depends on InfluxProtocolParser which uses JBoss Netty's ChannelBuffer.
// This functionality has been deprecated along with the TCP server in the gateway module.
// Use Kafka-based ingestion benchmarks instead.

// Commented out original implementation:
// import java.nio.charset.StandardCharsets
// import java.util.concurrent.TimeUnit
// import ch.qos.logback.classic.{Level, Logger}
// import com.typesafe.scalalogging.StrictLogging
// import org.jboss.netty.buffer.ChannelBuffers
// import org.openjdk.jmh.annotations._
// import remote.RemoteStorage.{LabelPair, Sample, TimeSeries}
// import filodb.core.binaryrecord2.RecordBuilder
// import filodb.gateway.conversion.{InfluxProtocolParser, PrometheusInputRecord}
// import filodb.memory.MemFactory
//
// @State(Scope.Thread)
// class GatewayBenchmark extends StrictLogging {
//   ... (implementation commented out)
// }
