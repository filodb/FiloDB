package filodb.gateway.conversion

// This file has been commented out - Netty TCP server functionality removed
// (JBoss Netty 3.x is not available for Scala 2.13)
//
// InfluxRecord depends on InfluxProtocolParser which has been deprecated.
// The InfluxDB line protocol parsing functionality has been removed along with the TCP server.
// Use Kafka-based ingestion or implement an alternative parser.

// Commented out original implementation:
// import java.nio.charset.StandardCharsets
// import filodb.core.binaryrecord2.RecordBuilder
// import filodb.core.metadata.{Dataset, Schemas}
// import filodb.gateway.conversion.InputRecord
// import filodb.memory.format.{vectors => bv, ZeroCopyUTF8String => ZCUTF8}
//
// trait InfluxRecord extends InputRecord {
//   ... (implementation commented out)
// }
//
// class PrometheusGaugeRecord(...) extends InfluxRecord { ... }
// class PrometheusCounterRecord(...) extends InfluxRecord { ... }
// class PrometheusHistogramRecord(...) extends InfluxRecord { ... }
// class DeltaCounterRecord(...) extends InfluxRecord { ... }
// class DeltaHistogramRecord(...) extends InfluxRecord { ... }
// ... (other classes commented out)
