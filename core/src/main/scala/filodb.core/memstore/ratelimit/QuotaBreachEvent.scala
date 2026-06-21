package filodb.core.memstore.ratelimit

import filodb.core.DatasetRef

/**
 * Wire payload published whenever a `QuotaExceededProtocol` decides to surface a quota breach to
 * an external consumer (e.g. mosaic-gateway). Construction strips the workspace and namespace out
 * of the prefix so consumers don't have to re-parse it; the full prefix is preserved for cases
 * where breaches happen at the metric level (`shardKeyPrefix.length == 3`).
 *
 * Schema is versioned so consumers can evolve independently. Bump `schemaVersion` on any breaking
 * change.
 *
 * @param dataset           dataset on which the breach happened
 * @param shardKeyPrefix    full prefix the tracker fired on (length 1, 2 or 3 in the standard
 *                          `_ws_/_ns_/_metric_` schema)
 * @param workspace         convenience extraction from `shardKeyPrefix(0)`, empty if absent
 * @param namespace         convenience extraction from `shardKeyPrefix(1)`, empty if absent
 * @param shardNum          shard that produced the breach
 * @param quota             configured quota that was breached
 * @param clusterType       FiloDB cluster type (e.g. "raw", "preagg", "downsample")
 * @param partition         FiloDB partition name (passthrough of `filodb.partition`)
 * @param breachedAtMillis  publisher-side timestamp in epoch millis
 */
final case class QuotaBreachEvent(
    dataset: String,
    shardKeyPrefix: Seq[String],
    workspace: String,
    namespace: String,
    shardNum: Int,
    quota: Long,
    clusterType: String,
    partition: String,
    breachedAtMillis: Long) {

  /** Dedup key matches the granularity of the consumer-side throttle: (dataset, ws, ns). */
  def dedupKey: (String, String, String) = (dataset, workspace, namespace)

  /**
   * Hand-rolled JSON serialization to avoid pulling a JSON library into core. Payload is small
   * and well-bounded so the duplication cost is low; revisit if the schema ever grows.
   */
  def toJson: String = {
    val sb = new java.lang.StringBuilder(256)
    sb.append('{')
    appendField(sb, "schemaVersion", QuotaBreachEvent.SchemaVersion); sb.append(',')
    appendField(sb, "eventType", QuotaBreachEvent.EventType); sb.append(',')
    appendField(sb, "dataset", dataset); sb.append(',')
    appendStringArrayField(sb, "shardKeyPrefix", shardKeyPrefix); sb.append(',')
    appendField(sb, "workspace", workspace); sb.append(',')
    appendField(sb, "namespace", namespace); sb.append(',')
    appendField(sb, "shardNum", shardNum); sb.append(',')
    appendField(sb, "quota", quota); sb.append(',')
    appendField(sb, "clusterType", clusterType); sb.append(',')
    appendField(sb, "partition", partition); sb.append(',')
    appendField(sb, "breachedAtMillis", breachedAtMillis)
    sb.append('}')
    sb.toString
  }

  private def appendField(sb: java.lang.StringBuilder, name: String, value: String): Unit = {
    sb.append('"').append(name).append("\":")
    QuotaBreachEvent.appendJsonString(sb, value)
  }
  private def appendField(sb: java.lang.StringBuilder, name: String, value: Long): Unit = {
    sb.append('"').append(name).append("\":").append(value)
  }
  private def appendField(sb: java.lang.StringBuilder, name: String, value: Int): Unit = {
    sb.append('"').append(name).append("\":").append(value)
  }
  private def appendStringArrayField(
      sb: java.lang.StringBuilder, name: String, values: Seq[String]): Unit = {
    sb.append('"').append(name).append("\":[")
    var first = true
    values.foreach { v =>
      if (!first) sb.append(',')
      QuotaBreachEvent.appendJsonString(sb, v)
      first = false
    }
    sb.append(']')
  }
}

object QuotaBreachEvent {

  val SchemaVersion: Int = 1
  val EventType: String = "QUOTA_BREACH"

  def apply(
      ref: DatasetRef,
      shardKeyPrefix: Seq[String],
      shardNum: Int,
      quota: Long,
      clusterType: String,
      partition: String,
      breachedAtMillis: Long): QuotaBreachEvent = {
    val ws = if (shardKeyPrefix.nonEmpty) shardKeyPrefix.head else ""
    val ns = if (shardKeyPrefix.length >= 2) shardKeyPrefix(1) else ""
    QuotaBreachEvent(
      dataset = ref.toString,
      shardKeyPrefix = shardKeyPrefix.toIndexedSeq,
      workspace = ws,
      namespace = ns,
      shardNum = shardNum,
      quota = quota,
      clusterType = clusterType,
      partition = partition,
      breachedAtMillis = breachedAtMillis)
  }

  /**
   * Escapes a Scala string into a JSON string literal (including surrounding quotes). Handles the
   * full set of escapes mandated by RFC 8259 so we don't produce malformed JSON when ws/ns names
   * happen to contain control characters or quotes.
   */
  private[ratelimit] def appendJsonString(sb: java.lang.StringBuilder, s: String): Unit = {
    sb.append('"')
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      c match {
        case '"'  => sb.append("\\\"")
        case '\\' => sb.append("\\\\")
        case '\b' => sb.append("\\b")
        case '\f' => sb.append("\\f")
        case '\n' => sb.append("\\n")
        case '\r' => sb.append("\\r")
        case '\t' => sb.append("\\t")
        case ch if ch < 0x20 =>
          sb.append("\\u")
          val hex = Integer.toHexString(ch.toInt)
          var pad = 4 - hex.length
          while (pad > 0) { sb.append('0'); pad -= 1 }
          sb.append(hex)
        case ch => sb.append(ch)
      }
      i += 1
    }
    sb.append('"')
  }
}
