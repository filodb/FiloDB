package filodb.core.store

import com.typesafe.config.Config

/**
 * Connection + auth settings for publishing cardinality snapshots to a managed
 * Redis/Valkey cluster via the RCL client. Parsed from the ingestion
 * source's `store.active-series-redis` block. `connectionTimeoutMs` is an Int because
 * RedisClusterClientConfig.setDiscoveryServiceConnectionTimeoutMs takes an int.
 */
final case class AciRedisConfig(enabled: Boolean,
                                snapshotIntervalSeconds: Int,
                                discoveryServiceEndpoint: String,
                                workspaceName: String,
                                clusterName: String,
                                clusterDc: String,
                                password: String,
                                healthCheckIntervalMs: Long,
                                connectionTimeoutMs: Int,
                                mtlsEnabled: Boolean,
                                keystorePath: String,
                                keystorePassword: String,
                                keystoreType: String,
                                truststorePath: String,
                                truststorePassword: String,
                                truststoreType: String)

object AciRedisConfig {

  /** Parse from the `active-series-redis` sub-config (all keys have defaults in StoreConfig.defaults). */
  def fromConfig(c: Config): AciRedisConfig = AciRedisConfig(
    enabled = c.getBoolean("enabled"),
    snapshotIntervalSeconds = c.getInt("snapshot-interval-seconds"),
    discoveryServiceEndpoint = c.getString("discovery-service-endpoint"),
    workspaceName = c.getString("workspace-name"),
    clusterName = c.getString("cluster-name"),
    clusterDc = c.getString("cluster-dc"),
    password = c.getString("password"),
    healthCheckIntervalMs = c.getLong("health-check-interval-in-ms"),
    connectionTimeoutMs = c.getInt("connection-timeout-ms"),
    mtlsEnabled = c.getBoolean("mtls.enabled"),
    keystorePath = c.getString("mtls.keystore.path"),
    keystorePassword = c.getString("mtls.keystore.password"),
    keystoreType = c.getString("mtls.keystore.type"),
    truststorePath = c.getString("mtls.truststore.path"),
    truststorePassword = c.getString("mtls.truststore.password"),
    truststoreType = c.getString("mtls.truststore.type"))

  /** Render back to a dotted-key map for StoreConfig.toConfig round-trip. */
  def toConfigMap(prefix: String, cfg: AciRedisConfig): Map[String, Any] = Map(
    s"$prefix.enabled" -> cfg.enabled,
    s"$prefix.snapshot-interval-seconds" -> cfg.snapshotIntervalSeconds,
    s"$prefix.discovery-service-endpoint" -> cfg.discoveryServiceEndpoint,
    s"$prefix.workspace-name" -> cfg.workspaceName,
    s"$prefix.cluster-name" -> cfg.clusterName,
    s"$prefix.cluster-dc" -> cfg.clusterDc,
    s"$prefix.password" -> cfg.password,
    s"$prefix.health-check-interval-in-ms" -> cfg.healthCheckIntervalMs,
    s"$prefix.connection-timeout-ms" -> cfg.connectionTimeoutMs,
    s"$prefix.mtls.enabled" -> cfg.mtlsEnabled,
    s"$prefix.mtls.keystore.path" -> cfg.keystorePath,
    s"$prefix.mtls.keystore.password" -> cfg.keystorePassword,
    s"$prefix.mtls.keystore.type" -> cfg.keystoreType,
    s"$prefix.mtls.truststore.path" -> cfg.truststorePath,
    s"$prefix.mtls.truststore.password" -> cfg.truststorePassword,
    s"$prefix.mtls.truststore.type" -> cfg.truststoreType)
}
