package filodb.core.memstore

import java.security.{KeyManagementException, NoSuchAlgorithmException}
import javax.net.ssl.SSLParameters

import com.apple.its.redis.cluster.{RedisClusterClient, RedisClusterClientConfig}
import com.apple.its.redis.core.config.retry.RetryConfig
import com.apple.its.redis.core.utils.SslUtils
import com.typesafe.scalalogging.StrictLogging
// JedisPoolConfig arrives transitively via com.apple.its:redis-cluster; a future RCL upgrade
// that relocates/shades Jedis would require revisiting this import.
import redis.clients.jedis.JedisPoolConfig

import filodb.core.store.AciRedisConfig

/**
 * JVM-scoped singleton managed-Valkey (aci-redis) cluster client. One
 * `RedisClusterClient` per JVM, shared across all shards, torn down only by the
 * JVM shutdown hook (shard shutdown is not a signal to close the shared client).
 * Assumes a single aci-redis config per JVM (FiloDB raw has one dataset).
 */
object RedisSnapshotClient extends StrictLogging {
  @volatile private var client: RedisClusterClient = _
  private val lock = new Object

  /** Lazily build and cache the shared cluster client; register a JVM shutdown hook once. */
  def acquire(cfg: AciRedisConfig): RedisClusterClient = {
    if (client == null) lock.synchronized {
      if (client == null) {
        val built = build(cfg)
        Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
          override def run(): Unit = try built.shutdown() catch { case _: Throwable => () }
        }, "filodb-aci-redis-shutdown"))
        client = built
        logger.info(s"RedisSnapshotClient built RedisClusterClient discovery=${cfg.discoveryServiceEndpoint} " +
          s"cluster=${cfg.clusterName} dc=${cfg.clusterDc} workspace=${cfg.workspaceName} mtls=${cfg.mtlsEnabled}")
      }
    }
    client
  }

  private def build(cfg: AciRedisConfig): RedisClusterClient = {
    val config = new RedisClusterClientConfig()
    config.setHealthMonitorPingIntervalMs(cfg.healthCheckIntervalMs)
    config.setJedisInstancePassword(cfg.password)
    config.setJedisInstancePoolConfig(new JedisPoolConfig())
    config.setDiscoveryServiceWorkspaceName(cfg.workspaceName)
    config.setDiscoveryServiceClientId("filodb-raw")
    config.setDiscoveryServiceConnectionTimeoutMs(cfg.connectionTimeoutMs)
    config.setDiscoveryServiceClusterDc(cfg.clusterDc)
    config.setDiscoveryServiceClusterName(cfg.clusterName)

    if (cfg.mtlsEnabled) {
      val ctx =
        try SslUtils.buildSSLContext(cfg.keystorePath, cfg.keystoreType, cfg.keystorePassword,
                                     cfg.truststorePath, cfg.truststoreType, cfg.truststorePassword)
        catch {
          case e @ (_: NoSuchAlgorithmException | _: KeyManagementException) =>
            throw new RuntimeException("Failed to build SSLContext for aci-redis", e)
        }
      config.setJedisSSL(ctx.getSocketFactory, new SSLParameters())
    }

    val retry = new RetryConfig()
    retry.setMaxRetries(2)

    new RedisClusterClient.Builder()
      .withDiscoveryService(cfg.discoveryServiceEndpoint)
      .withRetryConfig(retry)
      .withConfig(config)
      .build()
  }
}
