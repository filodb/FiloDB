package filodb.coordinator.flight

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.FiniteDuration

import com.typesafe.scalalogging.StrictLogging
import monix.execution.{CancelableFuture, UncaughtExceptionReporter}
import monix.reactive.Observable
import org.apache.arrow.flight.{CallOptions, FlightClient, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.slf4j.LoggerFactory

import filodb.core.GlobalConfig

/**
 * Manages Arrow FlightClient instances for interacting with various peer nodes.
 * This manager provides connection pooling, automatic reconnection, keep-alive management,
 * and graceful shutdown functionality.
 *
 * The manager follows the same patterns established by GrpcPlanDispatcher for consistency
 * with the existing FiloDB codebase architecture.
 *
 * Thread-Safety:
 * - All public methods are thread-safe and can be called concurrently
 * - Multiple threads can safely fetch, create, or recreate clients for a location
 * - Ensures only one client is created per location, even under concurrent access
 * - Per-location synchronization prevents duplicate reconnections
 *
 * Features:
 * - Connection pooling with automatic client creation per Location
 * - Automatic reconnection on connection failures
 * - Background health checking and connection validation
 * - Graceful shutdown with proper resource cleanup
 * - Memory management with per-client Buffer allocators
 *
 * TODO:
 * - Removal of old clients after a certain period of inactivity. Depending on usage patterns,
 *  this may be necessary to prevent unbounded growth of the client pool.
 *
 * @param rootAllocator The root memory allocator for creating client allocators
 */
class FlightClientManager(rootAllocator: RootAllocator) {

  import FlightClientManager._

  private val logger = LoggerFactory.getLogger(classOf[FlightClientManager])

  // Load configuration
  private val flightClientConfig = GlobalConfig.systemConfig.getConfig("filodb.flight.client")
  private val maxMemoryPerClient = flightClientConfig.getBytes("per-client-allocator-limit")
  private val maxInboundMessageSize = flightClientConfig.getBytes("max-inbound-message-size").toInt
  private val healthCheckInterval = FiniteDuration(flightClientConfig.getDuration("health-check-interval").toMillis,
                                      TimeUnit.MILLISECONDS)
  private val healthCheckTimeoutMs = flightClientConfig.getDuration("health-check-timeout").toMillis

  // Client storage and metadata
  private val clientMap = new ConcurrentHashMap[String, FlightClientEntry]()
  private val isShutdown = new AtomicBoolean(false)

  // Background scheduler for health checks and reconnections
  private val healthCheckScheduler =
    monix.execution.Scheduler.singleThread("flight-client-health-checker",
     reporter = UncaughtExceptionReporter(logger.error("Uncaught Exception in Flight Client Healthcheck Scheduler", _)))
  private val healthCheckTask: CancelableFuture[Unit] = startHealthChecking()

  /**
   * Gets the health status of the client for the given location.
   * Returns None if no client exists for the location.
   * Returns Some(None) if health is unknown, Some(Some(true)) if healthy, Some(Some(false)) if unhealthy.
   */
  def getClientHealth(location: Location): Option[Option[Boolean]] = {
    if (isShutdown.get()) {
      throw new IllegalStateException("FlightClientManager has been shut down")
    }
    val locationKey = locationToKey(location)
    Option(clientMap.get(locationKey)).map(_.isHealthy)
  }

  /**
   * Gets or creates a FlightClient for the given location.
   * Note it does not connect to the location until the first request is made.
   * Thread-safe: multiple threads can safely call this method concurrently.
   *
   * Clients should NOT close the returned FlightClient. The manager handles client lifecycle.
   *
   * @param location The Arrow Flight Location to connect to
   * @param forceRebuild If true, it will rebuild connection for the location
   */
  def getClient(location: Location, forceRebuild: Boolean = false): FlightClient = {
    if (isShutdown.get()) {
      throw new IllegalStateException("FlightClientManager has been shut down")
    }
    val locationKey = locationToKey(location)

    // Use getOrElseUpdate for atomic creation - only one thread will create the entry
    val entry = clientMap.computeIfAbsent(locationKey, l => {
      logger.info(s"Creating new FlightClient for $location")
      createNewClientEntry(location, locationKey)
    })

    // Synchronize on entry to prevent concurrent reconnections for the same location
    entry.synchronized {
      if (forceRebuild || (entry.isHealthy.isDefined && !entry.isHealthy.get)) { // not healthy
        logger.info(s"Reconnecting unhealthy FlightClient for $location")
        reconnectClientUnsafe(entry, location)
      } else {
        entry.client
      }
    }
  }

  /**
   * Removes a client from the pool (e.g., when permanently shutting down a peer).
   */
  def removeClient(location: Location): Unit = {
    if (isShutdown.get()) {
      throw new IllegalStateException("FlightClientManager has been shut down")
    }
    val locationKey = locationToKey(location)
    val entry = clientMap.remove(locationKey)
    if (entry != null) {
      logger.info(s"Removing FlightClient for $location")
      closeClientEntry(entry)
    }
  }

  /**
   * Gracefully shuts down all clients and releases resources.
   */
  def shutdown(): Unit = {
    if (isShutdown.compareAndSet(false, true)) {
      logger.info("Shutting down FlightClientManager")

      // Stop health checking
      healthCheckTask.cancel()
      healthCheckScheduler.shutdown()

      // Close all clients
      clientMap.values.forEach(closeClientEntry)
      clientMap.clear()
      logger.info("FlightClientManager shutdown complete")
    }
  }

  // Private helper methods

  private def locationToKey(location: Location): String = {
    s"${location.getUri.getHost}:${location.getUri.getPort}"
  }

  def getClientAllocatorAllocatedBytes(location: Location): Option[Long] = {
    if (isShutdown.get()) {
      throw new IllegalStateException("FlightClientManager has been shut down")
    }
    val locationKey = locationToKey(location)
    Option(clientMap.get(locationKey)).map(_.allocator.getAllocatedMemory)
  }

  private def createNewClientEntry(location: Location, locationKey: String): FlightClientEntry = {
    val allocator = rootAllocator.newChildAllocator(s"flight-client-$locationKey", 0, maxMemoryPerClient)

    try {
      val client = FlightClient.builder()
        .allocator(allocator)
        .location(location)
        .maxInboundMessageSize(maxInboundMessageSize)
        .build()

      val entry = FlightClientEntry(
        client = client,
        allocator = allocator,
        location = location,
        isHealthy = None,
        createdAt = System.currentTimeMillis(),
        lastHealthCheck = System.currentTimeMillis()
      )

      logger.info(s"Successfully created FlightClient for $location")
      entry

    } catch {
      case ex: Exception =>
        allocator.close()
        logger.error(s"Failed to create FlightClient for $location", ex)
        throw ex
    }
  }

  /**
   * Reconnects a client by closing the old one and creating a new one.
   * IMPORTANT: This method is not thread-safe and should only be called when synchronized on the entry.
   *
   * @param entry The FlightClientEntry to reconnect
   * @param location The location to connect to
   * @return The new FlightClient
   */
  private def reconnectClientUnsafe(entry: FlightClientEntry, location: Location): FlightClient = {
    // Close the old client
    closeClient(entry.client)

    // Create new client with existing allocator
    try {
      val newClient = FlightClient.builder()
        .allocator(entry.allocator)
        .location(location)
        .maxInboundMessageSize(maxInboundMessageSize)
        .build()

      // Update entry with new client
      entry.client = newClient
      entry.isHealthy = None
      entry.lastHealthCheck = System.currentTimeMillis()

      logger.info(s"Successfully reconnected FlightClient for $location")
      newClient

    } catch {
      case ex: Exception =>
        entry.isHealthy = Some(false)
        logger.error(s"Failed to reconnect FlightClient for $location", ex)
        throw ex
    }
  }

  private def closeClientEntry(entry: FlightClientEntry): Unit = {
    try {
      entry.allocator.close()
    } finally {
      closeClient(entry.client)
    }
  }

  private def closeClient(client: FlightClient): Unit = {
    try {
      client.close()
    } catch {
      case ex: Exception =>
        logger.warn("Error closing FlightClient", ex)
    }
  }

  private def startHealthChecking(): CancelableFuture[Unit] = {
    logger.info(s"Started FlightClient health checking with interval ${healthCheckInterval}ms")
    Observable.intervalWithFixedDelay(healthCheckInterval)
      .foreach( _ => performHealthChecks())(healthCheckScheduler)
  }

  private def performHealthChecks(): Unit = {
    if (isShutdown.get()) return

    try {
      clientMap.values.forEach { entry =>
        if (!isShutdown.get()) {
          checkClientHealth(entry)
        }
      }
    } catch {
      case ex: Exception =>
        logger.error("Error during health check cycle", ex)
    }
  }

  private def checkClientHealth(entry: FlightClientEntry): Unit = {
    try {
      // Simple health check: attempt to list actions (lightweight operation)
      val hn = entry.client.listActions(CallOptions.timeout(healthCheckTimeoutMs, TimeUnit.MILLISECONDS))
                                .iterator().hasNext

      // Synchronize when updating entry fields
      entry.synchronized {
        entry.lastHealthCheck = System.currentTimeMillis()
        if (!entry.isHealthy.getOrElse(false)) {
          logger.info(s"FlightClient for ${entry.location} is now healthy")
        }
        entry.isHealthy = Some(true)
      }
    } catch {
      case ex: Exception =>
        logger.warn(s"FlightClient for ${entry.location} failed health check", ex)
        // Synchronize when updating entry fields
        entry.synchronized {
          entry.lastHealthCheck = System.currentTimeMillis()
          entry.isHealthy = Some(false)
        }
    }
  }
}

object FlightClientManager extends StrictLogging {

  /**
   * Represents a FlightClient entry in the connection pool.
   */
  private[coordinator] case class FlightClientEntry(
    var client: FlightClient,
    allocator: BufferAllocator,
    location: Location,
    var isHealthy: Option[Boolean],
    createdAt: Long,
    var lastHealthCheck: Long
  )

  // Global FlightClientManager instance that follows the same pattern as GrpcPlanDispatcher
  private val globalManagerInstance = new FlightClientManager(FlightAllocator.rootAllocator)

  // Shutdown hook for graceful cleanup
  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    try {
      globalManagerInstance.shutdown()
    } catch {
      case ex: Exception =>
        logger.error(s"Error during FlightClientManager shutdown: ${ex.getMessage}")
    }
  }))

  /**
   * Gets the global FlightClientManager instance.
   */
  def global: FlightClientManager = globalManagerInstance

  /**
   * Convenience method to get a client from the global manager.
   */
  def getClient(location: Location): FlightClient = global.getClient(location)
}