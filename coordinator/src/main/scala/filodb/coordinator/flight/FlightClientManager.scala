package filodb.coordinator.flight

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.concurrent.TrieMap
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
 * Features:
 * - Connection pooling with automatic client creation per Location
 * - Configurable keep-alive and timeout settings
 * - Automatic reconnection on connection failures
 * - Background health checking and connection validation
 * - Graceful shutdown with proper resource cleanup
 * - Memory management with per-client Buffer allocators
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
  private val clientMap = new TrieMap[String, FlightClientEntry]()
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
    val locationKey = locationToKey(location)
    clientMap.get(locationKey).map(_.isHealthy)
  }

  /**
   * Gets or creates a FlightClient for the given location.
   * Note it does not connect to the location until the first request is made.
   *
   * @param location The Arrow Flight Location to connect to
   */
  def getClient(location: Location): FlightClient = {
    if (isShutdown.get()) {
      throw new IllegalStateException("FlightClientManager has been shut down")
    } else {
      val locationKey = locationToKey(location)
      clientMap.get(locationKey) match {
        case Some(entry) =>
          if (entry.isHealthy.isDefined && !entry.isHealthy.get) { // not healthy
            logger.info(s"Reconnecting unhealthy FlightClient for $location")
            reconnectClient(entry, location)
          } else {
            logger.debug(s"Reusing existing healthy FlightClient for $location")
            entry.client
          }
        case None =>
          logger.info(s"Creating new FlightClient for $location")
          createNewClient(location, locationKey)
      }
    }
  }

  /**
   * Forces reconnection for a client at the given location.
   * Useful when external code detects connection issues.
   */
  def forceRebuild(location: Location): FlightClient = {
    val locationKey = locationToKey(location)
    clientMap.get(locationKey) match {
      case Some(entry) =>
        logger.info(s"Force reconnecting FlightClient for $location")
        reconnectClient(entry, location)
      case None =>
        logger.info(s"Creating new FlightClient for forced reconnect to $location")
        createNewClient(location, locationKey)
    }
  }

  /**
   * Removes a client from the pool (e.g., when permanently shutting down a peer).
   */
  def removeClient(location: Location): Unit = {
    val locationKey = locationToKey(location)
    clientMap.remove(locationKey) match {
      case Some(entry) =>
        logger.info(s"Removing FlightClient for $location")
        closeClientEntry(entry)
      case None =>
        logger.debug(s"No FlightClient found to remove for $location")
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
      clientMap.values.foreach(closeClientEntry)
      clientMap.clear()
      logger.info("FlightClientManager shutdown complete")
    }
  }

  // Private helper methods

  private def locationToKey(location: Location): String = {
    s"${location.getUri.getHost}:${location.getUri.getPort}"
  }

  def getClientAllocatorAllocatedBytes(location: Location): Long = {
    val locationKey = locationToKey(location)
    clientMap(locationKey).allocator.getAllocatedMemory
  }

  private def createNewClient(location: Location, locationKey: String): FlightClient = {
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

      clientMap.put(locationKey, entry)
      logger.info(s"Successfully created FlightClient for $location")
      client

    } catch {
      case ex: Exception =>
        allocator.close()
        logger.error(s"Failed to create FlightClient for $location", ex)
        throw ex
    }
  }

  private def reconnectClient(entry: FlightClientEntry, location: Location): FlightClient = {
    // Close the old client
    closeClient(entry.client)

    // FIXME this is not threadsafe yet - two threads could try to reconnect simultaneously

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
      closeClient(entry.client)
    } finally {
      entry.allocator.close()
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
      clientMap.values.foreach { entry =>
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
      entry.lastHealthCheck = System.currentTimeMillis()
      // Simple health check: attempt to list actions (lightweight operation)
      val hn = entry.client.listActions(CallOptions.timeout(healthCheckTimeoutMs, TimeUnit.MILLISECONDS))
                                .iterator().hasNext
      if (entry.isHealthy.getOrElse(false)) {
        logger.info(s"FlightClient for ${entry.location} is now healthy")
      }
      entry.isHealthy = Some(true)
    } catch {
      case ex: Exception =>
        logger.warn(s"FlightClient for ${entry.location} failed health check", ex)
        entry.isHealthy = Some(false)
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