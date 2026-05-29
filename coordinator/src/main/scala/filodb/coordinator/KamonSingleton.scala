package filodb.coordinator

import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon

/**
 * Singleton object to ensure Kamon is initialized only once per JVM.
 * This prevents memory leaks from multiple Kamon.init() calls in Spark workers.
 */
object KamonSingleton extends StrictLogging {

  private val initialized = new AtomicBoolean(false)

  /**
   * Initializes Kamon if it hasn't been initialized already.
   * This method is thread-safe and idempotent.
   */
  def initOnce(): Unit = {
    if (initialized.compareAndSet(false, true)) {
      logger.info("Initializing Kamon for the first time in this JVM")
      Kamon.init()
      logger.info("Kamon initialization completed")
    } else {
      logger.debug("Kamon already initialized in this JVM, skipping")
    }
  }

  /**
   * Checks if Kamon has been initialized in this JVM.
   */
  def isInitialized: Boolean = initialized.get()

  /**
   * For testing purposes only - resets the initialization state.
   * Should not be used in production code.
   */
  private[coordinator] def resetForTesting(): Unit = {
    initialized.set(false)
  }
}