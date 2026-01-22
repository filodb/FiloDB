package filodb.core.query

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.typesafe.scalalogging.StrictLogging
import monix.execution.atomic.AtomicBoolean
import org.apache.arrow.memory.{AllocationListener, BufferAllocator, RootAllocator}

import filodb.core.GlobalConfig
import filodb.core.metrics.FilodbMetrics
import filodb.memory.data.Shutdown

object FlightAllocator {

  private val rootAllocatorMaxSize = GlobalConfig.systemConfig.getBytes("filodb.flight.root-allocator-max-memory")
  lazy private val rootAllocator = new RootAllocator(rootAllocatorMaxSize)
  private val flightServerMaxAlloc = GlobalConfig.systemConfig.getBytes("filodb.flight.server.allocator-limit")
  private val flightClientMaxAlloc = GlobalConfig.systemConfig.getBytes("filodb.flight.client.allocator-limit")

  /**
   * We need metrics to track both total allocated memory (counter) and currently used memory (up-down counter which
   * gets aggregated as a gauge eventually.
   */
  private val serverAllocated = FilodbMetrics.bytesCounter("flight-allocated-memory", Map("allocator" -> "server"))
  private val clientAllocated = FilodbMetrics.bytesCounter("flight-allocated-memory", Map("allocator" -> "client"))
  private val serverUsedMem = FilodbMetrics.bytesUpDownCounter("flight-used-memory", Map("allocator" -> "server"))
  private val clientUsedMem = FilodbMetrics.bytesUpDownCounter("flight-used-memory", Map("allocator" -> "client"))

  private val serverAllocationListener = new AllocationListener {
    override def onAllocation(size: Long): Unit = {
      serverAllocated.increment(size)
      serverUsedMem.increment(size)
    }

    override def onRelease(size: Long): Unit = {
      serverUsedMem.increment(-size)
    }
  }
  private val clientAllocationListener = new AllocationListener {
    override def onAllocation(size: Long): Unit = {
      clientAllocated.increment(size)
      clientUsedMem.increment(size)
    }
    override def onRelease(size: Long): Unit = {
      clientUsedMem.increment(-size)
    }
  }

  lazy val serverAllocator: BufferAllocator = rootAllocator.newChildAllocator("FilodbFlightServer",
                                                                  serverAllocationListener, 0, flightServerMaxAlloc)
  lazy val clientAllocator: BufferAllocator = rootAllocator.newChildAllocator("FilodbFlightClient",
                                                                  clientAllocationListener, 0, flightClientMaxAlloc)

  /**
   * Use only for unit testing
   */
  def newChildAllocatorForTesting(name: String, initialReservation: Long = 0,
                                                  maxAllocation: Long): BufferAllocator = {
    rootAllocator.newChildAllocator(name, initialReservation, maxAllocation)
  }

}

/**
 * Wrapper around a BufferAllocator used for Flight requests that provides
 * safe access to the allocator with read-write locks to prevent use-after-close, and to prevent
 * races between use and close This especially can occur when we scatter gather queries to multiple
 * nodes in parallel, and accumulate results. Monix mapParallelUnordered fails eagerly when one of the tasks
 * error, which can lead to one task closing the allocator while others are still using it.
 * @param allocator
 */
class FlightAllocator(private val allocator: BufferAllocator) extends AutoCloseable with StrictLogging {

  private val closed = AtomicBoolean(false)
  private val closeables = new ConcurrentLinkedQueue[AutoCloseable]()
  private val rwLock = new ReentrantReadWriteLock()

  def allocatedBytes: Long = allocator.getAllocatedMemory
  def allocationLimit: Long = allocator.getLimit

  /**
   * Protected access to the allocator for making allocations that need to live beyond
   * the scope of the lambda. The lambda is executed with a read lock held, preventing
   * the allocator from being closed while in use.
   * @param f the function to execute with the allocator
   * @param ifClosed the lambda to execute if the allocator is already closed
   * @tparam T the return type of the lambda
   * @return the result of the lambda
   */
  def withRequestAllocator[T](f: BufferAllocator => T)(ifClosed: => T): T = {
    if (closed.get()) ifClosed
    else {
      rwLock.readLock().lock()
      try {
        f(allocator)
      } finally {
        rwLock.readLock().unlock()
      }
    }
  }

  /**
   * Register a closeable resource that will be closed when this FlightAllocator is closed.
   * This should be called only within the scope of `withRequestAllocator` to ensure thread safety.
   */
  def registerCloseable(closeable: AutoCloseable): Unit = {
    require(rwLock.getReadHoldCount > 0, "registerCloseable must be called with read lock held")
    closeables.add(closeable)
  }

  def close(): Unit = {
    if (closed.get()) return
    rwLock.writeLock().lock()
    try {
      closeables.forEach(_.close())
      closeables.clear()
      if (allocator.getAllocatedMemory > 0) {
        logger.error(s"FlightAllocator close attempt with ${allocator.getAllocatedMemory} bytes still allocated")
        logger.error(s"Allocator Verbose Trace: ${allocator.toVerboseString}")
        Shutdown.haltAndCatchFire(new IllegalStateException("Arrow BufferAllocator memory leak detected"))
      }
      allocator.close()
      closed.set(true)
    } finally {
      rwLock.writeLock().unlock()
    }
  }
}