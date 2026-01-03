package filodb.coordinator.flight

import org.apache.arrow.memory.{AllocationListener, BufferAllocator, RootAllocator}

import filodb.core.GlobalConfig
import filodb.core.metrics.FilodbMetrics

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
  private[flight] def newChildAllocatorForTesting(name: String, initialReservation: Long = 0,
                                                  maxAllocation: Long): BufferAllocator = {
    rootAllocator.newChildAllocator(name, initialReservation, maxAllocation)
  }

}
