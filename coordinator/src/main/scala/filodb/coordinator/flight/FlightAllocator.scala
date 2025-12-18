package filodb.coordinator.flight

import org.apache.arrow.memory.RootAllocator

import filodb.core.GlobalConfig

object FlightAllocator {
  val rootAllocator = new RootAllocator(GlobalConfig.systemConfig.getBytes("filodb.flight.root-allocator-max-memory"))
}
