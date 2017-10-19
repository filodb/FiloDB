package filodb.memory.impl

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import com.kenai.jffi.MemoryIO
import filodb.memory.DynamicMemoryManager

import scala.collection.JavaConverters._

/**
  * Uses MemoryIO to do Dynamic memory allocation
  */
class NativeMemoryManager(val upperBoundSizeInBytes: Long) extends DynamicMemoryManager with CleanShutdown {

  protected val usedSoFar = new AtomicLong(0)
  protected val sizeMapping = new ConcurrentHashMap[Long, Long]()

  def usedMemory: Long = usedSoFar.get()

  def availableDynMemory: Long = upperBoundSizeInBytes - usedSoFar.get()


  override def allocateMemory(size: Long): Long = {
    val currentSize = usedSoFar.get()
    val resultantSize = currentSize + size
    if (!(resultantSize > upperBoundSizeInBytes)) {
      val address: Long = MemoryIO.getCheckedInstance().allocateMemory(size, true)
      usedSoFar.compareAndSet(currentSize, currentSize + size)
      sizeMapping.put(address, size)
      address
    } else {
      throw new IndexOutOfBoundsException(
        "Resultant memory size %s after allocating with requested size %s is greater than upper bound size %s"
          .format(resultantSize, size, upperBoundSizeInBytes)
      )
    }
  }


  override def freeMemory(address: Long): Unit = {
    val size = sizeMapping.get(address)
    if (size > 0) {
      val currentSize = usedSoFar.get()
      MemoryIO.getCheckedInstance().freeMemory(address)
      usedSoFar.compareAndSet(currentSize, currentSize - size)
      val removed = sizeMapping.remove(address)
    } else {
      throw new IllegalArgumentException(
        String.format("Address %s was not allocated by this memory manager", address + ""))
    }
  }

  override protected[memory] def freeAll(): Unit = {
    sizeMapping.entrySet().asScala.foreach { entry =>
      freeMemory(entry.getKey)
    }
    sizeMapping.clear()
  }

  override def shutdown(): Unit = {
    freeAll()
  }

}
