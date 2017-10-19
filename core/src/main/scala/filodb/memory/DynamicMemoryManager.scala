package filodb.memory

/**
  * A manager for dynamically allocated memory.
  * Such as for use in a BinaryVector. Said memory can be used for
  * off heap memory management.
  */
trait DynamicMemoryManager {

  /**
    * Allocates memory for requested size.
    *
    * @param size Request memory allocation size
    * @return The native address which represents the starting location of memory allocated
    */
  def allocateMemory(size: Long): Long

  /**
    * Frees memory allocated at the passed address
    *
    * @param address The native address which represents the starting location of memory allocated
    */
  def freeMemory(address: Long): Unit

  /**
    * Free all memory allocated by this memory manager
    */
  protected[memory] def freeAll(): Unit

}
