package filodb.memory

import com.kenai.jffi.MemoryIO
import org.scalatest.{FlatSpec, Matchers}

import filodb.memory.BinaryRegion.Memory

/**
  * Buffer manager allocation and freeing tests
  */
class NativeMemoryManagerSpec extends FlatSpec with Matchers {


  it should "Allocate and allow writing up to the allocation size" in {
    //16 for magic header
    val bufferManager = new NativeMemoryManager(1000 + 16)
    checkAllocation(bufferManager.allocate(300))
    checkAllocation(bufferManager.allocate(300))
    checkAllocation(bufferManager.allocate(300))
    checkAllocation(bufferManager.allocate(100))
  }

  it should "Fail when trying to allocate beyond limit" in {
    val bufferManager = new NativeMemoryManager(1000)
    checkAllocation(bufferManager.allocate(300))
    checkAllocation(bufferManager.allocate(300))
    checkAllocation(bufferManager.allocate(300))
    intercept[IndexOutOfBoundsException] {
      checkAllocation(bufferManager.allocate(300))
    }
  }

  it should "Fail when trying to allocate beyond limit. Then succeed after freeing" in {
    val bufferManager = new NativeMemoryManager(1000)
    checkAllocation(bufferManager.allocate(300))
    val toFree = bufferManager.allocate(300)
    checkAllocation(toFree)
    checkAllocation(bufferManager.allocate(300))
    intercept[IndexOutOfBoundsException] {
      checkAllocation(bufferManager.allocate(300))
    }
    bufferManager.freeMemory(toFree._2)
    checkAllocation(bufferManager.allocate(300))
  }

  private def checkAllocation(memory: Memory) = {
    val bytes1 = "Hello Huge Pages".getBytes("UTF-8")
    val buffer = MemoryIO.getCheckedInstance().newDirectByteBuffer(memory._2, 300)
    buffer.putInt(bytes1.length)
    buffer.put(bytes1)
    buffer.flip
    val l = buffer.getInt()
    val check1 = new Array[Byte](l)
    buffer.get(check1)
    new String(check1, "UTF-8") should be("Hello Huge Pages")
  }
}
