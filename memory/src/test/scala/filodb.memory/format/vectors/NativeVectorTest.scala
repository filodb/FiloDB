package filodb.memory.format.vectors

import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

import filodb.memory.NativeMemoryManager
import filodb.memory.format.MemoryAccessor

trait NativeVectorTest extends FunSpec with Matchers with BeforeAndAfterAll {

  val acc = MemoryAccessor.nativePointer

  val memFactory = new NativeMemoryManager(10 * 1024 * 1024)

  override def afterAll(): Unit = {
    memFactory.freeAll()
  }
}