package filodb.memory.format.vectors

import org.scalatest.BeforeAndAfterAll

import filodb.memory.NativeMemoryManager
import filodb.memory.format.{MemoryAccessor}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

trait NativeVectorTest extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  val acc = MemoryAccessor.nativePtrAccessor

  val memFactory = new NativeMemoryManager(10 * 1024 * 1024)

  override def afterAll(): Unit = {
    memFactory.freeAll()
  }
}