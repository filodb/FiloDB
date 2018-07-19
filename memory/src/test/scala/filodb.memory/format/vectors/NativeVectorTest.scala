package filodb.memory.format.vectors

import org.scalatest.{FunSpec, Matchers, BeforeAndAfterAll}

import filodb.memory.NativeMemoryManager

trait NativeVectorTest extends FunSpec with Matchers with BeforeAndAfterAll {
  val memFactory = new NativeMemoryManager(10 * 1024 * 1024)

  override def afterAll(): Unit = {
    memFactory.freeAll()
  }
}