package filodb.core.memstore

import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schemas
import filodb.memory.NativeMemoryManager
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.memory.format.UnsafeUtils
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}

class PublishIntervalFinderSpec  extends FunSpec with Matchers with ScalaFutures with BeforeAndAfter {

  val nativeMemoryManager = new NativeMemoryManager(300000, Map.empty)

  after {
    nativeMemoryManager.shutdown()
  }

  it ("should extract step tag for publish interval when present") {
    val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8, "_ns_".utf8 -> "my_ns".utf8, "_step_".utf8 -> "10".utf8)
    val partBuilder = new RecordBuilder(nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.promCounter, "counterName".utf8, seriesTags)
    val pubInt = StepTagPublishIntervalFinder.findPublishIntervalMs(
      Schemas.promCounter.partition.hash, UnsafeUtils.ZeroArray,
      partKey)
    pubInt shouldEqual Some(10000L)
  }

  it ("should extract return None for step tag for publish interval when absent") {
    val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8, "_ns_".utf8 -> "my_ns".utf8)
    val partBuilder = new RecordBuilder(nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.promCounter, "counterName".utf8, seriesTags)
    val pubInt = StepTagPublishIntervalFinder.findPublishIntervalMs(
      Schemas.promCounter.partition.hash, UnsafeUtils.ZeroArray,
      partKey)
    pubInt shouldEqual None
  }

}
