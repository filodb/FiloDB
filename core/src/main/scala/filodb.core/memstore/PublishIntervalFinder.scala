package filodb.core.memstore

import filodb.core.binaryrecord2.MapItemConsumer
import filodb.core.metadata.Schemas
import filodb.memory.{UTF8StringMedium, UTF8StringShort}
import filodb.memory.format.{ZeroCopyUTF8String => UTF8Str}

trait PublishIntervalFinder {
  def findPublishIntervalMs(pkSchemaId: Int, pkBase: Any, pkOffset: Long): Option[Long]
}

object StepTagPublishIntervalFinder extends PublishIntervalFinder {
  val (stepBase, stepOffset) = UTF8StringShort.apply("_step_")

  override def findPublishIntervalMs(pkSchemaId: Int, pkBase: Any, pkOffset: Long): Option[Long] = {
    var result: Option[Long] = None
    if (pkSchemaId == Schemas.global.part.hash) {
      val consumer = new MapItemConsumer {
        override def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
          val isStepTag = UTF8StringShort.matchAt(keyBase, keyOffset, stepBase, stepOffset, 0)
          if (isStepTag) {
            val str = new UTF8Str(valueBase, valueOffset + 2, UTF8StringMedium.numBytes(valueBase, valueOffset))
            result = Some(str.toString.toLong * 1000) // step tag unit is seconds
          }
        }
      }
      Schemas.global.part.binSchema.consumeMapItems(pkBase, pkOffset, 1, consumer)
    }
    result
  }
}