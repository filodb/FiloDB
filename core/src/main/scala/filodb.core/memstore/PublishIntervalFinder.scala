package filodb.core.memstore

import filodb.core.binaryrecord2.MapItemConsumer
import filodb.core.metadata.Schemas
import filodb.memory.{UTF8StringMedium, UTF8StringShort}
import filodb.memory.format.{ZeroCopyUTF8String => UTF8Str}

trait PublishIntervalFinder {
  def findPublishIntervalMs(pkSchemaId: Int, pkBase: Any, pkOffset: Long): Option[Long]
}

object StepTagPublishIntervalFinder extends PublishIntervalFinder {

  import UTF8Str._
  val stepTag = "_step_".utf8

  override def findPublishIntervalMs(pkSchemaId: Int, pkBase: Any, pkOffset: Long): Option[Long] = {
    var result: Option[Long] = None
    if (pkSchemaId == Schemas.global.part.hash) {
      val consumer = new MapItemConsumer {
        override def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
          val keyUtf8 = new UTF8Str(keyBase, keyOffset + 1, UTF8StringShort.numBytes(keyBase, keyOffset))
          if (keyUtf8 == stepTag) {
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