package filodb.kafka

import java.util.concurrent.BlockingQueue

import akka.actor.Props

import filodb.coordinator.DirectRowSource
import filodb.core.metadata.RichProjection
import filodb.core.memstore.{IngestRecord, MemStore}

object QueuedRowSource {
  def props(queue: BlockingQueue[IngestRecord],
            memStore: MemStore,
            projection: RichProjection): Props =
    Props(classOf[KafkaRowSource], queue, memStore, projection)
}

/** Temporary: A RowSource taking its input from a BlockingQueue.*/
class KafkaRowSource(queue: BlockingQueue[IngestRecord],
                      val memStore: MemStore,
                      val projection: RichProjection) extends DirectRowSource {
  val batchIterator = new Iterator[Seq[IngestRecord]] {
    val rows = new collection.mutable.ArrayBuffer[IngestRecord]

    //scalastyle:off
    @annotation.tailrec
    private def peekAndAdd(): Unit = queue.poll() match {
      case null =>
      case record =>
        rows += record
        peekAndAdd()
    }
    //scalastyle:on

    def hasNext: Boolean = {
      rows.clear()
      val record = queue.take()
      rows += record
      peekAndAdd()
      true
    }

    def next: Seq[IngestRecord] = rows
  }
}
