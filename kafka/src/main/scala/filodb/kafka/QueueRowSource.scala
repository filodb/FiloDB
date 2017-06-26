package filodb.kafka

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import akka.actor.Actor
import filodb.coordinator.DirectRowSource
import filodb.core.metadata.RichProjection
import filodb.core.memstore.{IngestRecord, MemStore}

/** Temporary: uses current vs new way. Provides backpressure to FiloDB. */
trait QueueRowSource extends DirectRowSource {
  import QueueRowSource._

  def memStore: MemStore
  def projection: RichProjection

  protected val queue: BlockingQueue[IngestRecord] = new LinkedBlockingQueue[IngestRecord]()

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

  override def receive: Actor.Receive = backpressureReceive orElse super.receive

  private[filodb] def backpressureReceive: Actor.Receive = {
    case GetQueueCount => sender() ! QueueCount(queue.size)
  }
}

object QueueRowSource {
  final case object GetQueueCount
  final case class QueueCount(count: Int)
}