package filodb.http

import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import org.scalatest.Assertions._

import filodb.core.query.{QueryContext, QueryStats}
import filodb.query.QueryCompletionListener

object RecordingQueryCompletionListener {
  case class Event(queryContext: QueryContext, queryStats: QueryStats)

  /**
   * Waits for the listener to record a callback, then makes sure no extra callbacks arrive.
   * @param timeoutMs Max time until first event is recorded.
   * @param stragglerMs Max time waiting after first event is recorded for stragglers to arrive.
   * @return the recorded events (exactly one)
   */
  def awaitExactlyOnce(listener: RecordingQueryCompletionListener,
                       timeoutMs: Long = 500,
                       stragglerMs: Long = 250): Seq[Event] = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (listener.snapshot.isEmpty && System.currentTimeMillis() < deadline) {
      Thread.sleep(50)
    }
    Thread.sleep(stragglerMs) // a duplicate callback, if any, would show up by now
    val events = listener.snapshot
    assert(events.size == 1, s"expected exactly 1 QueryCompletionListener callback but got ${events.size}")
    events
  }
}

/**
 * Test [[QueryCompletionListener]] that records every callback so tests can assert on them.
 */
class RecordingQueryCompletionListener extends QueryCompletionListener {
  import RecordingQueryCompletionListener.Event

  private val events = new ConcurrentLinkedQueue[Event]()

  /** When set, the next callback is recorded and then throws; the flag resets itself. */
  @volatile var failOnNext = false

  override def onQueryComplete(queryContext: QueryContext, queryStats: QueryStats): Unit = {
    events.add(Event(queryContext, queryStats))
    if (failOnNext) {
      failOnNext = false
      throw new RuntimeException("*** TEST *** Expected listener failure")
    }
  }

  def snapshot: Seq[Event] = events.asScala.toSeq
  def clear(): Unit = events.clear()
}
