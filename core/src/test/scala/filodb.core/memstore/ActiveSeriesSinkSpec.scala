package filodb.core.memstore

import scala.collection.mutable

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ActiveSeriesSinkSpec extends AnyFunSpec with Matchers {

  describe("NoOpActiveSeriesSink") {
    it("ignores onActivate/onDeactivate without throwing") {
      noException should be thrownBy {
        NoOpActiveSeriesSink.onActivate(Seq("ws", "ns"), Array[Byte](1, 2, 3))
        NoOpActiveSeriesSink.onDeactivate(Seq("ws", "ns"), Array[Byte](1, 2, 3))
        NoOpActiveSeriesSink.close()
      }
    }
  }

  describe("RecordingSink (test fixture)") {
    it("records the order of activate/deactivate calls") {
      val sink = new RecordingActiveSeriesSink

      val pk1 = Array[Byte](1, 2, 3)
      val pk2 = Array[Byte](4, 5, 6)

      sink.onActivate(Seq("demo", "default"), pk1)
      sink.onActivate(Seq("demo", "default"), pk2)
      sink.onDeactivate(Seq("demo", "default"), pk1)

      sink.events should have size 3
      sink.events(0)._1 shouldEqual "activate"
      sink.events(0)._2 shouldEqual Seq("demo", "default")
      sink.events(0)._3 shouldEqual pk1.toSeq

      sink.events(1)._1 shouldEqual "activate"
      sink.events(1)._3 shouldEqual pk2.toSeq

      sink.events(2)._1 shouldEqual "deactivate"
      sink.events(2)._3 shouldEqual pk1.toSeq
    }
  }
}

/**
 * Thread-safe in-memory sink used in tests. Captures the sequence of calls so
 * test assertions can verify the four hook sites in TimeSeriesShard fire as
 * expected.
 */
class RecordingActiveSeriesSink extends ActiveSeriesSink {
  private val buf = mutable.Buffer.empty[(String, Seq[String], Seq[Byte])]
  private val lock = new Object

  override def onActivate(shardKeyValues: Seq[String], partKeyBytes: Array[Byte]): Unit = lock.synchronized {
    buf += (("activate", shardKeyValues, partKeyBytes.toSeq))
  }

  override def onDeactivate(shardKeyValues: Seq[String], partKeyBytes: Array[Byte]): Unit = lock.synchronized {
    buf += (("deactivate", shardKeyValues, partKeyBytes.toSeq))
  }

  override def close(): Unit = ()

  def events: Seq[(String, Seq[String], Seq[Byte])] = lock.synchronized(buf.toSeq)

  def reset(): Unit = lock.synchronized(buf.clear())
}
