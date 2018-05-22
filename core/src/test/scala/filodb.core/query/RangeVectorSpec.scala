package filodb.core.query

import org.scalatest.{FunSpec, Matchers}

import filodb.core.metadata.Column.ColumnType
import filodb.memory.format.{RowReader, SeqRowReader, ZeroCopyUTF8String}


class RangeVectorSpec  extends FunSpec with Matchers {

  it("should be able to create and read from SerializableRangeVector") {
    val now = System.currentTimeMillis()
    val numRawSamples = 1000
    val reportingInterval = 10000
    val tuples = (numRawSamples until 0).by(-1).map(n => (now - n * reportingInterval, n.toDouble))

    val rv = new RangeVector {
      override def rows: Iterator[RowReader] = tuples.map { t =>
        new SeqRowReader(Seq[Any](t._1, t._2))
      }.iterator
      override def key: RangeVectorKey = new RangeVectorKey {
        def labelValues: Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = Map.empty
        def sourceShards: Seq[Int] = Seq(0)
      }
    }
    val cols = Array(new ColumnInfo("timestamp", ColumnType.LongColumn),
                     new ColumnInfo("value", ColumnType.DoubleColumn))
    val srv = SerializableRangeVector(rv, cols, numRawSamples)
    val observedTs = srv.rows.toSeq.map(_.getLong(0))
    val observedVal = srv.rows.toSeq.map(_.getDouble(1))
    observedTs shouldEqual tuples.map(_._1)
    observedVal shouldEqual tuples.map(_._2)

    // now we should also be able to create SerializableRangeVector using fast filo row iterator
    // since srv interator is based on FFRR, try that
    val srv2 = SerializableRangeVector(srv, cols, numRawSamples)
    val observedTs2 = srv2.rows.toSeq.map(_.getLong(0))
    val observedVal2 = srv2.rows.toSeq.map(_.getDouble(1))
    observedTs2 shouldEqual tuples.map(_._1)
    observedVal2 shouldEqual tuples.map(_._2)

  }
}
