package filodb.core.query

import org.scalatest.{FunSpec, Matchers}

import filodb.core.metadata.Column.ColumnType
import filodb.memory.format.{RowReader, SeqRowReader, ZeroCopyUTF8String}


class RangeVectorSpec  extends FunSpec with Matchers {
  val now = System.currentTimeMillis()
  val numRawSamples = 1000
  val reportingInterval = 10000
  val tuples = (numRawSamples until 0).by(-1).map(n => (now - n * reportingInterval, n.toDouble))

  class TuplesRangeVector(inputTuples: Seq[(Long, Double)]) extends RangeVector {
    override def rows: Iterator[RowReader] = inputTuples.map { t =>
      new SeqRowReader(Seq[Any](t._1, t._2))
    }.iterator
    override def key: RangeVectorKey = new RangeVectorKey {
      def labelValues: Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = Map.empty
      def sourceShards: Seq[Int] = Nil
      def partIds: Seq[Int] = Nil
    }
  }

  val cols = Array(new ColumnInfo("timestamp", ColumnType.LongColumn),
                   new ColumnInfo("value", ColumnType.DoubleColumn))

  it("should be able to create and read from SerializableRangeVector") {
    val rv = new TuplesRangeVector(tuples)
    val srv = SerializableRangeVector(rv, cols)
    val observedTs = srv.rows.toSeq.map(_.getLong(0))
    val observedVal = srv.rows.toSeq.map(_.getDouble(1))
    observedTs shouldEqual tuples.map(_._1)
    observedVal shouldEqual tuples.map(_._2)

    val srv2 = SerializableRangeVector(srv, cols)
    val observedTs2 = srv2.rows.toSeq.map(_.getLong(0))
    val observedVal2 = srv2.rows.toSeq.map(_.getDouble(1))
    observedTs2 shouldEqual tuples.map(_._1)
    observedVal2 shouldEqual tuples.map(_._2)
  }

  it("should be able to share containers across multiple SerializableRangeVectors") {
    val rvs = Seq(new TuplesRangeVector(tuples take 400), new TuplesRangeVector(tuples drop 400))
    val schema = SerializableRangeVector.toSchema(cols)
    val builder = SerializableRangeVector.toBuilder(schema)

    // Sharing one builder across multiple input RangeVectors
    val srvs = rvs.map(rv => SerializableRangeVector(rv, builder, schema))

    // Now verify each of them
    val observedTs = srvs(0).rows.toSeq.map(_.getLong(0))
    val observedVal = srvs(0).rows.toSeq.map(_.getDouble(1))
    observedTs shouldEqual tuples.take(400).map(_._1)
    observedVal shouldEqual tuples.take(400).map(_._2)

    val observedTs2 = srvs(1).rows.toSeq.map(_.getLong(0))
    val observedVal2 = srvs(1).rows.toSeq.map(_.getDouble(1))
    observedTs2 shouldEqual tuples.drop(400).map(_._1)
    observedVal2 shouldEqual tuples.drop(400).map(_._2)
  }
}
