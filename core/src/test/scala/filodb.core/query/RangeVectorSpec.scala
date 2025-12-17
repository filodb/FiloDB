package filodb.core.query


import filodb.core.metadata.Column.ColumnType
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


class RangeVectorSpec  extends AnyFunSpec with Matchers {
  val now = System.currentTimeMillis()
  val numRawSamples = 1000
  val reportingInterval = 10000
  val tuples = (numRawSamples until 0).by(-1).map(n => (now - n * reportingInterval, n.toDouble))
  val queryStats = QueryStats()

  class TuplesRangeVector(inputTuples: Seq[(Long, Double)]) extends RangeVector {
    import NoCloseCursor._
    override def rows(): RangeVectorCursor = inputTuples.map { t =>
      new SeqRowReader(Seq[Any](t._1, t._2))
    }.iterator
    override def key: RangeVectorKey = new RangeVectorKey {
      def labelValues: Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = Map.empty
      def sourceShards: Seq[Int] = Nil
      def partIds: Seq[Int] = Nil
      def schemaNames: Seq[String] = Nil
      def keySize: Int = 0
    }

    override def outputRange: Option[RvRange] = None
  }

  val cols = Array(new ColumnInfo("timestamp", ColumnType.TimestampColumn),
                   new ColumnInfo("value", ColumnType.DoubleColumn))

  it("should be able to create and read from SerializedRangeVector") {
    val rv = new TuplesRangeVector(tuples)
    val srv = SerializedRangeVector(rv, cols.toIndexedSeq, queryStats)
    // Extract values during iteration since the underlying reader object is reused
    val observed = srv.rows().map(r => (r.getLong(0), r.getDouble(1))).toVector
    val observedTs = observed.map(_._1)
    val observedVal = observed.map(_._2)
    observedTs shouldEqual tuples.map(_._1)
    observedVal shouldEqual tuples.map(_._2)

    val srv2 = SerializedRangeVector(srv, cols.toIndexedSeq, queryStats)
    // Extract values during iteration since the underlying reader object is reused
    val observed2 = srv2.rows().map(r => (r.getLong(0), r.getDouble(1))).toVector
    val observedTs2 = observed2.map(_._1)
    val observedVal2 = observed2.map(_._2)
    observedTs2 shouldEqual tuples.map(_._1)
    observedVal2 shouldEqual tuples.map(_._2)
  }

  it("should be able to share containers across multiple SerializedRangeVectors") {
    val rvs = Seq(new TuplesRangeVector(tuples take 400), new TuplesRangeVector(tuples drop 400))
    val schema = SerializedRangeVector.toSchema(cols.toIndexedSeq)
    val builder = SerializedRangeVector.newBuilder()

    // Sharing one builder across multiple input RangeVectors
    val srvs = rvs.map(rv => SerializedRangeVector(rv, builder, schema, "RangeVectorSpec", queryStats))

    // Now verify each of them - extract values during iteration since the underlying reader object is reused
    val observed = srvs(0).rows().map(r => (r.getLong(0), r.getDouble(1))).toVector
    val observedTs = observed.map(_._1)
    val observedVal = observed.map(_._2)
    observedTs shouldEqual tuples.take(400).map(_._1)
    observedVal shouldEqual tuples.take(400).map(_._2)

    val observed2 = srvs(1).rows().map(r => (r.getLong(0), r.getDouble(1))).toVector
    val observedTs2 = observed2.map(_._1)
    val observedVal2 = observed2.map(_._2)
    observedTs2 shouldEqual tuples.drop(400).map(_._1)
    observedVal2 shouldEqual tuples.drop(400).map(_._2)
  }
}
