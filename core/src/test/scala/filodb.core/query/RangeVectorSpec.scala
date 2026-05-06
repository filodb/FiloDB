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
    val observedTs = srv.rows().map(_.getLong(0)).toSeq
    val observedVal = srv.rows().map(_.getDouble(1)).toSeq
    observedTs shouldEqual tuples.map(_._1)
    observedVal shouldEqual tuples.map(_._2)

    val srv2 = SerializedRangeVector(srv, cols.toIndexedSeq, queryStats)
    val observedTs2 = srv2.rows().map(_.getLong(0)).toSeq
    val observedVal2 = srv2.rows().map(_.getDouble(1)).toSeq
    observedTs2 shouldEqual tuples.map(_._1)
    observedVal2 shouldEqual tuples.map(_._2)
  }

  it("should be able to share containers across multiple SerializedRangeVectors") {
    val rvs = Seq(new TuplesRangeVector(tuples take 400), new TuplesRangeVector(tuples drop 400))
    val schema = SerializedRangeVector.toSchema(cols)
    val builder = SerializedRangeVector.newBuilder()

    // Sharing one builder across multiple input RangeVectors
    val srvs = rvs.map(rv => SerializedRangeVector(rv, builder, schema, "RangeVectorSpec", queryStats))

    // Now verify each of them
    val observedTs = srvs(0).rows().map(_.getLong(0)).toSeq
    val observedVal = srvs(0).rows().map(_.getDouble(1)).toSeq
    observedTs shouldEqual tuples.take(400).map(_._1)
    observedVal shouldEqual tuples.take(400).map(_._2)

    val observedTs2 = srvs(1).rows().map(_.getLong(0)).toSeq
    val observedVal2 = srvs(1).rows().map(_.getDouble(1)).toSeq
    observedTs2 shouldEqual tuples.drop(400).map(_._1)
    observedVal2 shouldEqual tuples.drop(400).map(_._2)
  }

  it("should estimate default row count when no sources are implemented") {
    class TestRv extends RangeVector {
      override def outputRange: Option[RvRange] = None
      override def numRows: Option[Int] = None
      override def key: RangeVectorKey = ???
      override def rows(): RangeVectorCursor = ???
    }

    val rv = new TestRv
    assert(rv.estimateNumRows() == 1)
  }

  it("should estimate row count with numRows when only numRows is implemented") {
    class TestRv extends RangeVector {
      override def outputRange: Option[RvRange] = None
      override def numRows: Option[Int] = Some(123)
      override def key: RangeVectorKey = ???
      override def rows(): RangeVectorCursor = ???
    }

    val rv = new TestRv
    assert(rv.estimateNumRows() == 123)
  }

  it("should estimate row count with outputRange when only outputRange is implemented") {
    class TestRv(range: RvRange) extends RangeVector {
      override def outputRange: Option[RvRange] = Some(range)
      override def numRows: Option[Int] = None
      override def key: RangeVectorKey = ???
      override def rows(): RangeVectorCursor = ???
    }

    val instantRv = new TestRv(RvRange(1000, 10, 1000))
    assert(instantRv.estimateNumRows() == 1)

    val rangeRvNoSteps = new TestRv(RvRange(1000, 10, 1009))
    assert(rangeRvNoSteps.estimateNumRows() == 1)

    val rangeRvWithSteps = new TestRv(RvRange(1000, 10, 1100))
    assert(rangeRvWithSteps.estimateNumRows() == 11)
  }

  it("should estimate row count with numRows when both numRows and outputRange are implemented") {
    class TestRv extends RangeVector {
      override def outputRange: Option[RvRange] = Some(RvRange(1000, 10, 1100))
      override def numRows: Option[Int] = Some(123)
      override def key: RangeVectorKey = ???
      override def rows(): RangeVectorCursor = ???
    }

    val rv = new TestRv
    assert(rv.estimateNumRows() == 123)
  }
}
