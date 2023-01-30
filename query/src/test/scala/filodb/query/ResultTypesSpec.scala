package filodb.query

import org.scalatest.concurrent.ScalaFutures

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.ZeroCopyUTF8String
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


class ResultTypesSpec extends AnyFunSpec with Matchers with ScalaFutures {

  val columns: Seq[ColumnInfo] = Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn))
  val resultSchema = ResultSchema(columns, 1)
  val ignoreKey = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))
  it("should have result type RangeVectors when multiple rows exist ") {
    val rv = new RangeVector {
      val row = new TransientRow()

      override def key: RangeVectorKey = ignoreKey

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 3.3d),
        new TransientRow(2L, 5.1d)).iterator

      override def numRows: Option[Int] = Option(rows.size)

      override def outputRange: Option[RvRange] = None
    }
    val queryResult = QueryResult("id:1", resultSchema, Seq(rv))
    queryResult.resultType.toString shouldEqual ("RangeVectors")
  }

  it("should have result type InstantVector when there is only one row per RangeVector") {
    val rv1 = new RangeVector {
      val row = new TransientRow()

      override def key: RangeVectorKey = ignoreKey

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 3.3d)).toIterator
      override def numRows: Option[Int] = Option(rows.size)

      override def outputRange: Option[RvRange] = None

    }

    val rv2 = new RangeVector {
      val row = new TransientRow()

      override def key: RangeVectorKey = ignoreKey

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 9.4d)).toIterator
      override def numRows: Option[Int] = Option(rows.size)
      override def outputRange: Option[RvRange] = None

    }
    val queryResult = QueryResult("id:1", resultSchema, Seq(rv1, rv2))

    queryResult.resultType shouldEqual(QueryResultType.InstantVector)
  }

  it("should have result type Scalar when there is no RangeVectorKey") {
    val rv1 = new RangeVector {
      val row = new TransientRow()

      override def key: RangeVectorKey = CustomRangeVectorKey(Map.empty)

      import NoCloseCursor._
      override def rows(): RangeVectorCursor = Seq(
        new TransientRow(1L, 3.3d)).toIterator
      override def numRows: Option[Int] = Option(rows.size)
      override def outputRange: Option[RvRange] = None

    }

    val queryResult = QueryResult("id:1", resultSchema, Seq(rv1))
    queryResult.resultType shouldEqual(QueryResultType.Scalar)
  }

  it("should ignore isCumulative field when comparing ColumnInfos") {
    val columns1: Seq[ColumnInfo] = Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn, true),
      ColumnInfo("value", ColumnType.DoubleColumn))
    val resultSchema1 = ResultSchema(columns1, 1)

    val columns2: Seq[ColumnInfo] = Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn, false),
      ColumnInfo("value", ColumnType.DoubleColumn))
    val resultSchema2 = ResultSchema(columns2, 1)

    resultSchema1 shouldEqual resultSchema2

    val columns3: Seq[ColumnInfo] = Seq(ColumnInfo("timestamp1", ColumnType.TimestampColumn, false),
      ColumnInfo("value", ColumnType.DoubleColumn))
    val resultSchema3 = ResultSchema(columns3, 1)

    resultSchema2 should not be resultSchema3

  }
}