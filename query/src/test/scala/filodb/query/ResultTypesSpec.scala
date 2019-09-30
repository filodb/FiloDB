package filodb.query

import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}
import filodb.query.exec.TransientRow

class ResultTypesSpec extends FunSpec with Matchers with ScalaFutures {

  val columns: Seq[ColumnInfo] = Seq(ColumnInfo("timestamp", ColumnType.LongColumn),
    ColumnInfo("value", ColumnType.DoubleColumn))
  val resultSchema = ResultSchema(columns, 1)
  val ignoreKey = CustomRangeVectorKey(
    Map(ZeroCopyUTF8String("ignore") -> ZeroCopyUTF8String("ignore")))
  it("should have result type RangeVectors when multiple rows exist ") {
    val rv = new RangeVector {
      val row = new TransientRow()

      override def key: RangeVectorKey = ignoreKey

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 3.3d),
        new TransientRow(2L, 5.1d)).iterator

      override def numRows: Option[Int] = Option(rows.size)

    }
    val queryResult = QueryResult("id:1", resultSchema, Seq(rv))
    queryResult.resultType.toString shouldEqual ("RangeVectors")
  }

  it("should have result type InstantVector when there is only one row per RangeVector") {
    val rv1 = new RangeVector {
      val row = new TransientRow()

      override def key: RangeVectorKey = ignoreKey

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 3.3d)).toIterator
      override def numRows: Option[Int] = Option(rows.size)

    }

    val rv2 = new RangeVector {
      val row = new TransientRow()

      override def key: RangeVectorKey = ignoreKey

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 9.4d)).toIterator
      override def numRows: Option[Int] = Option(rows.size)

    }
    val queryResult = QueryResult("id:1", resultSchema, Seq(rv1, rv2))

    queryResult.resultType shouldEqual(QueryResultType.InstantVector)
  }

  it("should have result type Scalar when there is no RangeVectorKey") {
    val rv1 = new RangeVector {
      val row = new TransientRow()

      override def key: RangeVectorKey = CustomRangeVectorKey(Map.empty)

      override def rows: Iterator[RowReader] = Seq(
        new TransientRow(1L, 3.3d)).toIterator
      override def numRows: Option[Int] = Option(rows.size)

    }

    val queryResult = QueryResult("id:1", resultSchema, Seq(rv1))
    queryResult.resultType shouldEqual(QueryResultType.Scalar)
  }
}