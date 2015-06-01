package filodb.core.datastore

import java.nio.ByteBuffer
import org.velvia.filo.{ColumnParser, ColumnWrapper}

import filodb.core.metadata.Column

object Serde {
  import Column.ColumnType._

  val ColTypeToClazz = Map[Column.ColumnType, Class[_]](
                         IntColumn    -> classOf[Int],
                         LongColumn   -> classOf[Long],
                         DoubleColumn -> classOf[Double],
                         StringColumn -> classOf[String],
                         BitmapColumn -> classOf[Boolean]
                       )

  import org.velvia.filo.ColumnMaker
  import org.velvia.filo.ColumnParser._

  trait ColumnRowExtractor[CT, R] {
    val wrapper: ColumnWrapper[CT]
    def extractToRow(rowNo: Int, row: R)
  }

  // TODO: replace this with macros !!
  class IntColumnRowExtractor[R](bytes: ByteBuffer, colIndex: Int, setter: RowSetter[R])
  extends ColumnRowExtractor[Int, R] {
    val wrapper = ColumnParser.parse[Int](bytes)
    def extractToRow(rowNo: Int, row: R) {
      if (wrapper.isAvailable(rowNo)) setter.setInt(row, colIndex, wrapper(rowNo))
    }
  }

  class DoubleColumnRowExtractor[R](bytes: ByteBuffer, colIndex: Int, setter: RowSetter[R])
  extends ColumnRowExtractor[Double, R] {
    val wrapper = ColumnParser.parse[Double](bytes)
    def extractToRow(rowNo: Int, row: R) {
      if (wrapper.isAvailable(rowNo)) setter.setDouble(row, colIndex, wrapper(rowNo))
    }
  }

  class LongColumnRowExtractor[R](bytes: ByteBuffer, colIndex: Int, setter: RowSetter[R])
  extends ColumnRowExtractor[Long, R] {
    val wrapper = ColumnParser.parse[Long](bytes)
    def extractToRow(rowNo: Int, row: R) {
      if (wrapper.isAvailable(rowNo)) setter.setLong(row, colIndex, wrapper(rowNo))
    }
  }

  class StringColumnRowExtractor[R](bytes: ByteBuffer, colIndex: Int, setter: RowSetter[R])
  extends ColumnRowExtractor[String, R] {
    val wrapper = ColumnParser.parse[String](bytes)
    def extractToRow(rowNo: Int, row: R) {
      if (wrapper.isAvailable(rowNo)) setter.setString(row, colIndex, wrapper(rowNo))
    }
  }

  def getRowExtractors[R](chunks: Array[ByteBuffer], columns: Seq[Column], setter: RowSetter[R]):
      Array[ColumnRowExtractor[_, R]] = {
    require(chunks.size == columns.size, "chunks and columns must be same length")
    val extractors = for { i <- 0 until chunks.length } yield {
      columns(i).columnType match {
        case IntColumn    => new IntColumnRowExtractor(chunks(i), i, setter)
        case LongColumn   => new LongColumnRowExtractor(chunks(i), i, setter)
        case DoubleColumn => new DoubleColumnRowExtractor(chunks(i), i, setter)
        case StringColumn => new StringColumnRowExtractor(chunks(i), i, setter)
        case BitmapColumn => ???
      }
    }
    extractors.toArray
  }
}