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

  abstract class ColumnRowExtractor[CT: ColumnMaker, R] {
    val bytes: ByteBuffer
    val setter: RowSetter[R]
    val colIndex: Int
    val wrapper: ColumnWrapper[CT] = ColumnParser.parse[CT](bytes)(implicitly[ColumnMaker[CT]])
    final def extractToRow(rowNo: Int, row: R): Unit =
      if (wrapper.isAvailable(rowNo)) extract(rowNo, row) else setter.setNA(row, colIndex)

    // Basically, set the row R from column rowNo assuming value is available
    def extract(rowNo: Int, row: R): Unit
  }

  // TODO: replace this with macros !!
  class IntColumnRowExtractor[R](val bytes: ByteBuffer, val colIndex: Int, val setter: RowSetter[R])
  extends ColumnRowExtractor[Int, R] {
    final def extract(rowNo: Int, row: R): Unit = setter.setInt(row, colIndex, wrapper(rowNo))
  }

  class DoubleColumnRowExtractor[R](val bytes: ByteBuffer, val colIndex: Int, val setter: RowSetter[R])
  extends ColumnRowExtractor[Double, R] {
    final def extract(rowNo: Int, row: R): Unit = setter.setDouble(row, colIndex, wrapper(rowNo))
  }

  class LongColumnRowExtractor[R](val bytes: ByteBuffer, val colIndex: Int, val setter: RowSetter[R])
  extends ColumnRowExtractor[Long, R] {
    final def extract(rowNo: Int, row: R): Unit = setter.setLong(row, colIndex, wrapper(rowNo))
  }

  class StringColumnRowExtractor[R](val bytes: ByteBuffer, val colIndex: Int, val setter: RowSetter[R])
  extends ColumnRowExtractor[String, R] {
    final def extract(rowNo: Int, row: R): Unit = setter.setString(row, colIndex, wrapper(rowNo))
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