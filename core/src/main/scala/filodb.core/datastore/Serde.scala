package filodb.core.datastore

import java.nio.ByteBuffer
import org.velvia.filo.{RowExtractors, RowSetter}

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

  def getRowExtractors[R](chunks: Array[ByteBuffer], columns: Seq[Column], setter: RowSetter[R]):
      Array[RowExtractors.ColumnRowExtractor[_, R]] =
    RowExtractors.getRowExtractors(chunks, columns.map(c => ColTypeToClazz(c.columnType)), setter)
}