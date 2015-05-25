package filodb.core.datastore

import java.nio.ByteBuffer
import org.velvia.filo.{ColumnParser, ColumnWrapper}
import scala.reflect.ClassTag

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

   // TODO: refactor this, just use this as container for deser stuff for now
  sealed trait FiloDeserializer[A] {
    def getClazz(implicit classTagA: ClassTag[A]): Class[_] = classTagA.runtimeClass
    val clazz = getClass
    def deserialize[A: ColumnMaker](bytes: ByteBuffer): ColumnWrapper[A] =
      ColumnParser.parse[A](bytes)(implicitly[ColumnMaker[A]])
  }
}