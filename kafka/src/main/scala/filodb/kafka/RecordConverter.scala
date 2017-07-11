package filodb.kafka

import scala.util.control.NonFatal
import scala.reflect.ClassTag
import scala.util.Try

import com.typesafe.scalalogging.StrictLogging

import filodb.core.metadata.RichProjection
import org.velvia.filo.{RowReader, SeqRowReader}

/** Extend to create custom converters for even types. */
trait RecordConverter {

  def convert(proj: RichProjection, event: AnyRef, offset: Long): Seq[RowReader]
}

object RecordConverter extends Instance with StrictLogging {

  def apply(fqcn: String): RecordConverter =
    createClass(fqcn)
      .flatMap{ c: Class[_] => createInstance[RecordConverter](c)}
      .recover { case NonFatal(e) =>
        logger.error(s"Unable to instantiate IngestionConverter from $fqcn", e)
        throw e
      }.get
}

/** A simple converter for String types. */
class StringRecordConverter extends RecordConverter {

  override def convert(proj: RichProjection, event: AnyRef, offset: Long): Seq[RowReader] = {
    event match {
      case e: String => Seq(SeqRowReader(e))
      case _ => Seq.empty[RowReader]
    }
  }
}

trait Instance {

  /** Used to bootstrap configurable FQCNs from config to instances. */
  def createClass[T: ClassTag](fqcn: String): Try[Class[T]] =
    Try[Class[T]] {
      val c = Class.forName(fqcn, false, getClass.getClassLoader).asInstanceOf[Class[T]]
      val t = implicitly[ClassTag[T]].runtimeClass
      if (c.isAssignableFrom(c) || c.getName == fqcn) {
        c
      }
      else {
        throw new ClassCastException(s"$c must be assignable from or be $c")
      }
    }

  /** Attempts to create instance of configured type, for no-arg constructor. */
  def createInstance[T: ClassTag](clazz: Class[_]): Try[T] =
    Try {
      val constructor = clazz.getDeclaredConstructor()
      constructor.setAccessible(true)
      val obj = constructor.newInstance()
      obj.asInstanceOf[T]
    }
}

