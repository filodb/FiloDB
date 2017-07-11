package filodb

import java.io.{File => JFile}
import java.util.{Map => JMap, Properties => JProperties}

import scala.collection.JavaConverters._

/** Simple conversion implicits. */
// scalastyle:off
package object kafka {

  implicit final class AnyOps[A](self: A) {
    def ===(other: A): Boolean = self == other
  }

  implicit final class JavaOps(mutable: JMap[String, Object]) {

    def asImmutable: Map[String, AnyRef] =
      Map.empty[String, AnyRef] ++ mutable.asScala
  }

  implicit final class MapOps(immutable: Map[String, AnyRef]) {
    def asProps: JProperties = {
      val props = new JProperties()
      props.putAll(immutable.filter(_._2 != null).asJava)
      props
    }
  }

  implicit final class PropertiesOps(props: JProperties) {

    def asMap: Map[String, AnyRef] = props.asScala.toMap[String, AnyRef]
  }

  implicit final class FileOps(file: JFile) {

    def asMap = {
      val source = scala.io.Source.fromFile(file.getAbsolutePath)
      try {
        val props = new java.util.Properties()
        props.load(source.reader)
        props.asMap
      } finally source.close()
    }
  }
}
// scalastyle:on