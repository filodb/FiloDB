package filodb

import java.util.{Map => JMap, Properties => JProperties}

import scala.collection.JavaConverters._

import com.typesafe.config.Config

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

  implicit final class ConfigOps(config: Config) {
    // Returns a map with an entry for every node or leaf in the config; it's like walking the config
    // depth first.  Allows us to return the full config path like 'a.b.c' for every entry.
    def flattenToMap: Map[String, AnyRef] =
      config.entrySet.asScala.map { e => e.getKey -> e.getValue.unwrapped }.toMap
  }
}
// scalastyle:on