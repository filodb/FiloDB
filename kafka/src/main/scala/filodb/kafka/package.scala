package filodb

import java.util.{Map => JMap, Properties => JProperties}

import scala.collection.JavaConverters._
import scala.util.Try

import com.typesafe.config.{Config, ConfigFactory}

/** Simple conversion implicits. */
// scalastyle:off
package object kafka {

  implicit final class AnyOps[A](self: A) {
    def ===(other: A): Boolean = self == other
  }

  implicit final class KafkaClientOps(typedConfig: Map[String, AnyRef]) {
    def asConfig: Config =
      ConfigFactory.parseMap(typedConfig.map {
        case (k, v: Class[_]) => k -> v.getName // resolves `Config` Class type issue
        case (k, v: java.util.Collection[_]) => k -> v.asScala.map(_.toString).mkString(",")
        case (k, v) => Try(k -> v).getOrElse(k -> v.toString)
      }.asJava)
  }

  implicit final class JavaOps(mutable: JMap[String, Object]) {

    def asImmutable: Map[String, AnyRef] =
      Map.empty[String, AnyRef] ++ mutable.asScala
  }

  implicit final class MapOps(immutable: Map[String, AnyRef]) {
    def asProps: JProperties = {
      val props = new JProperties()
      for ((k,v) <- immutable) {
        if (k != null && v != null) {
          props.put(k, v)
        }
      }
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
