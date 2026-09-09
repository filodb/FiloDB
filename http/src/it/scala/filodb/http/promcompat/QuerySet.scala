package filodb.http.promcompat

import scala.jdk.CollectionConverters._

import com.typesafe.config.ConfigFactory

final case class PromQuery(promql: String, errorLimit: String, flags: String)

object QuerySet {
  /** Loads a HOCON resource whose top-level keys are category names mapped to flat string lists
    * of length %3 == 0: (promql, errorLimit, flags) repeated. */
  def load(resource: String): Map[String, Seq[PromQuery]] = {
    val url = getClass.getResource(resource)
    require(url != null, s"Query resource not found: $resource")
    fromConfig(ConfigFactory.parseURL(url))
  }

  private[promcompat] def fromConfig(config: com.typesafe.config.Config): Map[String, Seq[PromQuery]] = {
    config.root().keySet().asScala.map { category =>
      val flat = config.getStringList(category).asScala.toVector
      require(flat.length % 3 == 0, s"Category '$category' list length ${flat.length} is not a multiple of 3")
      val queries = flat.grouped(3).map(g => PromQuery(g(0), g(1), g(2))).toVector
      category -> queries
    }.toMap
  }
}
