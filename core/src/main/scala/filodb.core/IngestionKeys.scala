package filodb.core

import scala.util.Try

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/** High-usage ingestion setup and recovery keys read from config and storage.
  * E.g. see `filodb.core.store.MetaStore` and `filodb.cassandra.IngestionConfigTable`.
  */
object IngestionKeys {

  val Dataset = "dataset"

  val Database = "database"

  val Resources = "resources"

  val SourceConfig = "sourceconfig"

  val SourceFactory = "sourcefactory"

  val FactoryClass = "factoryclass"

  val NumShards = "num-shards"

  val MinNumNodes = "min-num-nodes"

  /** Error helpers for Config. */
  implicit final class ConfigOps(c: Config) {

    /** Attempts to resolve config submitted. */
    def resolveT: Try[Config] = Try(c.resolve)

    /** Attempts to resolve configured `key`'s value to a String if exists. */
    def stringT(key: String): Try[String] = Try(c.as[String](key))
    def intT(key: String): Try[Int] = Try(c.as[Int](key))
    def configT(key: String): Try[Config] = Try(c.getConfig(key))
  }
}