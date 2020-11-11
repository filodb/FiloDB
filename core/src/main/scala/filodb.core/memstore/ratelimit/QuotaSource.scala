package filodb.core.memstore.ratelimit

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader

import filodb.core.DatasetRef

case class QuotaRecord(shardKeyPrefix: Seq[String], quota: Int)

/**
 * Source of quotas for shard key prefixes.
 */
trait QuotaSource {

  /**
   * Fetch all configured quotas. Invoked when a new Time Series Shard is bootstrapped.
   *
   * The quota represents number of immediate children allowed for the given
   * shard key prefix within each shard.
   */
  def getQuotas(dataset: DatasetRef): Iterator[QuotaRecord]

  /**
   * Quota to use in case explicit quota record is not present.
   * Return value is one item for each level of the tree.
   * Hence number of items in the returned sequence should be
   * shardKeyLen + 1
   */
  def getDefaults(dataset: DatasetRef): Seq[Int]
}

/**
 * QuotaSource implementation where static quota definitions are loaded from server configuration.
 */
class ConfigQuotaSource(filodbConfig: Config, shardKeyLen: Int) extends QuotaSource {
  implicit val quotaReader: ValueReader[QuotaRecord] = ValueReader.relative { quotaConfig =>
    QuotaRecord(quotaConfig.as[Seq[String]]("shardKeyPrefix"),
                quotaConfig.as[Int]("quota"))
  }

  def getQuotas(dataset: DatasetRef): Iterator[QuotaRecord] = {
    if (filodbConfig.hasPath(s"quotas.$dataset.custom")) {
      filodbConfig.as[Seq[QuotaRecord]](s"quotas.$dataset.custom").iterator
    } else {
      Iterator.empty
    }
  }

  def getDefaults(dataset: DatasetRef): Seq[Int] = {
    if (filodbConfig.hasPath(s"quotas.$dataset.custom")) {
      val defaults = filodbConfig.as[Seq[Int]](s"quotas.$dataset.defaults")
      require(defaults.length == shardKeyLen + 1, s"Quota defaults $defaults was not of length ${shardKeyLen + 1}")
      defaults
    } else {
      val default = filodbConfig.as[Int]("quotas.default")
      Seq.fill(shardKeyLen + 1)(default)
    }
  }
}