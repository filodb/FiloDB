package filodb.core.downsample

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

import filodb.core.DatasetRef

object IndexMetastoreImplementation extends Enumeration {
  val NoImp, File, Ephemeral = Value
}

final case class DownsampleConfig(config: Config) {
  /**
    * Resolutions to downsample at
    */
  val resolutions = if (config.hasPath ("resolutions")) config.as[Seq[FiniteDuration]]("resolutions")
                    else Seq.empty

  // FIXME: just initially create a immutable trie and remove this method.
  /**
   * DFS through nested mutable Maps, and return an equivalent tree of nested immutable Maps.
   */
  private def mapToImmutable(map: mutable.Map[String, Any]): Map[String, Any] = {
    if (map.isEmpty) {
      return Map()
    }
    map.map{ case (k, v) =>
      k -> mapToImmutable(v.asInstanceOf[mutable.Map[String, Any]])
    }.toMap
  }

  /**
   * Convert data-shape keys into a trie, where exactly the set of unique keys is described
   *   by the set of unique paths through the trie.
   */
  private def dataShapeKeysToTrie(keys: Seq[Seq[String]]): Map[String, Any] = {
    val map = new mutable.HashMap[String, Any]
    for (key <- keys) {
      var ptr = map
      for (value <- key) {
        ptr = ptr.getOrElseUpdate(value, new mutable.HashMap[String, Any])
          .asInstanceOf[mutable.HashMap[String, Any]]
      }
    }
    mapToImmutable(map)
  }

  /**
   * A sequence of label keys that constitute a "data-shape" key.
   * A series' corresponding label values are used to determine whether-or-not its data-shape stats are published.
   * Additionally, stats are published against these labels iff enable-data-shape-key-labels=true.
   *
   * For example:
   *   data-shape-key=[labelA, labelB]
   *   data-shape-allow=[(valueA1, valueB1), (valueA2)]
   *   data-shape-block=[(valueA2, valueB2)]
   *
   *   --> Metrics are published for all series with labelA=valueA1,labelB=valueB1
   *       and all with labelA=valueA2 except where labelB=valueB2. Additionally, metrics
   *       are published with labelA=value and labelB=value tags iff enable-data-shape-key-labels=true.
   */
  val dataShapeKey = config.getOrElse[Seq[String]]("data-shape-key", Seq())

  /**
   * Maps each data-shape key label to its index.
   * Used to efficiently build a key while iterating labels.
   */
  val dataShapeKeyIndex = dataShapeKey.zipWithIndex.map{ case (key, i) => key -> i }.toMap

  /**
   * Data-shape stats are published with data-shape-key values iff this sequence is nonempty.
   * If populated, this sequence must have the same size as data-shape-key; each ith string will be the
   *   published key for the ith data-shape key's value.
   * For example:
   *       data-shape-key = ["label1", "label2"]
   *       data-shape-key-publish-labels = ["foo", "bar"]
   *   Suppose we had the time-series labels:
   *       my_metric{label1="value1", label2="value2", ...}
   *   If this matches a configured "allow" set of labels and no "block" set, then
   *     data-shape metrics would be updated as:
   *       data_shape{foo="value1", bar="value2", dimension="label-count", ...}
   *   If data-shape-key-published-labels is empty, then data-shape metrics would
   *     not include the data-shape key values.
   */
  val dataShapeKeyPublishLabels = config.getOrElse[Seq[String]]("data-shape-key-publish-labels", Seq())

  /**
   * Allow/Block publish of data-shape stats for specific data-shape keys.
   * Example:
   *   Allow: [ [ws1] ]
   *   Block: [ [ws1, ns1] ]
   *   --> publish data-shape stats for all of ws1 except in ns1
   *
   * Each sequence can have a length less than the length of a key.
   */
  val dataShapeAllowTrie: Map[String, Any] = dataShapeKeysToTrie(
    config.getOrElse[Seq[Seq[String]]]("data-shape-allow", Seq()))
  val dataShapeBlockTrie: Map[String, Any] = dataShapeKeysToTrie(
    config.getOrElse[Seq[Seq[String]]]("data-shape-block", Seq()))

  /**
   * A bucket-count data-shape metric is published iff this is true.
   */
  val enableDataShapeBucketCount = config.getOrElse[Boolean]("enable-data-shape-bucket-count", false)

  /**
    * TTL for downsampled data for the resolutions in same order
    */
  val ttls = if (config.hasPath ("ttls")) config.as[Seq[FiniteDuration]]("ttls")
             else Seq.empty
  require(resolutions.length == ttls.length, "Downsample TTLs and resolutions are not same length")
  require(ttls.sorted == ttls, "Downsample TTLs are not sorted")
  require(resolutions.sorted == resolutions, "Downsample resolutions are not sorted")

  /**
    * Schemas to downsample
    */
  val schemas = if (config.hasPath ("raw-schema-names")) config.as[Seq[String]]("raw-schema-names")
                else Seq.empty
  val indexLocation = config.getOrElse[Option[String]]("index-location", None)

  val maxRefreshHours = config.getOrElse("max-refresh-hours", Long.MaxValue)

  val indexRebuildParallelismMultiplier = config.getOrElse("index-rebuild-parallelism-multiplier", 1.0)

  val housekeepingParallelismMultiplier = config.getOrElse("housekeeping-parallelism-multiplier", 1.0)

  val enablePersistentIndexing = indexLocation.isDefined

  val indexMetastoreImplementation = if (config.hasPath("index-metastore-implementation")) {
    val impl = config.as[String]("index-metastore-implementation")
    val lowercaseImpl = impl.toLowerCase
    lowercaseImpl match {
      case "file" => IndexMetastoreImplementation.File
      case "ephemeral" => IndexMetastoreImplementation.Ephemeral
      case _ => IndexMetastoreImplementation.NoImp
    }
  } else {
    IndexMetastoreImplementation.NoImp
  }

  def downsampleDatasetRefs(rawDataset: String): Seq[DatasetRef] = {
    resolutions.map { res =>
      DatasetRef(s"${rawDataset}_ds_${res.toMinutes}")
    }
  }

}

object DownsampleConfig {
  val disabled = DownsampleConfig(ConfigFactory.empty)
  def downsampleConfigFromSource(sourceConfig: Config): DownsampleConfig = {
    if (sourceConfig.hasPath("downsample")) DownsampleConfig(sourceConfig.getConfig("downsample"))
    else disabled
  }
}

