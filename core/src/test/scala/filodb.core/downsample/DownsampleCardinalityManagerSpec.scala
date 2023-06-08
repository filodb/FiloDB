package filodb.core.downsample

import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import filodb.core.{GlobalConfig, MachineMetricsData, MetricsTestData}
import filodb.core.memstore.PartKeyLuceneIndex
import filodb.core.memstore.ratelimit.ConfigQuotaSource
import filodb.core.metadata._
import filodb.core.store.StoreConfig

import scala.concurrent.duration.DurationInt

class DownsampleCardinalityManagerSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  val filodbConfig = GlobalConfig.defaultFiloConfig
  val downsampleStoreConfig = StoreConfig(
    filodbConfig.getConfig("downsampler.downsample-store-config"))
  val shardKeyLen = MetricsTestData.timeseriesDatasetMultipleShardKeys.options.shardKeyColumns.length
  val quotaSource = new ConfigQuotaSource(filodbConfig, shardKeyLen)
  val partSchema = Schemas(MetricsTestData.timeseriesDatasetMultipleShardKeys.schema).part

  def getTestLuceneIndex(shardNum: Int): PartKeyLuceneIndex = {
    new PartKeyLuceneIndex(
      MachineMetricsData.dataset2.ref,
      MachineMetricsData.dataset2.schema.partition, true, true,
      shardNum, 1.hour.toMillis,
      Some(new java.io.File(System.getProperty("java.io.tmpdir"), "part-key-lucene-index")))
  }

  it("shouldTriggerCardinalityCount should return expected values") {
    val testShardNum = 10
    val idx = getTestLuceneIndex(testShardNum)
    val cardManager = new DownsampleCardinalityManager(
      MetricsTestData.timeseriesDatasetMultipleShardKeys.ref, testShardNum, shardKeyLen, idx, partSchema,
      filodbConfig, downsampleStoreConfig, quotaSource)

    var resultMap:Map[Int,Set[Int]] = Map()
    resultMap += (0 -> Set(0,8,16))
    resultMap += (1 -> Set(1,9,17))
    resultMap += (2 -> Set(2,10,18))
    resultMap += (3 -> Set(3,11,19))
    resultMap += (4 -> Set(4,12,20))
    resultMap += (5 -> Set(5,13,21))
    resultMap += (6 -> Set(6,14,22))
    resultMap += (7 -> Set(7,15,23))

    // testing for shardsPerNode = 8
    for {shardNum <- 0 until 256} {
      // for each shard
      for {currentHour <- 0 until 24} {
        val shardAfterMod = shardNum % 8
        val assertValue = resultMap(shardAfterMod).contains(currentHour)
        cardManager.shouldTriggerCardinalityCount(shardNum, 8, currentHour) shouldEqual assertValue
      }
    }
  }

  it("getNumShardsPerNodeFromConfig should work with fallback and default value") {
    val testShardNum = 10
    val idx = getTestLuceneIndex(testShardNum)
    val cardManager = new DownsampleCardinalityManager(
      MetricsTestData.timeseriesDatasetMultipleShardKeys.ref, testShardNum, shardKeyLen, idx, partSchema,
      filodbConfig, downsampleStoreConfig, quotaSource)

    // `dataset-config` has required config
    val confWithDatasetConfigs = """
      |  filodb {
      |    dataset-configs = [
      |      {
      |        dataset = "prometheus"
      |        min-num-nodes = 8
      |        num-shards = 16
      |      }
      |    ]
      |  }
      |""".stripMargin

    var conf = ConfigFactory.parseString(confWithDatasetConfigs).getConfig("filodb")
    cardManager.getNumShardsPerNodeFromConfig("prometheus", conf) shouldEqual 2

    // no `dataset-configs` - but should fallback to `inline-dataset-configs`
    val confWithInlineDatasetConfigs =
      """
        |  filodb {
        |    inline-dataset-configs = [
        |      {
        |        dataset = "prometheus"
        |        min-num-nodes = 8
        |        num-shards = 16
        |      }
        |    ]
        |  }
        |""".stripMargin
    conf = ConfigFactory.parseString(confWithInlineDatasetConfigs).getConfig("filodb")
    cardManager.getNumShardsPerNodeFromConfig("prometheus", conf) shouldEqual 2

    // `dataset-configs` present, but doesn't have config for `prometheus` dataset. should use `inline-dataset-config`
    val confWithDatasetConfigMissingDataset =
      """
        |  filodb {
        |
        |    dataset-configs = [
        |      {
        |        dataset = "not-prometheus"
        |        min-num-nodes = 8
        |        num-shards = 16
        |      }
        |    ]
        |
        |    inline-dataset-configs = [
        |      {
        |        dataset = "prometheus"
        |        min-num-nodes = 8
        |        num-shards = 32
        |      }
        |    ]
        |  }
        |""".stripMargin
    conf = ConfigFactory.parseString(confWithDatasetConfigMissingDataset).getConfig("filodb")
    cardManager.getNumShardsPerNodeFromConfig("prometheus", conf) shouldEqual 4

    // both `dataset-configs` and `inline-dataset-configs` don't have the required dataset, hence fallback used
    val confWithBothConfigsMissingDataset =
      """
        |  filodb {
        |
        |    dataset-configs = [
        |      {
        |        dataset = "not-prometheus"
        |        min-num-nodes = 8
        |        num-shards = 16
        |      }
        |    ]
        |
        |    inline-dataset-configs = [
        |      {
        |        dataset = "not-prometheus"
        |        min-num-nodes = 8
        |        num-shards = 16
        |      }
        |    ]
        |  }
        |""".stripMargin

    conf = ConfigFactory.parseString(confWithBothConfigsMissingDataset).getConfig("filodb")
    cardManager.getNumShardsPerNodeFromConfig("prometheus", conf) shouldEqual 8

    // both `dataset-configs` and `inline-dataset-configs` are missing, hence fallback used
    val confWithBothConfigsMissing =
      """
        |  filodb {
        |
        |  }
        |""".stripMargin

    conf = ConfigFactory.parseString(confWithBothConfigsMissing).getConfig("filodb")
    cardManager.getNumShardsPerNodeFromConfig("prometheus", conf) shouldEqual 8
  }
}
