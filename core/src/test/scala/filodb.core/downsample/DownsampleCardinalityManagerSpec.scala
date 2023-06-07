package filodb.core.downsample

import org.scalatest.{BeforeAndAfterAll}
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
}
