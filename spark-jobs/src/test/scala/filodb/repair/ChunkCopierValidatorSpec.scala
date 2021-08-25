package filodb.repair

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import filodb.core.GlobalConfig
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

@DoNotDiscover
class ChunkCopierValidatorSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with ScalaFutures {
  val sourceConfigPath = "conf/timeseries-filodb-server.conf"
  val targetConfigPath = "spark-jobs/src/test/resources/timeseries-filodb-buddy-server.conf"

  it ("runs") {
    val sparkConf = {
      val conf = new SparkConf(loadDefaults = true)
      conf.setMaster("local[2]")

      conf.set("spark.filodb.chunks.copier.validator.source.config.value", parseFileConfig(sourceConfigPath))
      conf.set("spark.filodb.chunks.copier.validator.target.config.value", parseFileConfig(targetConfigPath))

      conf.set("spark.filodb.chunks.copier.validator.dataset", "prometheus")
      conf.set("spark.filodb.chunks.copier.validator.is.downsample.copy", "false")
      conf.set("spark.filodb.chunks.copier.validator.dataset.downsample.resolution", "")

      conf.set("spark.filodb.chunks.copier.validator.start.time", "2020-10-13T00:00:00Z")
      conf.set("spark.filodb.chunks.copier.validator.end.time", "2020-10-13T05:00:00Z")
      conf
    }

    ChunkCopierValidatorMain.run(sparkConf)
  }

  def parseFileConfig(confStr: String) = {
    val config = ConfigFactory
      .parseFile(new File(confStr))
      .withFallback(GlobalConfig.systemConfig)
    config.root().render(ConfigRenderOptions.concise())
  }

}
