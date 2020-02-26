package filodb.repair

import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

class ChunkCopierSpec extends FunSpec with Matchers with BeforeAndAfterAll {
  it ("should copy chunks") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")

    sparkConf.set("spark.filodb.chunkcopier.source.configFile", "conf/timeseries-filodb-server.conf")
    sparkConf.set("spark.filodb.chunkcopier.source.dataset", "source")

    sparkConf.set("spark.filodb.chunkcopier.target.configFile", "conf/timeseries-filodb-server.conf")
    sparkConf.set("spark.filodb.chunkcopier.target.dataset", "target")

    sparkConf.set("spark.filodb.chunkcopier.ingestionTimeStart", "2020-02-26T13:00:00Z")
    sparkConf.set("spark.filodb.chunkcopier.ingestionTimeEnd",   "2020-02-26T23:00:00Z")
    sparkConf.set("spark.filodb.chunkcopier.diskTimeToLive", "7d")

    new ChunkCopier().run(sparkConf)
  }
}
