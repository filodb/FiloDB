package filodb.repair

//import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import com.typesafe.config.ConfigFactory

import filodb.cassandra.DefaultFiloSessionProvider
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.GlobalConfig
import filodb.core.metadata.{Dataset, Schemas}

class ChunkCopierSpec extends FunSpec with Matchers with BeforeAndAfterAll with ScalaFutures {
  implicit val defaultPatience =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(250, Millis))

  implicit val s = monix.execution.Scheduler.Implicits.global

  val configPath = "conf/timeseries-filodb-server.conf"

  val sysConfig = GlobalConfig.systemConfig.getConfig("filodb")
  val config = ConfigFactory.parseFile(new java.io.File(configPath))
    .getConfig("filodb").withFallback(sysConfig)

  lazy val session = new DefaultFiloSessionProvider(config.getConfig("cassandra")).session
  lazy val colStore = new CassandraColumnStore(config, s, session)

  it ("should run a simple Spark job") {
    // This test verifies that the configuration can be read and that Spark runs. A full test
    // that verifies chunks are copied correctly is found in CassandraColumnStoreSpec.

    val dataset = Dataset("source", Schemas.gauge)
    val targetDataset = Dataset("target", Schemas.gauge)

    colStore.initialize(dataset.ref, 1).futureValue
    colStore.truncate(dataset.ref, 1).futureValue

    colStore.initialize(targetDataset.ref, 1).futureValue
    colStore.truncate(targetDataset.ref, 1).futureValue

    /*
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")

    sparkConf.set("spark.filodb.chunkcopier.source.configFile", configPath)
    sparkConf.set("spark.filodb.chunkcopier.source.dataset", "source")

    sparkConf.set("spark.filodb.chunkcopier.target.configFile", configPath)
    sparkConf.set("spark.filodb.chunkcopier.target.dataset", "target")

    sparkConf.set("spark.filodb.chunkcopier.ingestionTimeStart", "2020-02-26T13:00:00Z")
    sparkConf.set("spark.filodb.chunkcopier.ingestionTimeEnd",   "2020-02-26T23:00:00Z")
    sparkConf.set("spark.filodb.chunkcopier.diskTimeToLive", "7d")

    ChunkCopierMain.run(sparkConf)

    colStore.truncate(dataset.ref, 1).futureValue
    colStore.truncate(targetDataset.ref, 1).futureValue
     */
  }
}
