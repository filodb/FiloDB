package filodb.coordinator

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

class FiloDBSettingsSpec extends RunnableSpec {
  "FiloDBSettings" must {
    "have default expected settings from provided config" in {
      val settings = new FilodbSettings(AkkaSpec.settings.allConfig)
      !settings.config.isEmpty should be (true)
      !settings.allConfig.isEmpty should be (true)

      import settings._
      SeedNodes.size should be (1)
      InitializationTimeout should be(60.seconds)
      StorageStrategy should be (StoreStrategy.TimeSeriesNullSink)
    }
    "have default settings" in {
      val settings = new FilodbSettings(ConfigFactory.parseString(
        """filodb.seed-nodes = "filodb.cassandra.CassandraStoreFactory""""))

      import settings._
      SeedNodes.size should be (1)
      StorageStrategy should be (StoreStrategy.TimeSeriesNullSink)
    }
  }
}
