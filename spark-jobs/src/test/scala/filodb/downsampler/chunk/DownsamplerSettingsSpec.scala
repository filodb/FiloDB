package filodb.downsampler.chunk

import com.typesafe.config.ConfigFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class DownsamplerSettingsSpec extends AnyFunSpec with Matchers {
  describe("DownsamplerSettings write-to-ingestion-time-index") {
    it("should default to true when the key is absent (falls back to filodb-defaults)") {
      new DownsamplerSettings(ConfigFactory.empty()).writeToIngestionTimeIndex shouldEqual true
    }

    it("should honor an override of write-to-ingestion-time-index = false") {
      val conf = ConfigFactory.parseString("filodb.downsampler.write-to-ingestion-time-index = false")
      new DownsamplerSettings(conf).writeToIngestionTimeIndex shouldEqual false
    }
  }
}
